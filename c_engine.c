#define _GNU_SOURCE
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include "./blake3/blake3.h"
#include <jansson.h>


#define OP_UPLOAD_START 0x01
#define OP_UPLOAD_CHUNK 0x02
#define OP_UPLOAD_FINISH 0x03
#define OP_UPLOAD_DONE 0x81
#define OP_DOWNLOAD_START 0x10
#define OP_DOWNLOAD_CHUNK 0x90
#define OP_DOWNLOAD_DONE 0x91
#define OP_ERROR 0xFF
#define MAX_FRAME_PAYLOAD (4 * 1024 * 1024)
#define MAX_FILENAME_LEN 255
#define DEFAULT_CHUNK_SIZE (256 * 1024)
#define N_WORKERS 4
#define DATA_ROOT "./data"
#define BLOCKS_DIR DATA_ROOT "/blocks"
#define MANIFESTS_DIR DATA_ROOT "/manifests"


static const char* g_sock_path = NULL;


typedef struct {
	uint32_t index;
	uint32_t size;
	char hash_hex[65];
} chunk_info_t;

typedef struct {
	int version;
	char hash_algo[16];
	uint32_t chunk_size;
	uint64_t total_size;
	char filename[256];
	chunk_info_t *chunks;
	size_t chunk_count;
	size_t chunk_cap;
} manifest_t;

typedef struct {
	int active;
	pthread_mutex_t mu;
	pthread_cond_t  cv;
	int error;
	size_t pending;
	manifest_t mf;
} upload_state_t;

typedef struct download_state {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    manifest_t mf;
    size_t total_chunks;
    uint32_t next_to_send;
    int error_flag;
    uint8_t  **bufs;
    uint32_t *lens;
    uint8_t  *ready;
} download_state_t;

typedef enum {
	JOB_UPLOAD_HASH_STORE,
	JOB_DOWNLOAD_READ_VERIFY
} job_type_t;

typedef struct job {
	job_type_t type;
	struct job *next;
	uint32_t index;
	upload_state_t *up;
	uint8_t *data;
	uint32_t len;
	download_state_t *ds;
} job_t;

typedef struct {
	uint8_t op;
	uint32_t len;
	uint8_t *payload;
} frame_t;


static job_t *job_head = NULL;
static job_t *job_tail = NULL;
static pthread_mutex_t job_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  job_cv = PTHREAD_COND_INITIALIZER;

static size_t uvarint_encode(uint64_t x, uint8_t *out, size_t outcap) {
    size_t i = 0;
    while (x >= 0x80) {
        if (i >= outcap) return 0;
        out[i++] = (uint8_t)((x & 0x7F) | 0x80);
        x >>= 7;
    }
    if (i >= outcap) return 0;
    out[i++] = (uint8_t)x;
    return i;
}

static size_t base32_encode_lower_nopad(const uint8_t *in, size_t inlen, char *out, size_t outcap) {
	static const char *alphabet = "abcdefghijklmnopqrstuvwxyz234567";
	size_t outlen = 0;

	uint64_t buffer = 0;
	int bits = 0;

	for (size_t i = 0; i < inlen; i++) {
		buffer = (buffer << 8) | in[i];
		bits += 8;
		while (bits >= 5) {
			bits -= 5;
			uint8_t idx = (buffer >> bits) & 0x1F;
			if (outlen + 1 >= outcap) return 0;
			out[outlen++] = alphabet[idx];
		}
	}

	if (bits > 0) {
		uint8_t idx = (buffer << (5 - bits)) & 0x1F;
		if (outlen + 1 >= outcap) return 0;
		out[outlen++] = alphabet[idx];
	}

	if (outlen >= outcap) return 0;
	out[outlen] = '\0';
	return outlen;
}

static void bytes_to_hex(const uint8_t *in, size_t n, char *out_hex) {
	static const char *hex = "0123456789abcdef";
	for (size_t i = 0; i < n; i++) {
		out_hex[2*i]     = hex[in[i] >> 4];
		out_hex[2*i + 1] = hex[in[i] & 0x0F];
	}
	out_hex[2*n] = '\0';
}

static void blake3_hash_hex(const uint8_t *data, size_t len, char out_hex[65]) {
	uint8_t out[32];
	blake3_hasher hasher;
	blake3_hasher_init(&hasher);
	blake3_hasher_update(&hasher, data, len);
	blake3_hasher_finalize(&hasher, out, 32);
	bytes_to_hex(out, 32, out_hex);
}

static int write_all(int fd, const void *buf, size_t n) {
	size_t sent = 0;
	while (sent < n) {
		ssize_t w = write(fd, (const char*)buf + sent, n - sent);
		if (w == 0) {
			errno = EPIPE;
			return -1;
		}
		if (w < 0) {
			if (errno == EINTR) continue;
			return -1;
		}
		sent += (size_t)w;
	}
	return 0;
}

static int send_frame(int fd, uint8_t op, const void *payload, uint32_t len) {
	if (len > MAX_FRAME_PAYLOAD) {
		errno = EMSGSIZE;
		return -1;
	}
	uint8_t header[5];
	header[0] = op;
	uint32_t be_len = htonl(len);
	memcpy(header + 1, &be_len, 4);

	if (write_all(fd, header, 5) < 0) return -1;
	if (len && write_all(fd, payload, len) < 0) return -1;
	return 0;
}

static size_t json_escape(char *dst, size_t cap, const char *src) {
	size_t out = 0;
	for (const unsigned char *p = (const unsigned char*)src; *p; p++) {
		const char *rep = NULL;
		char tmp[7];
		switch (*p) {
		case '\"':
			rep = "\\\"";
			break;
		case '\\':
			rep = "\\\\";
			break;
		case '\b':
			rep = "\\b";
			break;
		case '\f':
			rep = "\\f";
			break;
		case '\n':
			rep = "\\n";
			break;
		case '\r':
			rep = "\\r";
			break;
		case '\t':
			rep = "\\t";
			break;
		default:
			if (*p < 0x20) {
				snprintf(tmp, sizeof(tmp), "\\u%04x", *p);
				rep = tmp;
			}
		}
		if (rep) {
			size_t rlen = strlen(rep);
			if (out + rlen >= cap) break;
			memcpy(dst + out, rep, rlen);
			out += rlen;
		} else {
			if (out + 1 >= cap) break;
			dst[out++] = (char)*p;
		}
	}
	if (cap) dst[out < cap ? out : cap-1] = '\0';
	return out;
}

static int send_error(int cfd, const char *code, const char *message) {
	char esc_code[64];
	char esc_msg[256];

	json_escape(esc_code, sizeof(esc_code), code ? code : "E_UNKNOWN");
	json_escape(esc_msg, sizeof(esc_msg), message ? message : "");

	char buf[512];
	int n = snprintf(buf, sizeof(buf),
	                 "{\"code\":\"%s\",\"message\":\"%s\"}",
	                 esc_code, esc_msg);
	if (n < 0) return -1;
	if ((size_t)n >= sizeof(buf)) n = (int)sizeof(buf) - 1;

	return send_frame(cfd, OP_ERROR, buf, (uint32_t)n);
}

static int protocol_error(int cfd, const char *msg) {
	fprintf(stderr, "[ENGINE] protocol error: %s\n", msg);
	fflush(stderr);
	(void)send_error(cfd, "E_PROTO", msg);
	return -1;
}

static ssize_t read_n(int fd, void *buf, size_t n) {
	size_t got = 0;
	while (got < n) {
		ssize_t r = read(fd, (char*)buf + got, n - got);
		if (r == 0) return 0;
		if (r < 0) {
			if (errno == EINTR) continue;
			return -1;
		}
		got += (size_t)r;
	}
	return (ssize_t)got;
}

static int mkdir_p(const char *path, mode_t mode) {
	char tmp[PATH_MAX];
	if (!path || !*path) {
		errno = EINVAL;
		return -1;
	}

	size_t len = strlen(path);
	if (len >= sizeof(tmp)) {
		errno = ENAMETOOLONG;
		return -1;
	}

	memcpy(tmp, path, len + 1);

	if (tmp[len - 1] == '/') tmp[len - 1] = '\0';

	for (char *p = tmp + 1; *p; p++) {
		if (*p == '/') {
			*p = '\0';
			if (mkdir(tmp, mode) < 0 && errno != EEXIST) return -1;
			*p = '/';
		}
	}
	if (mkdir(tmp, mode) < 0 && errno != EEXIST) return -1;
	return 0;
}

static int recv_frame(int fd, frame_t *fr) {
	uint8_t header[5];
	ssize_t r = read_n(fd, header, 5);
	if (r == 0) return 0;
	if (r < 0) return -1;

	fr->op = header[0];
	uint32_t len_be;
	memcpy(&len_be, header + 1, 4);
	fr->len = ntohl(len_be);

	if (fr->len > MAX_FRAME_PAYLOAD) {
		errno = EMSGSIZE;
		return -1;
	}

	fr->payload = NULL;
	if (fr->len) {
		fr->payload = (uint8_t*)malloc(fr->len);
		if (!fr->payload) return -1;
		ssize_t rr = read_n(fd, fr->payload, fr->len);
		if (rr <= 0) {
			free(fr->payload);
			fr->payload = NULL;
			return -1;
		}
	}
	return 1;
}

static int cmp_chunk_index(const void *a, const void *b) {
	const chunk_info_t *x = (const chunk_info_t *)a;
	const chunk_info_t *y = (const chunk_info_t *)b;

	if (x->index < y->index) return -1;
	if (x->index > y->index) return 1;
	return 0;
}

static int multihash_code_for_algo(const char *algo, uint64_t *code_out) {
	if (!algo) return -1;
	if (strcmp(algo, "sha256") == 0) {
		*code_out = 0x12;
		return 0;
	}
	if (strcmp(algo, "blake3") == 0) {
		*code_out = 0x1e;
		return 0;
	}
	return -1;
}

static int manifest_add_chunk(manifest_t *mf, uint32_t index, uint32_t size, const char hash_hex[65]) {
	if (mf->chunk_count == mf->chunk_cap) {
		size_t new_cap = mf->chunk_cap ? mf->chunk_cap * 2 : 16;
		chunk_info_t *n = realloc(mf->chunks, new_cap * sizeof(*n));
		if (!n) {
			perror("realloc manifest chunks");
			return -1;
		}
		mf->chunks = n;
		mf->chunk_cap = new_cap;
	}
	chunk_info_t *ci = &mf->chunks[mf->chunk_count++];
	ci->index = index;
	ci->size = size;
	memcpy(ci->hash_hex, hash_hex, 65);
	ci->hash_hex[64] = '\0';
	mf->total_size += size;
	return 0;
}

static char* build_manifest_json(const manifest_t *mf, size_t *out_len) {
	json_t *root = json_object();
	if (!root) return NULL;

	if (json_object_set_new(root, "version", json_integer(mf->version)) < 0 ||
	        json_object_set_new(root, "hash_algo", json_string(mf->hash_algo)) < 0 ||
	        json_object_set_new(root, "chunk_size", json_integer((json_int_t)mf->chunk_size)) < 0 ||
	        json_object_set_new(root, "total_size", json_integer((json_int_t)mf->total_size)) < 0 ||
	        json_object_set_new(root, "filename", json_string(mf->filename)) < 0) {
		json_decref(root);
		return NULL;
	}

	json_t *arr = json_array();
	if (!arr) {
		json_decref(root);
		return NULL;
	}

	for (size_t i = 0; i < mf->chunk_count; i++) {
		const chunk_info_t *ci = &mf->chunks[i];
		json_t *o = json_object();
		if (!o) {
			json_decref(arr);
			json_decref(root);
			return NULL;
		}

		json_object_set_new(o, "index", json_integer(ci->index));
		json_object_set_new(o, "size",  json_integer(ci->size));
		json_object_set_new(o, "hash",  json_string(ci->hash_hex));

		json_array_append_new(arr, o);
	}

	json_object_set_new(root, "chunks", arr);

	char *s = json_dumps(root, JSON_COMPACT | JSON_SORT_KEYS);
	json_decref(root);

	if (s && out_len) *out_len = strlen(s);
	return s;
}

static char* load_file(const char *path, size_t *out_len) {
	int fd = open(path, O_RDONLY | O_CLOEXEC);
	if (fd < 0) {
		perror("open");
		return NULL;
	}

	struct stat st;
	if (fstat(fd, &st) < 0) {
		perror("fstat");
		close(fd);
		return NULL;
	}

	if (st.st_size < 0) {
		close(fd);
		errno = EINVAL;
		return NULL;
	}

	size_t size = (size_t)st.st_size;
	char *buf = (char*)malloc(size + 1);
	if (!buf) {
		perror("malloc");
		close(fd);
		return NULL;
	}

	if (size > 0) {
		ssize_t r = read_n(fd, buf, size);
		if (r <= 0) {
			fprintf(stderr, "load_file: short read for %s\n", path);
			free(buf);
			close(fd);
			errno = EIO;
			return NULL;
		}
	}

	close(fd);
	buf[size] = '\0';
	if (out_len) *out_len = size;
	return buf;
}

static int load_manifest(const char *cid, manifest_t *mf) {
	memset(mf, 0, sizeof(*mf));

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/%s.json", MANIFESTS_DIR, cid);

	size_t mlen = 0;
	char *buf = load_file(path, &mlen);
	if (!buf) return -1;

	json_error_t error;
	json_t *root = json_loads(buf, 0, &error);
	free(buf);
	if (!root) return -1;

	int rc = -1;

	json_t *v = json_object_get(root, "version");
	json_t *ha = json_object_get(root, "hash_algo");
	json_t *cs = json_object_get(root, "chunk_size");
	json_t *ts = json_object_get(root, "total_size");
	json_t *fn = json_object_get(root, "filename");
	json_t *arr = json_object_get(root, "chunks");

	if (!json_is_integer(v) || !json_is_string(ha) || !json_is_integer(cs) ||
	        !json_is_integer(ts) || !json_is_string(fn) || !json_is_array(arr)) {
		goto cleanup;
	}

	mf->version = (int)json_integer_value(v);
	strncpy(mf->hash_algo, json_string_value(ha), sizeof(mf->hash_algo)-1);
	mf->chunk_size = (uint32_t)json_integer_value(cs);
	mf->total_size = (uint64_t)json_integer_value(ts);
	strncpy(mf->filename, json_string_value(fn), sizeof(mf->filename)-1);

	size_t n = json_array_size(arr);
	mf->chunks = calloc(n ? n : 1, sizeof(chunk_info_t));
	if (!mf->chunks) goto cleanup;
	mf->chunk_cap = n;
	mf->chunk_count = 0;

	for (size_t i = 0; i < n; i++) {
		json_t *o = json_array_get(arr, i);
		if (!json_is_object(o)) goto cleanup;

		json_t *idx = json_object_get(o, "index");
		json_t *sz  = json_object_get(o, "size");
		json_t *hs  = json_object_get(o, "hash");

		if (!json_is_integer(idx) || !json_is_integer(sz) || !json_is_string(hs)) goto cleanup;

		const char *hstr = json_string_value(hs);
		if (!hstr || strlen(hstr) != 64) goto cleanup;

		chunk_info_t *ci = &mf->chunks[mf->chunk_count++];
		ci->index = (uint32_t)json_integer_value(idx);
		ci->size  = (uint32_t)json_integer_value(sz);
		memcpy(ci->hash_hex, hstr, 65);
		ci->hash_hex[64] = '\0';
	}

	qsort(mf->chunks, mf->chunk_count, sizeof(chunk_info_t), cmp_chunk_index);
	for (size_t i = 0; i < mf->chunk_count; i++) {
		if (mf->chunks[i].index != i) goto cleanup;
	}

	rc = 0;

cleanup:
	json_decref(root);
	if (rc != 0) {
		free(mf->chunks);
		memset(mf, 0, sizeof(*mf));
	}
	return rc;
}

static void compute_cid(const manifest_t *mf, const char *manifest_json, size_t mlen, char cid_buf[128]) {
	uint8_t digest[32];
	blake3_hasher h;
	blake3_hasher_init(&h);
	blake3_hasher_update(&h, manifest_json, mlen);
	blake3_hasher_finalize(&h, digest, 32);

	uint64_t mh_code = 0;
	if (multihash_code_for_algo(mf->hash_algo, &mh_code) < 0) {
		snprintf(cid_buf, 128, "b");
		return;
	}

	uint8_t cid_bytes[256];
	size_t off = 0;

	off += uvarint_encode(0x01, cid_bytes + off, sizeof(cid_bytes) - off);

	off += uvarint_encode(0x0129, cid_bytes + off, sizeof(cid_bytes) - off);

	off += uvarint_encode(mh_code, cid_bytes + off, sizeof(cid_bytes) - off);
	off += uvarint_encode(32, cid_bytes + off, sizeof(cid_bytes) - off);

	if (off + 32 > sizeof(cid_bytes)) {
		snprintf(cid_buf, 128, "b");
		return;
	}
	memcpy(cid_bytes + off, digest, 32);
	off += 32;

	cid_buf[0] = 'b';
	if (base32_encode_lower_nopad(cid_bytes, off, cid_buf + 1, 127) == 0) {
		snprintf(cid_buf, 128, "b");
	}
}

static int fsync_dir(const char *dirpath) {
	int dfd = open(dirpath, O_RDONLY | O_DIRECTORY);
	if (dfd < 0) return -1;
	int rc = fsync(dfd);
	close(dfd);
	return rc;
}

static int write_manifest_atomic(const char *cid, const char *manifest, size_t mlen) {
	if (mkdir_p(MANIFESTS_DIR, 0777) < 0) return -1;

	char final_path[PATH_MAX];
	char tmp_path[PATH_MAX];
	snprintf(final_path, sizeof(final_path), "%s/%s.json", MANIFESTS_DIR, cid);
	snprintf(tmp_path, sizeof(tmp_path), "%s/.%s.tmp.%d", MANIFESTS_DIR, cid, getpid());

	int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
	if (fd < 0) return -1;

	if (write_all(fd, manifest, mlen) < 0) {
		close(fd);
		unlink(tmp_path);
		return -1;
	}
	if (fsync(fd) < 0) {
		close(fd);
		unlink(tmp_path);
		return -1;
	}
	close(fd);

	if (rename(tmp_path, final_path) < 0) {
		unlink(tmp_path);
		return -1;
	}

	(void)fsync_dir(MANIFESTS_DIR);
	return 0;
}

static void enqueue_job(job_t *job) {
	job->next = NULL;
	pthread_mutex_lock(&job_mu);
	if (job_tail) {
		job_tail->next = job;
		job_tail = job;
	} else {
		job_head = job_tail = job;
	}
	pthread_cond_signal(&job_cv);
	pthread_mutex_unlock(&job_mu);
}

static int block_paths_from_hash(const char *hash_hex, char dir_aa[PATH_MAX], char dir_aabb[PATH_MAX], char final_path[PATH_MAX], char tmp_path[PATH_MAX]) {
	if (!hash_hex || strlen(hash_hex) < 4) {
		errno = EINVAL;
		return -1;
	}

	char aa[3] = { hash_hex[0], hash_hex[1], '\0' };
	char bb[3] = { hash_hex[2], hash_hex[3], '\0' };

	if (snprintf(dir_aa, PATH_MAX, "%s/%s", BLOCKS_DIR, aa) >= PATH_MAX) {
		errno = ENAMETOOLONG;
		return -1;
	}
	if (snprintf(dir_aabb, PATH_MAX, "%s/%s/%s", BLOCKS_DIR, aa, bb) >= PATH_MAX) {
		errno = ENAMETOOLONG;
		return -1;
	}
	if (snprintf(final_path, PATH_MAX, "%s/%s/%s/%s", BLOCKS_DIR, aa, bb, hash_hex) >= PATH_MAX) {
		errno = ENAMETOOLONG;
		return -1;
	}
	if (snprintf(tmp_path, PATH_MAX, "%s/%s/%s/.%s.tmp.%d", BLOCKS_DIR, aa, bb, hash_hex, (int)getpid()) >= PATH_MAX) {
		errno = ENAMETOOLONG;
		return -1;
	}

	return 0;
}

static int store_block(const char *hash_hex, const uint8_t *data, size_t len) {
	char dir_aa[PATH_MAX], dir_aabb[PATH_MAX], final_path[PATH_MAX], tmp_path[PATH_MAX];
	if (block_paths_from_hash(hash_hex, dir_aa, dir_aabb, final_path, tmp_path) < 0) return -1;

	if (mkdir_p(BLOCKS_DIR, 0777) < 0) return -1;
	if (mkdir_p(dir_aa, 0777) < 0) return -1;
	if (mkdir_p(dir_aabb, 0777) < 0) return -1;

	int fd = open(final_path, O_RDONLY | O_CLOEXEC);
	if (fd >= 0) {
		close(fd);
		return 0;
	}

	int tfd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
	if (tfd < 0) return -1;

	if (write_all(tfd, data, len) < 0) {
		close(tfd);
		unlink(tmp_path);
		return -1;
	}
	if (fsync(tfd) < 0) {
		close(tfd);
		unlink(tmp_path);
		return -1;
	}
	close(tfd);

	if (link(tmp_path, final_path) < 0) {
		if (errno == EEXIST) {
			unlink(tmp_path);
			return 0;
		}
		unlink(tmp_path);
		return -1;
	}

	unlink(tmp_path);
	(void)fsync_dir(dir_aabb);
	return 0;
}

static int load_block(const chunk_info_t *ci, uint8_t **out_data, size_t *out_len) {
	const char *hash_hex = ci->hash_hex;

	char dir_aa[PATH_MAX], dir_aabb[PATH_MAX], path[PATH_MAX], tmp[PATH_MAX];
	if (block_paths_from_hash(hash_hex, dir_aa, dir_aabb, path, tmp) < 0) return -1;

	int fd = open(path, O_RDONLY | O_CLOEXEC);
	if (fd < 0) return -1;

	struct stat st;
	if (fstat(fd, &st) < 0) {
		close(fd);
		return -1;
	}
	if ((uint64_t)st.st_size != (uint64_t)ci->size) {
		close(fd);
		errno = EIO;
		return -1;
	}

	size_t expected = ci->size;
	uint8_t *buf = (uint8_t*)malloc(expected ? expected : 1);
	if (!buf) {
		close(fd);
		return -1;
	}

	if (expected > 0) {
		ssize_t r = read_n(fd, buf, expected);
		if (r != (ssize_t)expected) {
			free(buf);
			close(fd);
			errno = EIO;
			return -1;
		}
	}
	close(fd);

	char check_hash[65];
	blake3_hash_hex(buf, expected, check_hash);

	if (strncmp(check_hash, hash_hex, 64) != 0) {
		free(buf);
		errno = EBADMSG;
		return -1;
	}

	*out_data = buf;
	*out_len  = expected;
	return 0;
}

static void* worker_main(void *arg) {
	(void)arg;
	for (;;) {
		pthread_mutex_lock(&job_mu);
		while (!job_head) pthread_cond_wait(&job_cv, &job_mu);
		job_t *job = job_head;
		job_head = job->next;
		if (!job_head) job_tail = NULL;
		pthread_mutex_unlock(&job_mu);

		if (job->type == JOB_UPLOAD_HASH_STORE) {

			char h[65];
			blake3_hash_hex(job->data, job->len, h);

			int ok = (store_block(h, job->data, job->len) == 0);

			pthread_mutex_lock(&job->up->mu);
			if (!ok) {
				job->up->error = 1;
			} else {
				if (manifest_add_chunk(&job->up->mf, job->index, (uint32_t)job->len, h) < 0)
					job->up->error = 1;
			}
			job->up->pending--;
			pthread_cond_signal(&job->up->cv);
			pthread_mutex_unlock(&job->up->mu);

			free(job->data);
			free(job);

		} else if (job->type == JOB_DOWNLOAD_READ_VERIFY) {
			uint8_t *buf = NULL;
			size_t len = 0;

			int status = load_block(&job->ds->mf.chunks[job->index], &buf, &len);

			pthread_mutex_lock(&job->ds->mu);
			if (status < 0) {
				job->ds->error_flag = 1;
			} else {
				job->ds->bufs[job->index] = buf;
				job->ds->lens[job->index] = (uint32_t)len;
				job->ds->ready[job->index] = 1;
			}
			pthread_cond_broadcast(&job->ds->cv);
			pthread_mutex_unlock(&job->ds->mu);

			if (status < 0 && buf) free(buf);
			free(job);

		} else {
			free(job);
		}
	}
	return NULL;
}

static void upload_state_reset(upload_state_t *up) {
	if (!up) return;
	if (up->active) {
		pthread_mutex_destroy(&up->mu);
		pthread_cond_destroy(&up->cv);
	}

	free(up->mf.chunks);
	memset(up, 0, sizeof(*up));
}

static void free_frame(frame_t *fr) {
	if (fr && fr->payload) free(fr->payload);
	if (fr) memset(fr, 0, sizeof(*fr));
}

static void* handle_connection(void *arg) {
	int cfd = (int)(intptr_t)arg;

	upload_state_t up;
	memset(&up, 0, sizeof(up));

	uint32_t next_index = 0;

	uint32_t recv_chunks = 0;

	for (;;) {
		frame_t fr;
		memset(&fr, 0, sizeof(fr));

		int rr = recv_frame(cfd, &fr);
		if (rr == 0) break;

		if (rr < 0) {
			fprintf(stderr, "[ENGINE] recv_frame failed\n");
			break;
		}

		uint8_t op = fr.op;
		uint8_t *payload = fr.payload;
		uint32_t len = fr.len;

		if (op == OP_UPLOAD_START) {
			if (up.active) {
				send_error(cfd, "E_PROTO", "UPLOAD already active");
				free_frame(&fr);
				break;
			}
			if (len == 0 || len > MAX_FILENAME_LEN) {
				send_error(cfd, "E_PROTO", "bad filename length");
				free_frame(&fr);
				break;
			}

			memset(&up, 0, sizeof(up));
			up.active = 1;
			pthread_mutex_init(&up.mu, NULL);
			pthread_cond_init(&up.cv, NULL);
			up.pending = 0;
			up.error = 0;

			up.mf.version = 1;
			strncpy(up.mf.hash_algo, "blake3", sizeof(up.mf.hash_algo)-1);

			up.mf.chunk_size = DEFAULT_CHUNK_SIZE;
			up.mf.total_size = 0;
			up.mf.chunks = NULL;
			up.mf.chunk_count = 0;
			up.mf.chunk_cap = 0;

			size_t n = len;
			if (n >= sizeof(up.mf.filename)) n = sizeof(up.mf.filename) - 1;
			memcpy(up.mf.filename, payload, n);
			up.mf.filename[n] = '\0';

			next_index = 0;
			recv_chunks = 0;

		} else if (op == OP_UPLOAD_CHUNK) {
			if (!up.active) {
				send_error(cfd, "E_PROTO", "UPLOAD_CHUNK without UPLOAD_START");
				free_frame(&fr);
				break;
			}
			if (len == 0 || len > DEFAULT_CHUNK_SIZE) {
				send_error(cfd, "E_PROTO", "bad chunk size");
				free_frame(&fr);
				break;
			}

			job_t *job = (job_t*)calloc(1, sizeof(*job));
			if (!job) {
				send_error(cfd, "E_MEM", "malloc job failed");
				free_frame(&fr);
				break;
			}

			job->type = JOB_UPLOAD_HASH_STORE;

			job->up = &up;
			job->index = next_index++;
			job->len = len;

			job->data = (uint8_t*)malloc(len);
			if (!job->data) {
				free(job);
				send_error(cfd, "E_MEM", "malloc job data failed");
				free_frame(&fr);
				break;
			}
			memcpy(job->data, payload, len);

			pthread_mutex_lock(&up.mu);
			up.pending++;
			pthread_mutex_unlock(&up.mu);

			enqueue_job(job);
			recv_chunks++;

		} else if (op == OP_UPLOAD_FINISH) {
			if (!up.active) {
				send_error(cfd, "E_PROTO", "UPLOAD_FINISH without UPLOAD_START");
				free_frame(&fr);
				break;
			}
			if (recv_chunks == 0) {
				send_error(cfd, "E_PROTO", "UPLOAD_FINISH but no chunks received");
				upload_state_reset(&up);
				free_frame(&fr);
				break;
			}

			pthread_mutex_lock(&up.mu);
			while (up.pending > 0 && !up.error) {
				pthread_cond_wait(&up.cv, &up.mu);
			}
			int had_error = up.error;
			pthread_mutex_unlock(&up.mu);

			if (had_error) {
				send_error(cfd, "E_IO", "upload failed");
				upload_state_reset(&up);
				free_frame(&fr);
				break;
			}

			qsort(up.mf.chunks, up.mf.chunk_count, sizeof(up.mf.chunks[0]), cmp_chunk_index);

			size_t manifest_len = 0;
			char *manifest = build_manifest_json(&up.mf, &manifest_len);
			if (!manifest) {
				send_error(cfd, "E_IO", "manifest build failed");
				upload_state_reset(&up);
				free_frame(&fr);
				break;
			}

			char cid[128];
			compute_cid(&up.mf, manifest, manifest_len, cid);

			if (write_manifest_atomic(cid, manifest, manifest_len) < 0) {
				free(manifest);
				send_error(cfd, "E_IO", "manifest write failed");
				upload_state_reset(&up);
				free_frame(&fr);
				break;
			}

			free(manifest);

			send_frame(cfd, OP_UPLOAD_DONE, cid, (uint32_t)strlen(cid));

			upload_state_reset(&up);
			next_index = 0;
			recv_chunks = 0;

		} else if (op == OP_DOWNLOAD_START) {
			if (len == 0 || len >= 128) {
				send_error(cfd, "E_BAD_CID", "bad CID length");
				free_frame(&fr);
				break;
			}

			char cid[129];
			memcpy(cid, payload, len);
			cid[len] = '\0';

			manifest_t mf;
			if (load_manifest(cid, &mf) < 0) {
				send_error(cfd, "E_NOT_FOUND", "manifest not found");
				free_frame(&fr);
				break;
			}

			qsort(mf.chunks, mf.chunk_count, sizeof(mf.chunks[0]), cmp_chunk_index);

			download_state_t ds;
			memset(&ds, 0, sizeof(ds));
			pthread_mutex_init(&ds.mu, NULL);
			pthread_cond_init(&ds.cv, NULL);
			ds.error_flag = 0;
			ds.next_to_send = 0;

			ds.total_chunks = mf.chunk_count;
			ds.bufs  = (uint8_t**)calloc(ds.total_chunks, sizeof(uint8_t*));
			ds.lens  = (uint32_t*)calloc(ds.total_chunks, sizeof(uint32_t));
			ds.ready = (uint8_t*)calloc(ds.total_chunks, sizeof(uint8_t));
			if (!ds.bufs || !ds.lens || !ds.ready) {
				send_error(cfd, "E_MEM", "download alloc failed");
				free(mf.chunks);
				free(ds.bufs);
				free(ds.lens);
				free(ds.ready);
				pthread_mutex_destroy(&ds.mu);
				pthread_cond_destroy(&ds.cv);
				free_frame(&fr);
				break;
			}

			ds.mf = mf;

			for (size_t i = 0; i < ds.total_chunks; i++) {
				job_t *job = (job_t*)calloc(1, sizeof(*job));
				if (!job) {
					pthread_mutex_lock(&ds.mu);
					ds.error_flag = 1;
					pthread_cond_broadcast(&ds.cv);
					pthread_mutex_unlock(&ds.mu);
					break;
				}
				job->type = JOB_DOWNLOAD_READ_VERIFY;
				job->ds = &ds;
				job->index = (uint32_t)i;

				enqueue_job(job);
			}

			int ok = 1;
			for (ds.next_to_send = 0; ds.next_to_send < ds.total_chunks; ds.next_to_send++) {
				pthread_mutex_lock(&ds.mu);
				while (!ds.ready[ds.next_to_send] && !ds.error_flag) {
					pthread_cond_wait(&ds.cv, &ds.mu);
				}
				int err = ds.error_flag;
				uint8_t *buf = ds.bufs[ds.next_to_send];
				uint32_t blen = ds.lens[ds.next_to_send];
				pthread_mutex_unlock(&ds.mu);

				if (err || !buf) {
					send_error(cfd, "E_IO", "failed to load/verify block");
					ok = 0;
					break;
				}

				if (send_frame(cfd, OP_DOWNLOAD_CHUNK, buf, blen) < 0) {
					ok = 0;
					break;
				}

				free(buf);
				pthread_mutex_lock(&ds.mu);
				ds.bufs[ds.next_to_send] = NULL;
				pthread_mutex_unlock(&ds.mu);
			}

			if (ok) send_frame(cfd, OP_DOWNLOAD_DONE, NULL, 0);

			pthread_mutex_lock(&ds.mu);
			for (size_t i = 0; i < ds.total_chunks; i++) {
				if (ds.bufs[i]) free(ds.bufs[i]);
			}
			pthread_mutex_unlock(&ds.mu);

			free(ds.bufs);
			free(ds.lens);
			free(ds.ready);

			free(ds.mf.chunks);

			pthread_mutex_destroy(&ds.mu);
			pthread_cond_destroy(&ds.cv);

		} else {
			send_error(cfd, "E_PROTO", "unknown opcode");
			free_frame(&fr);
			break;
		}

		free_frame(&fr);
	}

	upload_state_reset(&up);
	close(cfd);
	return NULL;
}

int main(int argc, char** argv) {
	if (argc != 2) {
		fprintf(stderr, "usage: %s /tmp/cengine.sock\n", argv[0]);
		return 2;
	}
	g_sock_path = argv[1];

	if (mkdir_p(DATA_ROOT, 0777) < 0) return 2;
	if (mkdir_p(BLOCKS_DIR, 0777) < 0) return 2;
	if (mkdir_p(MANIFESTS_DIR, 0777) < 0) return 2;

	for (int i = 0; i < N_WORKERS; i++) {
		pthread_t wth;
		int rc = pthread_create(&wth, NULL, worker_main, NULL);
		if (rc != 0) {
			fprintf(stderr, "pthread_create(worker) failed: %d\n", rc);
			return 2;
		}
		pthread_detach(wth);
	}

	int fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0) {
		perror("socket");
		return 2;
	}

	struct sockaddr_un addr;
	memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	strncpy(addr.sun_path, g_sock_path, sizeof(addr.sun_path) - 1);

	unlink(g_sock_path);

	if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
		perror("bind");
		close(fd);
		return 2;
	}

	chmod(g_sock_path, 0777);

	if (listen(fd, 64) < 0) {
		perror("listen");
		close(fd);
		unlink(g_sock_path);
		return 2;
	}

	printf("[ENGINE] listening on %s\n", g_sock_path);
	fflush(stdout);

	for (;;) {
		int cfd = accept(fd, NULL, NULL);
		if (cfd < 0) {
			if (errno == EINTR) continue;
			perror("accept");
			break;
		}

		pthread_t th;
		int rc = pthread_create(&th, NULL, handle_connection, (void*)(intptr_t)cfd);
		if (rc != 0) {
			fprintf(stderr, "pthread_create(conn) failed: %d\n", rc);
			close(cfd);
			continue;
		}
		pthread_detach(th);
	}

	close(fd);
	unlink(g_sock_path);
	return 0;
}