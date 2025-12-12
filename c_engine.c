// Build: gcc -O2 -pthread -o c_engine c_engine.c
// Run:   ./c_engine /tmp/cengine.sock

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
#include "hash.h"


#define OP_UPLOAD_START  0x01
#define OP_UPLOAD_CHUNK  0x02
#define OP_UPLOAD_FINISH 0x03
#define OP_UPLOAD_DONE   0x81
#define OP_DOWNLOAD_START 0x11
#define OP_DOWNLOAD_CHUNK 0x91
#define OP_DOWNLOAD_DONE  0x92
#define OP_ERROR 0xFF

#define MAX_FRAME_SIZE (4 * 1024 * 1024)
#define MAX_FILENAME_LEN 255

#define DEFAULT_CHUNK_SIZE (256 * 1024)

#define N_WORKERS 4

static const char* DATA_ROOT = "./data";
static const char* BLOCKS_DIR = "./data/blocks";
static const char* MANIFESTS_DIR = "./data/manifests";
static const char* g_sock_path = NULL;

typedef struct {
	int active;
	char filename[256];

	size_t total_size;

	struct chunk_info {
		uint32_t index;
		uint32_t size;
		char hash[65];
	} *chunks;

	size_t chunk_count;
	size_t chunk_cap;
} upload_state_t;

typedef struct {
	char filename[256];
	size_t total_size;
	struct chunk_info *chunks;
	size_t chunk_count;
	size_t chunk_cap;
} manifest_state_t;

typedef enum {
	JOB_UPLOAD_STORE,
	JOB_DOWNLOAD_LOAD
} job_type_t;

typedef struct job {
	job_type_t type;
	pthread_mutex_t mu;
	pthread_cond_t  cv;
	int done;
	int status;
	uint8_t *data;
	size_t len;
	char hash_hex[65];
	struct chunk_info *ci;
	struct job *next;
} job_t;

static job_t *job_head = NULL;
static job_t *job_tail = NULL;
static pthread_mutex_t job_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  job_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t blocks_mu = PTHREAD_MUTEX_INITIALIZER;

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

static void digest_to_hex(const uint8_t *digest, char *out_hex, size_t digest_len) {
	static const char *hex = "0123456789abcdef";
	for (size_t i = 0; i < digest_len; i++) {
		out_hex[2*i]     = hex[(digest[i] >> 4) & 0xF];
		out_hex[2*i + 1] = hex[digest[i] & 0xF];
	}
	out_hex[2*digest_len] = '\0';
}

int write_all(int fd, const void* buf, size_t n) {
	size_t sent = 0;
	while (sent < n) {
		ssize_t w = write(fd, (const char*)buf + sent, n - sent);
		if (w < 0) {
			if (errno == EINTR) continue;
			perror("write");
			return -1;
		}
		sent += (size_t)w;
	}
	return 0;
}

int send_frame(int fd, uint8_t op, const void* payload, uint32_t len) {
	uint8_t header[5];
	header[0] = op;
	uint32_t be_len = htonl(len);
	memcpy(header + 1, &be_len, 4);
	if (write_all(fd, header, 5) < 0) return -1;
	if (len && write_all(fd, payload, len) < 0) return -1;
	return 0;
}

static int send_error(int cfd, const char *code, const char *message) {
    char buf[256];
    int n = snprintf(buf, sizeof(buf),
                     "{\"code\":\"%s\",\"message\":\"%s\"}",
                     code, message ? message : "");
    if (n < 0) return -1;
    if (n >= (int)sizeof(buf)) n = (int)sizeof(buf) - 1;
    return send_frame(cfd, OP_ERROR, buf, (uint32_t)n);
}

static int send_upload_error(int cfd, const char *msg) {
    fprintf(stderr, "[ENGINE] protocol error: %s\n", msg);
    fflush(stderr);
    send_error(cfd, "E_PROTO", msg);
    return -1;
}

ssize_t read_n(int fd, void* buf, size_t n) {
	size_t got = 0;
	while (got < n) {
		ssize_t r = read(fd, (char*)buf + got, n - got);
		if (r == 0) return 0;
		if (r < 0) {
			if (errno == EINTR) continue;
			perror("read");
			return -1;
		}
		got += r;
	}
	return (ssize_t)got;
}

static int ensure_dir(const char *path) {
	if (mkdir(path, 0777) < 0) {
		if (errno == EEXIST) return 0;
		perror("mkdir");
		return -1;
	}
	return 0;
}

static int store_block(const char *hash_hex, const uint8_t *data, size_t len) {
	pthread_mutex_lock(&blocks_mu);
	int rc = -1;

    char aa[3], bb[3];
    aa[0] = hash_hex[0];
    aa[1] = hash_hex[1];
    aa[2] = '\0';

    bb[0] = hash_hex[2];
    bb[1] = hash_hex[3];
    bb[2] = '\0';

    char dir[PATH_MAX];
    int n;

    n = snprintf(dir, sizeof(dir), "%s/%s", BLOCKS_DIR, aa);
    if (n < 0 || n >= (int)sizeof(dir)) {
        errno = ENAMETOOLONG;
        goto out;
    }
    if (ensure_dir(dir) < 0) goto out;

    n = snprintf(dir, sizeof(dir), "%s/%s/%s", BLOCKS_DIR, aa, bb);
    if (n < 0 || n >= (int)sizeof(dir)) {
        errno = ENAMETOOLONG;
        goto out;
    }
    if (ensure_dir(dir) < 0) goto out;

    char path[PATH_MAX];
    n = snprintf(path, sizeof(path), "%s/%s/%s/%s", BLOCKS_DIR, aa, bb, hash_hex);
    if (n < 0 || n >= (int)sizeof(path)) {
        errno = ENAMETOOLONG;
        goto out;
    }

    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) {
        if (errno == EEXIST) {
            rc = 0;
            goto out;
        }
        perror("open block");
        goto out;
    }

    if (write_all(fd, data, len) < 0) {
        close(fd);
        unlink(path);
        goto out;
    }
    if (fsync(fd) < 0) {
        perror("fsync block");
    }
    close(fd);
	rc = 0;

out:
    pthread_mutex_unlock(&blocks_mu);
    return rc;
}

static int load_block(const struct chunk_info *ci, uint8_t **out_data, size_t *out_len) {
	const char *hash_hex = ci->hash;

	char aa[3], bb[3];
	aa[0] = hash_hex[0];
	aa[1] = hash_hex[1];
	aa[2] = '\0';

	bb[0] = hash_hex[2];
	bb[1] = hash_hex[3];
	bb[2] = '\0';

	char path[PATH_MAX];
	int n = snprintf(path, sizeof(path), "%s/%s/%s/%s",
	                 BLOCKS_DIR, aa, bb, hash_hex);
	if (n < 0 || n >= (int)sizeof(path)) {
		errno = ENAMETOOLONG;
		return -1;
	}

	int fd = open(path, O_RDONLY);
	if (fd < 0) {
		perror("open block for read");
		return -1;
	}

	size_t expected = ci->size;
	uint8_t *buf = malloc(expected);
	if (!buf) {
		perror("malloc block");
		close(fd);
		return -1;
	}

	if (read_n(fd, buf, expected) <= 0) {
		perror("read block");
		free(buf);
		close(fd);
		return -1;
	}
	close(fd);

	uint8_t digest[HASH_LEN];
	char check_hash[HASH_LEN*2 + 1];

	hash_bytes(buf, expected, digest);
	digest_to_hex(digest, check_hash, HASH_LEN);

	if (strncmp(check_hash, hash_hex, HASH_LEN*2) != 0) {
		fprintf(stderr, "[ENGINE] HASH MISMATCH for block %s (expected %s, got %s)\n", path, hash_hex, check_hash);
		free(buf);
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
		while (!job_head) {
			pthread_cond_wait(&job_cv, &job_mu);
		}
		job_t *job = job_head;
		job_head = job->next;
		if (!job_head) job_tail = NULL;
		pthread_mutex_unlock(&job_mu);

		if (job->type == JOB_UPLOAD_STORE) {
			uint8_t digest[HASH_LEN];
			hash_bytes(job->data, job->len, digest);
			digest_to_hex(digest, job->hash_hex, HASH_LEN);
			if (store_block(job->hash_hex, job->data, job->len) < 0) {
				job->status = -1;
			} else {
				job->status = 0;
			}
			free(job->data);
			job->data = NULL;
		} else if (job->type == JOB_DOWNLOAD_LOAD) {
			if (load_block(job->ci, &job->data, &job->len) < 0) {
				job->status = -1;
			} else {
				job->status = 0;
			}
		} else {
			job->status = -1;
		}

		pthread_mutex_lock(&job->mu);
		job->done = 1;
		pthread_cond_signal(&job->cv);
		pthread_mutex_unlock(&job->mu);
	}
	return NULL;
}

static char* load_file(const char *path, size_t *out_len) {
	int fd = open(path, O_RDONLY);
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
	size_t size = (size_t)st.st_size;
	char *buf = malloc(size + 1);
	if (!buf) {
		perror("malloc load_file");
		close(fd);
		return NULL;
	}
	if (read_n(fd, buf, size) <= 0) {
		perror("read load_file");
		free(buf);
		close(fd);
		return NULL;
	}
	close(fd);
	buf[size] = '\0';
	if (out_len) *out_len = size;
	return buf;
}

static int add_chunk(upload_state_t *up, uint32_t index, uint32_t size, const char hash_hex[65]) {
	if (up->chunk_count == up->chunk_cap) {
		size_t new_cap = up->chunk_cap ? up->chunk_cap * 2 : 16;
		struct chunk_info *n =
		    realloc(up->chunks, new_cap * sizeof(struct chunk_info));
		if (!n) {
			perror("realloc chunks");
			return -1;
		}
		up->chunks = n;
		up->chunk_cap = new_cap;
	}

	struct chunk_info *ci = &up->chunks[up->chunk_count];
	ci->index = index;
	ci->size = size;
	strncpy(ci->hash, hash_hex, 65);
	ci->hash[64] = '\0';

	up->chunk_count++;
	up->total_size += size;
	return 0;
}

static int manifest_add_chunk(manifest_state_t *mf, uint32_t index, uint32_t size, const char hash_hex[65]) {
	if (mf->chunk_count == mf->chunk_cap) {
		size_t new_cap = mf->chunk_cap ? mf->chunk_cap * 2 : 16;
		struct chunk_info *n =
		    realloc(mf->chunks, new_cap * sizeof(struct chunk_info));
		if (!n) {
			perror("realloc manifest chunks");
			return -1;
		}
		mf->chunks = n;
		mf->chunk_cap = new_cap;
	}
	struct chunk_info *ci = &mf->chunks[mf->chunk_count];
	ci->index = index;
	ci->size  = size;
	strncpy(ci->hash, hash_hex, 65);
	ci->hash[64] = '\0';
	mf->chunk_count++;
	return 0;
}

static char* build_manifest_json(const upload_state_t *up, size_t *out_len) {
	size_t cap = 1024 + up->chunk_count * 128;
	char *buf = malloc(cap);
	if (!buf) {
		perror("malloc manifest");
		return NULL;
	}
	size_t used = 0;

#define APPEND_FMT(...) \
        do { \
            int n = snprintf(buf + used, cap - used, __VA_ARGS__); \
            if (n < 0) { free(buf); return NULL; } \
            if ((size_t)n >= cap - used) { \
                 \
                cap = cap * 2 + (size_t)n; \
                char *nb = realloc(buf, cap); \
                if (!nb) { free(buf); perror("realloc manifest"); return NULL; } \
                buf = nb; \
                int n2 = snprintf(buf + used, cap - used, __VA_ARGS__); \
                if (n2 < 0) { free(buf); return NULL; } \
                n = n2; \
            } \
            used += (size_t)n; \
        } while (0)

	APPEND_FMT("{\n");
	APPEND_FMT("  \"version\": 1,\n");
	APPEND_FMT("  \"hash_algo\": \"blake3\",\n");
	APPEND_FMT("  \"chunk_size\": %u,\n", (unsigned)DEFAULT_CHUNK_SIZE);
	APPEND_FMT("  \"total_size\": %zu,\n", up->total_size);
	APPEND_FMT("  \"filename\": \"%s\",\n", up->filename);
	APPEND_FMT("  \"chunks\": [\n");

	for (size_t i = 0; i < up->chunk_count; i++) {
		const struct chunk_info *ci = &up->chunks[i];
		APPEND_FMT(
		    "    { \"index\": %u, \"size\": %u, \"hash\": \"%s\" }%s\n",
		    ci->index, ci->size, ci->hash,
		    (i + 1 < up->chunk_count) ? "," : ""
		);
	}

	APPEND_FMT("  ]\n");
	APPEND_FMT("}\n");

#undef APPEND_FMT

	*out_len = used;
	return buf;
}

static int load_manifest(const char *cid, manifest_state_t *mf) {
	memset(mf, 0, sizeof(*mf));

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/%s.json", MANIFESTS_DIR, cid);

	size_t mlen = 0;
	char *buf = load_file(path, &mlen);
	if (!buf) {
		fprintf(stderr, "[ENGINE] load_manifest: cannot read %s\n", path);
		return -1;
	}

	char *fname = strstr(buf, "\"filename\"");
	if (fname) {
		char *quote1 = strchr(fname, '"');
		if (quote1) {
			char *quote2 = strchr(quote1 + 1, '"');
			if (quote2) {
				char *quote3 = strchr(quote2 + 1, '"');
				if (quote3) {
					char *quote4 = strchr(quote3 + 1, '"');
					if (quote4) {
						size_t len = (size_t)(quote4 - (quote3 + 1));
						if (len >= sizeof(mf->filename))
							len = sizeof(mf->filename) - 1;
						memcpy(mf->filename, quote3 + 1, len);
						mf->filename[len] = '\0';
					}
				}
			}
		}
	}

	char *ts = strstr(buf, "\"total_size\"");
	if (ts) {
		char *colon = strchr(ts, ':');
		if (colon) {
			mf->total_size = strtoull(colon + 1, NULL, 10);
		}
	}

	char *chunks = strstr(buf, "\"chunks\"");
	if (!chunks) {
		fprintf(stderr, "[ENGINE] load_manifest: no chunks[] in manifest\n");
		free(buf);
		return -1;
	}
	char *bracket = strchr(chunks, '[');
	if (!bracket) {
		fprintf(stderr, "[ENGINE] load_manifest: malformed chunks[]\n");
		free(buf);
		return -1;
	}

	char *p = bracket;
	while (1) {
		char *brace_start = strchr(p, '{');
		if (!brace_start) break;

		char *brace_end = strchr(brace_start, '}');
		if (!brace_end) {
			fprintf(stderr, "[ENGINE] load_manifest: unmatched '{'\n");
			free(buf);
			free(mf->chunks);
			memset(mf, 0, sizeof(*mf));
			return -1;
		}

		char chunk_buf[256];
		size_t clen = (size_t)(brace_end - brace_start + 1);
		if (clen >= sizeof(chunk_buf)) clen = sizeof(chunk_buf) - 1;
		memcpy(chunk_buf, brace_start, clen);
		chunk_buf[clen] = '\0';

		uint32_t index = 0;
		uint32_t size  = 0;
		char hash_hex[65];

		int n = sscanf(chunk_buf,
		               " { \"index\" : %u , \"size\" : %u , \"hash\" : \"%64[^\"]\" }",
		               &index, &size, hash_hex);
		if (n == 3) {
			if (manifest_add_chunk(mf, index, size, hash_hex) < 0) {
				free(buf);
				free(mf->chunks);
				memset(mf, 0, sizeof(*mf));
				return -1;
			}
		} else {
			n = sscanf(chunk_buf,
			           " { \"index\": %u, \"size\": %u, \"hash\": \"%64[^\"]\" }",
			           &index, &size, hash_hex);
			if (n == 3) {
				if (manifest_add_chunk(mf, index, size, hash_hex) < 0) {
					free(buf);
					free(mf->chunks);
					memset(mf, 0, sizeof(*mf));
					return -1;
				}
			} else {
				fprintf(stderr, "[ENGINE] load_manifest: failed to parse chunk: %s\n",
				        chunk_buf);
			}
		}

		p = brace_end + 1;
	}

	free(buf);
	return 0;
}

static void compute_cid(const char *manifest, size_t mlen, char cid_buf[128]) {
	uint8_t digest[HASH_LEN];
	char    hex[HASH_LEN*2 + 1];

	hash_bytes((const uint8_t*)manifest, mlen, digest);
	digest_to_hex(digest, hex, HASH_LEN);

	snprintf(cid_buf, 128, "%s", hex);
}

static int write_manifest_atomic(const char *cid, const char *manifest, size_t mlen) {
	char final_path[PATH_MAX];
	char tmp_path[PATH_MAX];

	snprintf(final_path, sizeof(final_path),
	         "%s/%s.json", MANIFESTS_DIR, cid);
	snprintf(tmp_path, sizeof(tmp_path),
	         "%s/.%s.tmp", MANIFESTS_DIR, cid);

	int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (fd < 0) {
		perror("open manifest tmp");
		return -1;
	}

	if (write_all(fd, manifest, mlen) < 0) {
		close(fd);
		unlink(tmp_path);
		return -1;
	}
	if (fsync(fd) < 0) {
		perror("fsync manifest");
	}
	close(fd);

	if (rename(tmp_path, final_path) < 0) {
		perror("rename manifest");
		unlink(tmp_path);
		return -1;
	}
	return 0;
}

static void* handle_connection(void *arg) {
	int cfd = (int)(intptr_t)arg;
	upload_state_t up;
	memset(&up, 0, sizeof(up));
	for (;;) {
		uint8_t header[5];
		ssize_t r = read_n(cfd, header, 5);
		if (r == 0) break;
		if (r < 0) {
			fprintf(stderr, "[ENGINE] read header failed\n");
			break;
		}
		uint8_t op = header[0];
		uint32_t len;
		memcpy(&len, header + 1, 4);
		len = ntohl(len);
		if (len > MAX_FRAME_SIZE) {
			fprintf(stderr, "[ENGINE] frame too large: %u bytes\n", len);
			break;
		}
		uint8_t* payload = NULL;
		if (len) {
			payload = (uint8_t*)malloc(len);
			if (!payload) {
				perror("[ENGINE] malloc payload");
				break;
			}
			if (read_n(cfd, payload, len) <= 0) {
				perror("[ENGINE] read payload");
				free(payload);
				break;
			}
		}

		if (op == OP_UPLOAD_START) {
			if (up.active) {
				send_upload_error(cfd, "ERROR-PROTOCOL-UPLOAD-ALREADY-ACTIVE");
				free(payload);
				break;
			}

			if (len == 0 || len > MAX_FILENAME_LEN) {
				send_upload_error(cfd, "ERROR-PROTOCOL-BAD-FILENAME");
				free(payload);
				break;
			}
			printf("[ENGINE] UPLOAD_START: name=\"%.*s\"\n", (int)len, (char*)payload);
			fflush(stdout);

			up.active = 1;
			up.total_size = 0;
			up.chunk_count = 0;
			up.chunk_cap = 0;
			up.chunks = NULL;

			size_t n = len;
			if (n >= sizeof(up.filename)) n = sizeof(up.filename) - 1;
			memcpy(up.filename, payload, n);
			up.filename[n] = '\0';
		} else if (op == OP_UPLOAD_CHUNK) {
			if (!up.active) {
				fprintf(stderr, "[ENGINE] UPLOAD_CHUNK but no upload active\n");
				send_upload_error(cfd, "ERROR-PROTOCOL-CHUNK-WITHOUT-START");
				free(payload);
				break;
			}

			if (len == 0 || len > DEFAULT_CHUNK_SIZE) {
				send_upload_error(cfd, "ERROR-PROTOCOL-BAD-CHUNK-SIZE");
				free(payload);
				break;
			}

			uint32_t index = (uint32_t)up.chunk_count;
			uint32_t size = (uint32_t)len;

			job_t *job = (job_t*)malloc(sizeof(job_t));
			if (!job) {
				perror("malloc job");
				free(payload);
				payload = NULL;
			} else {
				memset(job, 0, sizeof(*job));
				job->type = JOB_UPLOAD_STORE;
				pthread_mutex_init(&job->mu, NULL);
				pthread_cond_init(&job->cv, NULL);
				job->done = 0;
				job->status = -1;

				job->data = payload;
				job->len  = len;

				payload = NULL;

				enqueue_job(job);

				pthread_mutex_lock(&job->mu);
				while (!job->done) {
					pthread_cond_wait(&job->cv, &job->mu);
				}
				pthread_mutex_unlock(&job->mu);

				if (job->status == 0) {
					if (add_chunk(&up, index, size, job->hash_hex) < 0) {
						fprintf(stderr, "[ENGINE] failed to add chunk\n");
					}
				} else {
					fprintf(stderr, "[ENGINE] worker failed for upload chunk\n");
				}

				pthread_mutex_destroy(&job->mu);
				pthread_cond_destroy(&job->cv);
				free(job);
			}
		} else if (op == OP_UPLOAD_FINISH) {
			if (!up.active) {
				fprintf(stderr, "[ENGINE] UPLOAD_FINISH but no upload active\n");
				send_upload_error(cfd, "ERROR-NO-UPLOAD");
			} else if (up.chunk_count == 0) {
				fprintf(stderr, "[ENGINE] UPLOAD_FINISH but no chunks\n");
				send_upload_error(cfd, "ERROR-PROTOCOL-NO-CHUNKS");
				free(up.chunks);
				memset(&up, 0, sizeof(up));
			} else {
				size_t manifest_len = 0;
				char *manifest = build_manifest_json(&up, &manifest_len);
				if (!manifest) {
					const char* errcid = "ERROR-MANIFEST";
					send_frame(cfd, OP_UPLOAD_DONE, errcid, (uint32_t)strlen(errcid));
				} else {
					char cid[128];
					compute_cid(manifest, manifest_len, cid);

					if (write_manifest_atomic(cid, manifest, manifest_len) < 0) {
						fprintf(stderr, "[ENGINE] failed to write manifest\n");
						const char* errcid = "ERROR-MANIFEST-WRITE";
						send_frame(cfd, OP_UPLOAD_DONE, errcid, (uint32_t)strlen(errcid));
					} else {
						printf("[ENGINE] UPLOAD_FINISH -> returning CID %s\n", cid);
						fflush(stdout);
						send_frame(cfd, OP_UPLOAD_DONE, cid, (uint32_t)strlen(cid));
					}
					free(manifest);
				}
				free(up.chunks);
				memset(&up, 0, sizeof(up));
			}
		} else if (op == OP_DOWNLOAD_START) {
			printf("[ENGINE] DOWNLOAD_START: cid=\"%.*s\"\n", (int)len, (char*)payload);
			fflush(stdout);

			if (len == 0 || len >= 128) {
				fprintf(stderr, "[ENGINE] DOWNLOAD_START: bad CID length %u\n", len);
    			send_error(cfd, "E_BAD_CID", "bad CID length");
    			free(payload);
    			break;
			}

			char cid[129];
			size_t cid_len = len;
			memcpy(cid, payload, cid_len);
			cid[cid_len] = '\0';

			manifest_state_t mf;
			if (load_manifest(cid, &mf) < 0) {
				fprintf(stderr, "[ENGINE] DOWNLOAD_START: failed to load manifest for CID %s\n", cid);
    			send_error(cfd, "E_NOT_FOUND", "manifest not found");
    			free(payload);
    			break;
			} else {
				for (size_t i = 0; i < mf.chunk_count; i++) {
					struct chunk_info *ci = &mf.chunks[i];

					job_t *job = (job_t*)malloc(sizeof(job_t));
					if (!job) {
						perror("malloc job");
						break;
					}
					memset(job, 0, sizeof(*job));
					job->type = JOB_DOWNLOAD_LOAD;
					pthread_mutex_init(&job->mu, NULL);
					pthread_cond_init(&job->cv, NULL);
					job->done = 0;
					job->status = -1;

					job->ci = ci;
					enqueue_job(job);

					pthread_mutex_lock(&job->mu);
					while (!job->done) {
						pthread_cond_wait(&job->cv, &job->mu);
					}
					pthread_mutex_unlock(&job->mu);

					if (job->status == 0) {
						if (send_frame(cfd, OP_DOWNLOAD_CHUNK, job->data, (uint32_t)job->len) < 0) {
							fprintf(stderr, "[ENGINE] failed to send DOWNLOAD_CHUNK\n");
							free(job->data);
							pthread_mutex_destroy(&job->mu);
							pthread_cond_destroy(&job->cv);
							free(job);
							break;
						}
						free(job->data);
					} else {
						fprintf(stderr, "[ENGINE] worker failed for download chunk %s\n", ci->hash);
						pthread_mutex_destroy(&job->mu);
						pthread_cond_destroy(&job->cv);
						free(job);
						break;
					}

					pthread_mutex_destroy(&job->mu);
					pthread_cond_destroy(&job->cv);
					free(job);
				}
				send_frame(cfd, OP_DOWNLOAD_DONE, NULL, 0);
				free(mf.chunks);
			}
		} else {
			fprintf(stderr, "[ENGINE] unknown opcode 0x%02x, closing connection\n", op);
			free(payload);
			break;
		}

		free(payload);
	}
	free(up.chunks);
	close(cfd);
	return NULL;
}

int main(int argc, char** argv) {
	if (argc != 2) {
		fprintf(stderr, "usage: %s /tmp/cengine.sock\n", argv[0]);
		return 2;
	}
	g_sock_path = argv[1];

	if (ensure_dir(DATA_ROOT) < 0) return 2;
	if (ensure_dir(BLOCKS_DIR) < 0) return 2;
	if (ensure_dir(MANIFESTS_DIR) < 0) return 2;

	for (int i = 0; i < N_WORKERS; i++) {
		pthread_t wth;
		pthread_create(&wth, NULL, worker_main, NULL);
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
		return 2;
	}
	if (listen(fd, 64) < 0) {
		perror("listen");
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
		pthread_create(&th, NULL, handle_connection, (void*)(intptr_t)cfd);
		pthread_detach(th);
	}

	close(fd);
	unlink(g_sock_path);
	return 0;
}