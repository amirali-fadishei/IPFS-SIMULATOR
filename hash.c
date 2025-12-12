#include "hash.h"
#include "blake3/blake3.h"

void hash_bytes(const uint8_t *data, size_t len, uint8_t out[HASH_LEN]) {
    blake3_hasher hasher;

    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, data, len);
    blake3_hasher_finalize(&hasher, out, HASH_LEN);
}
