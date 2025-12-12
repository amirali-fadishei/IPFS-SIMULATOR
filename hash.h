#ifndef HASH_H
#define HASH_H

#include <stddef.h>
#include <stdint.h>

#define HASH_LEN 32

void hash_bytes(const uint8_t *data, size_t len, uint8_t out[HASH_LEN]);

#endif
