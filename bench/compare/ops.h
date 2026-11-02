// Decoding of a batch packed by encodeOps (stores.go), shared by the C
// stores: [kind u8 (0 put, 1 delete)][key len u32 LE][value len u32 LE][key][value]...
#pragma once
#include <stddef.h>
#include <stdint.h>
#include <string.h>

typedef struct {
	int del;
	const char *k, *v;
	size_t kl, vl;
} bench_op;

// bench_next_op decodes the op at *pos and advances it. Returns 0 at the end.
static inline int bench_next_op(const char *buf, size_t len, size_t *pos, bench_op *o) {
	uint32_t kl, vl;
	if (*pos >= len) return 0;
	o->del = buf[*pos] == 1;
	memcpy(&kl, buf + *pos + 1, 4);
	memcpy(&vl, buf + *pos + 5, 4);
	o->k = buf + *pos + 9;
	o->kl = kl;
	o->v = o->k + kl;
	o->vl = vl;
	*pos += 9 + (size_t)kl + vl;
	return 1;
}
