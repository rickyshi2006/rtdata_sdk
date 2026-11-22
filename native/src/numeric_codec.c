#include <fenv.h>
#include <float.h>
#include <limits.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>

#ifdef _WIN32
#define API __declspec(dllexport)
#else
#define API __attribute__((visibility("default")))
#endif

#define MAX_RAW 65536u
#define MAX_TRANSFORMED 131072u
#define MAX_FRAMES 256u

static const char *const schemas[] = {
    "",
    "sqddddddqdqdddidididididididididids",
    "sqddddddqdqdddididididididididididdqdcsiisdidsc",
    "sqddddddqdididi",
    "sqddddddqdddqdqdqdqdqdqdqdqdqdqdqdqdqdqdqdqdqdqdqdqiqsqiiiirqqdddd",
    "sqddddddqdddqdqdqdqdqdqdqdqdqdqdqdqdqdqdqdqdqdqdqdqiqsqiiiir"
};
static const unsigned source_schema[] = {0, 1, 1, 2, 1, 3, 4, 5};
static const double scales[] = {1., 10., 100., 1000., 10000., 100000., 1000000., 10000000., 100000000.};

API int rt_numeric_abi(void) { return 1; }
API const char *rt_numeric_schema(unsigned id) { return id >= 1 && id <= 5 ? schemas[id] : ""; }

API int rt_numeric_supported(void) {
    if (CHAR_BIT != 8 || sizeof(double) != 8 || FLT_RADIX != 2 || DBL_MANT_DIG != 53 ||
        DBL_MAX_EXP != 1024 || FLT_EVAL_METHOD != 0 || fegetround() != FE_TONEAREST) return 0;
    /* Volatile operands test the current thread's arithmetic, not a folded literal. */
    volatile double one = 1., ten = 10.;
    double value = one / ten;
    uint64_t bits;
    memcpy(&bits, &value, sizeof bits);
    return bits == UINT64_C(0x3fb999999999999a);
}

static uint64_t little(const uint8_t *p, unsigned width) {
    uint64_t value = 0;
    for (unsigned i = 0; i < width; ++i) value |= (uint64_t)p[i] << (8 * i);
    return value;
}
static uint32_t big32(const uint8_t *p) {
    return (uint32_t)p[0] << 24 | (uint32_t)p[1] << 16 | (uint32_t)p[2] << 8 | p[3];
}
static void put(uint8_t *p, uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i) p[i] = (uint8_t)(value >> (8 * i));
}
static int varint(const uint8_t *data, size_t size, size_t *at, uint64_t *value) {
    *value = 0;
    for (unsigned n = 0; n < 10; ++n) {
        if (*at >= size) return 0;
        uint8_t byte = data[(*at)++];
        if (n == 9 && byte > 1) return 0;
        *value |= (uint64_t)(byte & 127) << (7 * n);
        if (byte < 128) return !n || byte != 0;
    }
    return 0;
}
static int64_t signed_value(uint64_t value) {
    return value & 1 ? -1 - (int64_t)(value >> 1) : (int64_t)(value >> 1);
}
static int utf8(const uint8_t *p, size_t size) {
    size_t at = 0;
    while (at < size) {
        uint8_t c = p[at++];
        unsigned count;
        uint32_t code, minimum;
        if (c < 128) continue;
        if (c >= 0xc2 && c <= 0xdf) { count = 1; code = c & 31; minimum = 0x80; }
        else if (c >= 0xe0 && c <= 0xef) { count = 2; code = c & 15; minimum = 0x800; }
        else if (c >= 0xf0 && c <= 0xf4) { count = 3; code = c & 7; minimum = 0x10000; }
        else return 0;
        if (count > size - at) return 0;
        while (count--) {
            c = p[at++];
            if ((c & 0xc0) != 0x80) return 0;
            code = (code << 6) | (c & 63);
        }
        if (code < minimum || code > 0x10ffff || (code >= 0xd800 && code <= 0xdfff)) return 0;
    }
    return 1;
}

/* Caller supplies valid non-overlapping buffers. Nothing is published on failure. */
API int rt_numeric_restore(const uint8_t *data, size_t size, size_t raw_size, size_t count,
                           uint8_t *output, size_t capacity, size_t *written) {
    if (!written) return 1;
    *written = 0;
    if (!data || !output || !size || size > MAX_TRANSFORMED || !raw_size || raw_size > MAX_RAW ||
        !count || count > MAX_FRAMES || capacity < raw_size || capacity > MAX_RAW ||
        !rt_numeric_supported()) return 1;
    uint8_t raw[MAX_RAW];
    size_t at = 0, used = 0;
    for (size_t frame = 0; frame < count; ++frame) {
        if (size - at < 42 || raw_size - used < 42) return 1;
        const uint8_t *header = data + at, *envelope = header + 10;
        size_t length = big32(header);
        unsigned schema = (unsigned)little(envelope + 2, 2);
        unsigned source = (unsigned)little(envelope + 4, 2);
        if (!big32(header + 4) || header[8] || header[9] != 0x63 || little(envelope, 2) != 1 ||
            schema < 1 || schema > 5 || source < 1 || source > 7 || source_schema[source] != schema ||
            !little(envelope + 8, 8) || !little(envelope + 16, 8) || little(envelope + 26, 2) ||
            length < 32 || length > raw_size - used - 10 || (source < 6 && length > 16384)) return 1;
        const char *plan = schemas[schema];
        size_t fields = strlen(plan), bitmap_size = (fields + 7) / 8;
        if (little(envelope + 6, 2) != fields || little(envelope + 24, 2) != bitmap_size ||
            32 + bitmap_size + little(envelope + 28, 4) != length || size - at - 42 < bitmap_size) return 1;
        const uint8_t *bitmap = data + at + 42;
        if ((bitmap[0] & 3) || (fields % 8 && (bitmap[bitmap_size - 1] >> (fields % 8)))) return 1;
        size_t end = used + 10 + length;
        memcpy(raw + used, data + at, 42 + bitmap_size);
        used += 42 + bitmap_size;
        at += 42 + bitmap_size;
        for (size_t i = 0; i < fields; ++i) {
            if (bitmap[i / 8] & (1u << (i % 8))) continue;
            char kind = plan[i];
            unsigned width = kind == 'i' ? 4 : kind == 'c' ? 1 : 8;
            uint64_t value;
            if (kind == 's' || kind == 'r') {
                width = kind == 'r' ? 4 : 2;
                if (size - at < width || end - used < width) return 1;
                size_t bytes = (size_t)little(data + at, width);
                if (bytes > (kind == 'r' ? 524288u : 1024u) || bytes > size - at - width ||
                    bytes > end - used - width || !utf8(data + at + width, bytes) ||
                    (!i && (!bytes || memchr(data + at + width, 0, bytes)))) return 1;
                memcpy(raw + used, data + at, width + bytes);
                used += width + bytes;
                at += width + bytes;
                continue;
            }
            if (end - used < width) return 1;
            if (kind == 'd') {
                if (at == size) return 1;
                unsigned tag = data[at++];
                if (tag > 9) return 1;
                if (!tag) {
                    if (size - at < 8) return 1;
                    value = little(data + at, 8);
                    at += 8;
                } else {
                    if (!varint(data, size, &at, &value) || value >= (UINT64_C(1) << 49)) return 1;
                    double number = (double)signed_value(value) / scales[tag - 1];
                    memcpy(&value, &number, 8);
                }
            } else if (kind == 'c') {
                if (at == size) return 1;
                value = data[at++];
            } else {
                if (!varint(data, size, &at, &value)) return 1;
                int64_t number = signed_value(value);
                if (kind == 'i' && (number < INT32_MIN || number > INT32_MAX)) return 1;
                value = (uint64_t)number;
            }
            put(raw + used, value, width);
            used += width;
        }
        if (used != end) return 1;
    }
    if (at != size || used != raw_size) return 1;
    memcpy(output, raw, raw_size);
    *written = raw_size;
    return 0;
}
