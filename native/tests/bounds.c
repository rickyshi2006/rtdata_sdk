#include <assert.h>
#include <fenv.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int rt_numeric_supported(void);
int rt_numeric_restore(const uint8_t *, size_t, size_t, size_t, uint8_t *, size_t, size_t *);

int main(void) {
    /* Schema 3: code='TEST', timestamp=1, pre_close=0.1, all other fields null. */
    uint8_t data[53] = {0};
    data[3] = 56; data[7] = 1; data[9] = 0x63;
    data[10] = 1; data[12] = 3; data[14] = 5; data[16] = 15;
    data[18] = 1; data[26] = 1; data[34] = 2; data[38] = 22;
    data[42] = 0xf8; data[43] = 0x7f;
    data[44] = 4; memcpy(data + 46, "TEST", 4);
    data[50] = 2; data[51] = 2; data[52] = 2;
    uint8_t output[65568];
    size_t written = 0;
    assert(rt_numeric_supported());
    memset(output, 0xa5, sizeof output);
    assert(!rt_numeric_restore(data, sizeof data, 66, 1, output + 16, 65536, &written));
    assert(written == 66);
    for (size_t i = 0; i < 16; ++i) assert(output[i] == 0xa5);
    for (size_t i = 82; i < sizeof output; ++i) assert(output[i] == 0xa5);
    for (size_t cut = 0; cut < sizeof data; ++cut) {
        uint8_t *short_input = malloc(cut ? cut : 1);
        assert(short_input);
        memcpy(short_input, data, cut);
        memset(output, 0xa5, sizeof output); written = 99;
        assert(rt_numeric_restore(short_input, cut, 66, 1, output + 16, 65536, &written));
        assert(!written);
        for (size_t i = 0; i < sizeof output; ++i) assert(output[i] == 0xa5);
        free(short_input);
    }
    for (size_t at = 0; at < sizeof data; ++at) {
        for (unsigned bit = 0; bit < 8; ++bit) {
            uint8_t bad[sizeof data]; memcpy(bad, data, sizeof bad); bad[at] ^= 1u << bit;
            memset(output, 0xa5, sizeof output);
            int error = rt_numeric_restore(bad, sizeof bad, 66, 1, output + 16, 65536, &written);
            assert(error ? written == 0 : written == 66);
            for (size_t i = 0; i < 16; ++i) assert(output[i] == 0xa5);
            for (size_t i = 16 + written; i < sizeof output; ++i) assert(output[i] == 0xa5);
        }
    }
    int modes[] = {FE_DOWNWARD, FE_UPWARD, FE_TOWARDZERO};
    for (unsigned i = 0; i < sizeof modes / sizeof modes[0]; ++i) {
        assert(!fesetround(modes[i]));
        assert(!rt_numeric_supported());
        memset(output, 0xa5, sizeof output); written = 99;
        assert(rt_numeric_restore(data, sizeof data, 66, 1, output, 65536, &written));
        assert(!written && fegetround() == modes[i]);
        for (size_t j = 0; j < sizeof output; ++j) assert(output[j] == 0xa5);
        assert(!fesetround(FE_TONEAREST));
    }
    puts("portable native short-input, canary, mutation and rounding checks passed");
    return 0;
}
