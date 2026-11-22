"""Optional, platform-specific numeric byte restoration; no transport negotiation."""
import ctypes
import sys
from pathlib import Path

ABI_VERSION = 1
MAX_RAW = 65536
MAX_TRANSFORMED = 131072
MAX_FRAMES = 256


class Decoder:
    def __init__(self):
        name = '_codec.dll' if sys.platform == 'win32' else '_codec.so'
        self._library = ctypes.CDLL(str(Path(__file__).resolve().parent / name))
        self._library.rt_numeric_abi.argtypes = []
        self._library.rt_numeric_abi.restype = ctypes.c_int
        if self._library.rt_numeric_abi() != ABI_VERSION:
            raise RuntimeError('numeric codec ABI mismatch')
        self._library.rt_numeric_supported.argtypes = []
        self._library.rt_numeric_supported.restype = ctypes.c_int
        self._library.rt_numeric_schema.argtypes = [ctypes.c_uint]
        self._library.rt_numeric_schema.restype = ctypes.c_char_p
        self._call = self._library.rt_numeric_restore
        self._call.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.c_size_t,
                              ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t,
                              ctypes.POINTER(ctypes.c_size_t)]
        self._call.restype = ctypes.c_int
        if not self.supported():
            raise RuntimeError('numeric codec arithmetic environment')

    def supported(self):
        return self._library.rt_numeric_supported() == 1

    def schema(self, schema_id):
        if type(schema_id) is not int or not 1 <= schema_id <= 5:
            raise ValueError('numeric schema id')
        return self._library.rt_numeric_schema(schema_id).decode('ascii')

    def restore(self, data, raw_size, count):
        if (type(data) is not bytes or not 0 < len(data) <= MAX_TRANSFORMED or
                type(raw_size) is not int or not 0 < raw_size <= MAX_RAW or
                type(count) is not int or not 0 < count <= MAX_FRAMES):
            raise ValueError('numeric restore bounds')
        output = ctypes.create_string_buffer(raw_size)
        written = ctypes.c_size_t()
        if (self._call(data, len(data), raw_size, count, output, raw_size,
                       ctypes.byref(written)) or written.value != raw_size):
            raise ValueError('numeric restore failed')
        return output.raw
