import os
import sys

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext
from setuptools.command.bdist_wheel import bdist_wheel


class NativeBuild(build_ext):
    def get_export_symbols(self, ext):
        # ctypes library, not a CPython module with a PyInit_* entry point.
        return ['rt_numeric_abi', 'rt_numeric_supported', 'rt_numeric_schema', 'rt_numeric_restore']

    def get_ext_filename(self, name):
        return os.path.join(*name.split('.')) + ('.dll' if sys.platform == 'win32' else '.so')


class NativeWheel(bdist_wheel):
    def get_tag(self):
        _, _, platform = super().get_tag()
        # This library uses ctypes, not the CPython extension ABI.
        return 'py3', 'none', platform


setup(
    ext_modules=[Extension('rtdata_numeric_codec._codec', ['src/numeric_codec.c'],
        libraries=[] if sys.platform == 'win32' else ['m'],
        extra_compile_args=['/std:c11', '/fp:strict'] if sys.platform == 'win32' else
            ['-std=c11', '-O2', '-fno-fast-math', '-ffp-contract=off', '-frounding-math'])],
    cmdclass={'build_ext': NativeBuild, 'bdist_wheel': NativeWheel},
)
