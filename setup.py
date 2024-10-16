import sys
import platform
from setuptools import setup
from Cython.Build import cythonize
from setuptools.extension import Extension

compiler_flags = []
linker_flags = []

if platform.system() == "Windows":
    if "msvc" in sys.version.lower():
        compiler_flags = ["/O2"]
        linker_flags = ["/O2"]
    elif "gcc" in sys.version.lower() or "clang" in sys.version.lower():
        compiler_flags = ["-O3"]
        linker_flags = ["-O3"]
else:
    compiler_flags = ["-O3"]
    linker_flags = ["-O3"]

extensions = [
    Extension(
        name="kite_ticker",
        sources=["kite_ticker.pyx"],
        language="c++",
        extra_compile_args=compiler_flags,
        extra_link_args=linker_flags
    )
]

setup(
    name="kite_ticker",
    ext_modules=cythonize(extensions, language_level=3),
)
