from setuptools import setup
from setuptools.extension import Extension
from Cython.Build import cythonize

extensions = [
    Extension(
        name="kite_ticker",
        sources=["kite_ticker.pyx"],
        language="c++",
        extra_compile_args=["-O3"], 
        extra_link_args=["-O3"]
    )
]

setup(
    name="kite_ticker",
    ext_modules=cythonize(extensions, language_level=3),
)
