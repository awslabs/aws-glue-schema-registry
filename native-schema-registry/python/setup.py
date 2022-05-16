from setuptools import setup, Extension
from glob import glob

source_files = glob("c/src/*.c") + glob("c/src/swig/glue_schema_registry_serde.i")
include_dirs = ["c/include/", "deps/target/"]
libraries = ["deps/target/nativeschemaregistry"]
extension_module = Extension(
    'PyGsrSerDe._GsrSerDe',
    sources=source_files,
    include_dirs=include_dirs,
    libraries=libraries,
    language='c'
)

setup(
    name='PyGsrSerDe',
    version='0.1',
    py_modules=['PyGsrSerDe'],
    ext_modules=[extension_module],
    packages=['PyGsrSerDe']
)
