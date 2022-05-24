#!/bin/zsh
python_version=$1
python_cmd="python$python_version"
#TODO: Simple script to test Python bindings, upgrade this to multi-platform compatible build.
rm -rf  build/ dist/ wheelhouse/ PyGsrSerDe.egg-info/
ln -sf $PWD/../target $PWD/../python/deps
ln -sf $PWD/../c $PWD/../python/c
$python_cmd -m pip  wheel -w dist --verbose .
cp c/src/swig/GsrSerDe.py ./PyGsrSerDe/
sed -ie "s/c.src.swig/./" PyGsrSerDe/GsrSerDe.py
$python_cmd -m pip  wheel -w dist --verbose .

export LD_LIBRARY_PATH=LD_LIBRARY_PATH:$PWD/ && auditwheel repair dist/*.whl --plat 'manylinux2014_x86_64'

$python_cmd -m pip install wheelhouse/*.whl --force-reinstall
