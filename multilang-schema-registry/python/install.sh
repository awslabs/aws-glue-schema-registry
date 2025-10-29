#!/bin/zsh
#TODO: Simple script to test Python bindings. Works only on Linux.
#upgrade this to multi-platform compatible build script.
#Run: bash install.sh 3.7
python_version=$1
python_cmd="python$python_version"
rm -rf  build/ dist/ wheelhouse/ PyGsrSerDe.egg-info
$python_cmd -m pip  wheel -w dist --verbose .

export LD_LIBRARY_PATH=LD_LIBRARY_PATH:$PWD/ && auditwheel repair dist/PyGsrSerDe-0.1-cp37-cp37m-linux_x86_64.whl --plat 'manylinux2014_x86_64'

$python_cmd -m pip install wheelhouse/PyGsrSerDe-0.1-cp37-cp37m-manylinux_2_17_x86_64.manylinux2014_x86_64.whl --force-reinstall

cd ../ && $python_cmd -m unittest PyGsrSerDe.test_PyGsrSerDe