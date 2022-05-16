#!/bin/zsh
#TODO: Simple script to test Python bindings, upgrade this to multi-platform compatible build.
rm -rf  build/ dist/ wheelhouse/ PyGsrSerDe.egg-info/
ln -sf $(PWD)/../target $(PWD)/../python/deps
ln -sf $(PWD)/../c $(PWD)/../python/c
python3.9 -m pip  wheel -w dist --verbose .

LD_LIBRARY_PATH=LD_LIBRARY_PATH:$(PWD)/ && auditwheel repair dist/*.whl --plat 'manylinux2014_x86_64'

python3.9 -m pip install wheelhouse/*.whl --force-reinstall
cd ../
python3.9
