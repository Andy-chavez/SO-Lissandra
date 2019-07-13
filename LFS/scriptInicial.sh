#!/bin/bash
cd ..
cd commonsPropias/
sudo make uninstall
sudo make install
cd ..
cd LFS/
mkdir Metadata
cd Metadata/
read -p "Ingresa BLOCK_SIZE del Metadata.bin " x
echo "BLOCK_SIZE=$x" >> Metadata.bin
read -p "Ingrese BLOCKS del archivo " y
echo "BLOCKS=$y" >>Metadata.bin
echo "MAGIC_NUMBER=LISSANDRA" >>Metadata.bin
cd ..
vi lisandra.config
mkdir Tables
mkdir Bloques
cd src/
make clean
make all
./elLFS.o
