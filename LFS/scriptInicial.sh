#!/bin/bash
mkdir Metadata
cd Metadata/
read -p "Ingresa BLOCK_SIZE del Metadata.bin " x
echo "BLOCK_SIZE=$x" >> Metadata.bin
read -p "Ingrese BLOCKS del archivo " y
echo "BLOCKS=$y" >>Metadata.bin
echo "MAGIC_NUMBER=LISSANDRA" >>Metadata.bin
cd ..
mkdir Tables
mkdir Bloques
