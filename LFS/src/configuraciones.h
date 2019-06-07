/*
 * configuraciones.h
 *
 *  Created on: 19 may. 2019
 *      Author: utnso
 */

#ifndef SRC_CONFIGURACIONES_H_
#define SRC_CONFIGURACIONES_H_

#include <commons/config.h>
#include <commons/string.h>
#include <commons/bitarray.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/io.h>
#include <fcntl.h>

pthread_mutex_t mutexMemtable;
pthread_mutex_t mutexLogger;
t_log* logger;

int tamanioBloques;
int cantDeBloques;
char* magicNumber;
t_config* archivoMetadata;
//hasta aca todo se saca del metadata

char* ipLisandra;
char* puertoLisandra;
char* puntoMontaje;
int tiempoDump;
//int tiempoDump y int Retardo por ahora no, pueden ir cambiando
int tamanioValue;
t_config* archivoDeConfig;
//hasta aca del archivo de config
t_list* memtable;
t_bitarray* bitarray;
t_list* listaDeTablasConTemporales;

void leerConfig(char* ruta){
	archivoDeConfig = config_create(ruta);
	ipLisandra = config_get_string_value(archivoDeConfig,"IP_LISANDRA");
	puertoLisandra = config_get_string_value(archivoDeConfig,"PUERTO_ESCUCHA");
	puntoMontaje = config_get_string_value(archivoDeConfig,"PUNTO_MONTAJE");
	tamanioValue = config_get_int_value(archivoDeConfig,"TAMAÑO_VALUE");
	tiempoDump = config_get_int_value(archivoDeConfig,"TIEMPO_DUMP");

}


void inicializarArchivoBitmap(){
	FILE *f;
	int i;

	f = fopen("/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/LFS/Metadata/Bitmap.bin", "wb");

	for(i=0; i < 64; i++){
		fputc(0,f);
	}

	fclose(f);
}

void inicializarBitmap() {

	struct stat s;
	int tamanio;
	unsigned char* bitmap;

	char* direccionBitmap = string_new();
	string_append(&direccionBitmap, puntoMontaje);
	string_append(&direccionBitmap, "Metadata/Bitmap.bin");

	int f = open(direccionBitmap, O_RDWR);

	int infoArchivo = fstat(f, &s);
	tamanio = s.st_size;

	bitmap =  mmap(0, tamanio, PROT_READ | PROT_WRITE, MAP_SHARED, f, 0);

	bitarray = bitarray_create_with_mode(bitmap,cantDeBloques/8, LSB_FIRST);

	free(direccionBitmap);
}

void leerMetadataFS (){
	char* rutaMetadata = string_new();
	string_append(&rutaMetadata,puntoMontaje);
	string_append(&rutaMetadata,"Metadata/Metadata.bin");
	archivoMetadata= config_create(rutaMetadata);
	tamanioBloques = config_get_int_value(archivoMetadata,"BLOCK_SIZE");
	cantDeBloques = config_get_int_value(archivoMetadata,"BLOCKS");
	magicNumber = config_get_string_value(archivoMetadata,"MAGIC_NUMBER");
}
void inicializarMemtable(){
	memtable = list_create();
	listaDeTablasConTemporales =list_create();

}

void inicializarLog(char* ruta){
	logger = log_create(ruta, "LISANDRA", 1, LOG_LEVEL_INFO);
}

void liberarConfigYLogs() {
	log_destroy(logger);
	config_destroy(archivoDeConfig);
}

#endif /* SRC_CONFIGURACIONES_H_ */
