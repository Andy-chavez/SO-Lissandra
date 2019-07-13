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
#include "utils.h"
#include "variablesGlobales.h"

// ------------------------------------------------------------------------ //
// 1) INICIALIZACIONES//

void inicializarSemaforos(){
		pthread_mutexattr_t atributoMemtable;
		pthread_mutexattr_init(&atributoMemtable);
		pthread_mutexattr_t atributoLogger;
		pthread_mutexattr_init(&atributoLogger);
		pthread_mutexattr_t atributoLoggerConsola;
		pthread_mutexattr_init(&atributoLoggerConsola);
		pthread_mutexattr_t atributoListaDeTablas;
		pthread_mutexattr_init(&atributoListaDeTablas);
		pthread_mutexattr_t atributoBitarray;
		pthread_mutexattr_init(&atributoBitarray);
		pthread_mutexattr_t atributoTiempoDump;
		pthread_mutexattr_init(&atributoTiempoDump);
		pthread_mutexattr_t atributoRetardo;
		pthread_mutexattr_init(&atributoRetardo);
		pthread_mutexattr_t atributoResultadosConsola;
		pthread_mutexattr_init(&atributoResultadosConsola);
		pthread_mutexattr_t atributoResultados;
		pthread_mutexattr_init(&atributoResultados);

		pthread_mutex_init(&mutexMemtable, &atributoMemtable);
		pthread_mutex_init(&mutexLogger, &atributoLogger);
		pthread_mutex_init(&mutexLoggerConsola, &atributoLoggerConsola);
		pthread_mutex_init(&mutexListaDeTablas,&atributoListaDeTablas);
		pthread_mutex_init(&mutexBitarray,&atributoBitarray);
		pthread_mutex_init(&mutexTiempoDump,&atributoTiempoDump);
		pthread_mutex_init(&mutexRetardo,&atributoRetardo);
		pthread_mutex_init(&mutexResultadosConsola,&atributoResultadosConsola);
		pthread_mutex_init(&mutexResultados,&atributoResultados);
		sem_init(&binarioSocket,0,1);

		pthread_mutexattr_settype(&atributoMemtable,PTHREAD_MUTEX_ERRORCHECK);
		pthread_mutexattr_settype(&atributoLogger,PTHREAD_MUTEX_ERRORCHECK);
		pthread_mutexattr_settype(&atributoLoggerConsola,PTHREAD_MUTEX_ERRORCHECK);
		pthread_mutexattr_settype(&atributoListaDeTablas,PTHREAD_MUTEX_ERRORCHECK);
		pthread_mutexattr_settype(&atributoBitarray,PTHREAD_MUTEX_ERRORCHECK);
		pthread_mutexattr_settype(&atributoTiempoDump,PTHREAD_MUTEX_ERRORCHECK);
		pthread_mutexattr_settype(&atributoRetardo,PTHREAD_MUTEX_ERRORCHECK);
		pthread_mutexattr_settype(&atributoResultadosConsola,PTHREAD_MUTEX_ERRORCHECK);
		pthread_mutexattr_settype(&atributoResultados,PTHREAD_MUTEX_ERRORCHECK);

}

void inicializarBloques(){
	for(int i=0;i<cantDeBloques;i++){
		char* ruta= string_new();
		char* numeroDeBloque =string_itoa(i);
		string_append(&ruta,puntoMontaje);
		string_append(&ruta,"Bloques/");
		string_append(&ruta,numeroDeBloque);
		string_append(&ruta,".bin");
		FILE *bloque = fopen(ruta,"a"); //cambiarlo por una 'a' cuando sea entrega
		free(numeroDeBloque);
		free(ruta);
		fclose(bloque);

	}
}

void inicializarArchivoBitmap(){
	FILE *f;
	int i;

	char* ruta = string_new();
	string_append(&ruta,puntoMontaje);
	string_append(&ruta,"Metadata/Bitmap.bin");

	if(existeArchivo(ruta)){
		free(ruta);
		return;
	}
	f = fopen(ruta, "wb");

	for(i=0; i < cantDeBloques/8; i++){
		fputc(0,f);
	}
	free(ruta);
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

void inicializarListas(){
	memtable = list_create();
	listaDeTablas = list_create();
}

void inicializarLog(){
	logger = log_create("lisandra.log", "LISANDRA", 0, LOG_LEVEL_INFO);
	loggerConsola = log_create("lisandraConsola.log","LISANDRA_CONSOLA",0,LOG_LEVEL_INFO);
	loggerResultados = log_create("lisandraResultados.log","LISANDRA_RESULTADO",0,LOG_LEVEL_INFO);
	loggerResultadosConsola = log_create("lisandraResultadosConsola.log","LISANDRA_CONSOLA_RESULTADO",0,LOG_LEVEL_INFO);
}

// ------------------------------------------------------------------------ //
// 2) LIBERACIONES//

void liberarSemaforos(){
	pthread_mutex_destroy(&mutexMemtable);
	pthread_mutex_destroy(&mutexLogger);
	pthread_mutex_destroy(&mutexLoggerConsola);
	pthread_mutex_destroy(&mutexListaDeTablas);

	pthread_mutex_destroy(&mutexBitarray);
	pthread_mutex_destroy(&mutexTiempoDump);
	pthread_mutex_destroy(&mutexRetardo);
	pthread_mutex_destroy(&mutexResultadosConsola);
	pthread_mutex_destroy(&mutexResultados);
}

void liberarConfigYLogs() {
	log_destroy(logger);
	log_destroy(loggerConsola);
	log_destroy(loggerResultados);
	log_destroy(loggerResultadosConsola);
	config_destroy(archivoDeConfig);
	config_destroy(archivoMetadata);
}
void liberarVariablesGlobales(){
	liberarSemaforos();
	liberarMemtable();
	liberarListaDeTablas();
	liberarConfigYLogs();
}

// ------------------------------------------------------------------------ //
// 3) LECTURAS//


void leerConfig(char* ruta){
	archivoDeConfig = config_create(ruta);
	ipLisandra = config_get_string_value(archivoDeConfig,"IP_LISANDRA");
	puertoLisandra = config_get_string_value(archivoDeConfig,"PUERTO_ESCUCHA");
	puntoMontaje = config_get_string_value(archivoDeConfig,"PUNTO_MONTAJE");
	tamanioValue = config_get_int_value(archivoDeConfig,"TAMAÑO_VALUE");
	tiempoDump = config_get_int_value(archivoDeConfig,"TIEMPO_DUMP");
	retardo = config_get_int_value(archivoDeConfig,"RETARDO");
}

void leerMetadataFS (){
	char* rutaMetadata = string_new();
	string_append(&rutaMetadata,puntoMontaje);
	string_append(&rutaMetadata,"Metadata/Metadata.bin");
	archivoMetadata= config_create(rutaMetadata);
	tamanioBloques = config_get_int_value(archivoMetadata,"BLOCK_SIZE");
	cantDeBloques = config_get_int_value(archivoMetadata,"BLOCKS");
	magicNumber = config_get_string_value(archivoMetadata,"MAGIC_NUMBER");
	free(rutaMetadata);
}




#endif /* SRC_CONFIGURACIONES_H_ */
