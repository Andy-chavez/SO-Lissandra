/*
 * utils.h
 *
 *  Created on: 22 jun. 2019
 *      Author: utnso
 */

#ifndef SRC_UTILS_H_
#define SRC_UTILS_H_

#include <commons/string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdio.h>
#include "variablesGlobales.h"
#include <math.h>
#include <commonsPropias/conexiones.h>
#include <commonsPropias/serializacion.h>
#include <commons/collections/list.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/types.h>

//funciones en comun entre funcionesLFS y compactador
int existeArchivo(char * filename);
void printearBitmap();
char* infoEnBloque(char* numeroBloque);
char* devolverBloqueLibre();
void guardarInfoEnArchivo(char* ruta, const char* info);
void liberarDoblePuntero(char** doblePuntero);
void* devolverMayor(registro* registro1, registro* registro2);
int calcularParticion(int key,int cantidadParticiones);
void liberarRegistros(registro* unRegistro);

void liberarRegistros(registro* unRegistro) {
		free(unRegistro->value);
		free(unRegistro);
}
int calcularParticion(int key,int cantidadParticiones){
	int particion= key%cantidadParticiones;
	return particion;
}

void* devolverMayor(registro* registro1, registro* registro2){
	if (registro1->timestamp > registro2->timestamp){
			return registro1;
		}else{
			return registro2;
		}
}

void liberarDoblePuntero(char** doblePuntero){
	string_iterate_lines(doblePuntero, free);
	free(doblePuntero);

}
void guardarInfoEnArchivo(char* ruta, const char* info){
	FILE *fp = fopen(ruta, "w");
	//int largo =strlen(info);
	if (fp != NULL){
		//fwrite(info , 1 , largo , fp );
		fputs(info, fp);
		fclose(fp);
	}
}

char* devolverBloqueLibre(){
	int i;

	int bloqueEncontrado = 0;
	int encontroBloque = 0;
	char* numero;

	for(i=0; i < cantDeBloques; i++){
		pthread_mutex_lock(&mutexBitarray);
		bool bit = bitarray_test_bit(bitarray, i);
		pthread_mutex_unlock(&mutexBitarray);
		if(bit == 0){
			encontroBloque = 1;
			bloqueEncontrado = i;
			break;
		}

	}

	if (encontroBloque == 1){
		pthread_mutex_lock(&mutexBitarray);
		bitarray_set_bit(bitarray, bloqueEncontrado);
		pthread_mutex_unlock(&mutexBitarray);
		numero = string_itoa(bloqueEncontrado);
	}
	return numero;
}

char* infoEnBloque(char* numeroBloque){ //pasarle el tamanio de la particion, o ver que onda (rutaTabla)
	//ver que agarre toda la info de los bloques correspondientes a esa tabla
	struct stat sb;

	char* rutaBloque = string_new();
	string_append(&rutaBloque,puntoMontaje);
	string_append(&rutaBloque,"Bloques/");
	string_append(&rutaBloque,numeroBloque);
	string_append(&rutaBloque,".bin");
	int archivo = open(rutaBloque,O_RDWR);

	fstat(archivo,&sb);
	if (sb.st_size == 0){
		free(rutaBloque);
		return NULL;
	}

	char* informacion = mmap(NULL,tamanioBloques,PROT_READ,MAP_PRIVATE,archivo,NULL);
	free(rutaBloque);
	return informacion;
}

void printearBitmap(){

	int j;
	for(j=0; j<30; j++){
		bool bit = bitarray_test_bit(bitarray, j);
		printf("%i \n", bit);
	}

}

int existeArchivo(char * filename){
    FILE *file;
    if (file = fopen(filename, "r")){
        fclose(file);
        return 1;
    }
    return 0;
}


#endif /* SRC_UTILS_H_ */
