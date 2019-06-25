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

typedef struct {
	char* nombre;
	t_list* listaRegistros;
} tablaMem;

typedef struct{
	consistencia tipoConsistencia;
	int cantParticiones;
	int tiempoCompactacion;
	char* nombreTabla;
	pthread_mutex_t semaforoTabla;
	pthread_t hiloDeCompactacion;
}metadataConSemaforo;

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
void marcarBloquesComoLibre(char** arrayDeBloques);
void liberarMemtable();
void liberarListaDeTablas();
void separarRegistrosYCargarALista(char* buffer, t_list* listaRegistros);
void liberarMetadataConSemaforo(metadataConSemaforo* unMetadata);
int obtenerCantTemporales(char* nombreTabla);
void enviarYLogearMensajeError(int socket, char* mensaje);
void enviarOMostrarYLogearInfo(int socket, char* mensaje);
void enviarYOLogearAlgo(int socket, char *mensaje, void(*log)(t_log *, char *));
void liberarBloquesDeTmpYPart(char* nombreArchivo,char* rutaTabla);
void agregarALista(char* timestamp,char* key,char* value,t_list* head);
void soloLoggear(int socket,char* mensaje);
pthread_mutex_t devolverSemaforoDeTabla(char* nombreTabla);

void soloLoggear(int socket,char* mensaje){
	if(socket==-1){
		pthread_mutex_lock(&mutexLoggerConsola);
		log_info(loggerConsola, mensaje);
		pthread_mutex_unlock(&mutexLoggerConsola);
	}
	else{
		pthread_mutex_lock(&mutexLogger);
		log_info(logger, mensaje);
		pthread_mutex_unlock(&mutexLogger);
	}
}
pthread_mutex_t devolverSemaforoDeTabla(char* nombreTabla){
		bool seEncuentraTabla(void* elemento){
			metadata* unMetadata = elemento;
			return string_equals_ignore_case(unMetadata->nombreTabla,nombreTabla);
		}
	pthread_mutex_lock(&mutexListaDeTablas);
	metadataConSemaforo* metadataBuscado = list_find(listaDeTablas,seEncuentraTabla);
	pthread_mutex_unlock(&mutexListaDeTablas);
	return metadataBuscado->semaforoTabla;
}

void agregarALista(char* unTimestamp,char* unaKey,char* unValue,t_list* head){
	registro* guardarRegistro= malloc (sizeof(registro));
	guardarRegistro->timestamp = atoi(unTimestamp);
	guardarRegistro->key = atoi(unaKey);
	guardarRegistro->value = string_duplicate(unValue);
	list_add(head,guardarRegistro);
}


void separarRegistrosYCargarALista(char* buffer, t_list* listaRegistros){
	char** separarRegistro = string_split(buffer,"\n");
			int j =0;
			for(j=0;*(separarRegistro+j)!=NULL;j++){
				char **aCargar =string_split(*(separarRegistro+j),";");
				agregarALista(*(aCargar+0),*(aCargar+1),*(aCargar+2),listaRegistros);
				liberarDoblePuntero(aCargar);
			}

	liberarDoblePuntero(separarRegistro);
}


void liberarBloquesDeTmpYPart(char* nombreArchivo,char* rutaTabla){
	char* rutaCompleta = string_new();
	string_append(&rutaCompleta,rutaTabla);
	string_append(&rutaCompleta,"/");
	string_append(&rutaCompleta,nombreArchivo);
	if(string_equals_ignore_case(nombreArchivo, "Metadata")){
		remove(rutaCompleta);
		return;
	}

	t_config* archivo= config_create(rutaCompleta);
	char **bloques = config_get_array_value(archivo,"BLOCKS");

	marcarBloquesComoLibre(bloques);
	liberarDoblePuntero(bloques);
	config_destroy(archivo);
	remove(rutaCompleta);
	free(rutaCompleta);

}


void enviarOMostrarYLogearInfo(int socket, char* mensaje) {
	enviarYOLogearAlgo(socket, mensaje, (void*) log_info);
}

void enviarYOLogearAlgo(int socket, char *mensaje, void(*log)(t_log *, char *)){
	if(socket != -1) {
		pthread_mutex_lock(&mutexLogger);
		log(logger, mensaje);
		pthread_mutex_unlock(&mutexLogger);
		enviar(socket, mensaje, strlen(mensaje) + 1);
	} else {
		pthread_mutex_lock(&mutexLoggerConsola);
		log(loggerConsola, mensaje);
		pthread_mutex_unlock(&mutexLoggerConsola);
	}
}

void enviarYLogearMensajeError(int socket, char* mensaje) {
	enviarYOLogearAlgo(socket, mensaje, (void*) log_error);
}



void liberarTablaMem(tablaMem* tabla) {
	free(tabla->nombre);
	list_destroy_and_destroy_elements(tabla->listaRegistros,(void*) liberarRegistros);
	free(tabla);
}

void liberarMemtable() { //no elimina toda la memtable sino las tablas y registros de ella
	list_clean_and_destroy_elements(memtable,(void*) liberarTablaMem);
}

void liberarMetadataConSemaforo(metadataConSemaforo* unMetadata){
	free(unMetadata->nombreTabla);
	pthread_mutex_destroy(&(unMetadata->semaforoTabla));

	pthread_cancel(unMetadata->hiloDeCompactacion);
	pthread_join(unMetadata->hiloDeCompactacion,NULL);
	free(unMetadata);
}

void liberarListaDeTablas(){
	list_clean_and_destroy_elements(listaDeTablas,(void*) liberarMetadataConSemaforo);
}
void liberarRegistros(registro* unRegistro) {
		free(unRegistro->value);
		free(unRegistro);
}
int calcularParticion(int key,int cantidadParticiones){
	int particion= key%cantidadParticiones;
	return particion;
}

int obtenerCantTemporales(char* nombreTabla){ //SIRVE PARA DUMP(TE DEVUELVE EL NUMERO A ESCRIBIR)
											//REUTILIZAR EN COMPACTACION
	//puntoMontaje/Tables/TABLA1/1.tmp, suponemos que los temporales se hacen en orden
	int cantTemporal = 0;
	int existe;
	do{

		char* ruta = string_new();
		char* numeroTmp =string_itoa(cantTemporal);
		string_append(&ruta,puntoMontaje);
		string_append(&ruta,"Tables/");
		string_append(&ruta,nombreTabla);
		string_append(&ruta,"/");
		string_append(&ruta,numeroTmp);
		string_append(&ruta,".tmp");
		existe = existeArchivo(ruta);
		free(numeroTmp);
		free(ruta);
		if(existe==0) break;
		cantTemporal++;
	}while(existe!=0);
	return cantTemporal;
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
		return;
	}
	fclose(fp);
}

void marcarBloquesComoLibre(char** arrayDeBloques){
	int pos =0;
	while(*(arrayDeBloques+pos)!=NULL){
			int posicionActual = atoi(*(arrayDeBloques+pos));

			pthread_mutex_lock(&mutexBitarray);
			bitarray_clean_bit(bitarray, posicionActual);
			pthread_mutex_unlock(&mutexBitarray);
			pos++;
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
