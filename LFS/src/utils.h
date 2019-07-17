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
#include <errno.h>

typedef struct {
	char* nombre;
	t_list* listaRegistros;
} tablaMem;

typedef struct{
	consistencia tipoConsistencia;
	int cantParticiones;
	int tiempoCompactacion;
	char* nombreTabla;
	sem_t *semaforoFS;
	sem_t *semaforoMemtable;
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
void enviarYLogearMensajeError(int socket, char* mensaje, ...);
void enviarOMostrarYLogearInfo(int socket, char* mensaje, ...);
void enviarYOLogearAlgo(int socket, char *mensaje, void(*log)(t_log *, char *), va_list parametrosAdicionales);
void liberarBloquesDeTmpYPart(char* nombreArchivo,char* rutaTabla);
void agregarALista(char* timestamp,char* key,char* value,t_list* head);
void soloLoggear(int socket,char* mensaje,...);
void soloLoggearError(int socket,char* mensaje,...);
sem_t *devolverSemaforoDeTablaFS(char* nombreTabla);
sem_t *devolverSemaforoDeTablaMemtable(char* nombreTabla);
void guardarRegistrosEnBloques(int tamanioTotalADumpear, int cantBloquesNecesarios, char** bloquesAsignados, char* buffer);
void soloLoggearResultados(int socket,int caso,char *mensaje, ...);

void guardarRegistrosEnBloques(int tamanioTotalADumpear, int cantBloquesNecesarios, char** bloquesAsignados, char* buffer) {

	int desplazamiento=0;
	int restante = tamanioTotalADumpear;
	int i;
	int j=0; //esto es para no desperdiciar espacio
	for(i=0; i<cantBloquesNecesarios;i++){
		j= i+1; //osea el proximo

		if(*(bloquesAsignados+j) == NULL){ //osea si es el ultimo bloque

			char* rutaBloque = string_new();
			string_append(&rutaBloque,puntoMontaje);
			string_append(&rutaBloque,"Bloques/");
			string_append(&rutaBloque,*(bloquesAsignados+i));
			string_append(&rutaBloque,".bin");
			FILE* fd = fopen(rutaBloque,"w");
			fwrite(buffer+desplazamiento,1,restante,fd);
			fclose(fd);
			free(rutaBloque);
			break;
		}

		char* rutaBloque = string_new();
		string_append(&rutaBloque,puntoMontaje);
		string_append(&rutaBloque,"Bloques/");
		string_append(&rutaBloque,*(bloquesAsignados+i)); //este es el numero de bloque donde escribo
		string_append(&rutaBloque,".bin");
		FILE* fd = fopen(rutaBloque,"w");
		fwrite(buffer+desplazamiento,1,tamanioBloques,fd);
		desplazamiento+= tamanioBloques;
		restante-=tamanioBloques;
		fclose(fd);
		free(rutaBloque);
	}
}

void soloLoggearError(int socket,char* mensaje,...){
	va_list parametrosAdicionales;
	va_start(parametrosAdicionales, mensaje);
	char* mensajeTotal = string_from_vformat(mensaje, parametrosAdicionales);
	if(socket==-1){
		sem_wait(&mutexLoggerConsola);
		log_error(loggerConsola, mensajeTotal);
		sem_post(&mutexLoggerConsola);
	}
	else{
		sem_wait(&mutexLogger);
		log_error(logger, mensajeTotal);
		sem_post(&mutexLogger);
	}
	free(mensajeTotal);
	va_end(parametrosAdicionales);
}
void soloLoggearResultados(int socket,int error,char *mensaje, ...){
	va_list parametrosAdicionales;
	va_start(parametrosAdicionales, mensaje);
	char* mensajeTotal = string_from_vformat(mensaje, parametrosAdicionales);
	if(socket==-1){
			if(error==1){ //error=1 significa que hubo algun error
				sem_wait(&mutexResultadosConsola);
				log_error(loggerResultadosConsola, mensajeTotal);
				sem_post(&mutexResultadosConsola);
			}
			else{
				sem_wait(&mutexResultadosConsola);
				log_info(loggerResultadosConsola, mensajeTotal);
				sem_post(&mutexResultadosConsola);
			}
		}
		else{
			if(error==1){
				sem_wait(&mutexResultados);
				log_error(loggerResultados, mensajeTotal);
				sem_post(&mutexResultados);
			}
			else{
				sem_wait(&mutexResultados);
				log_info(loggerResultados, mensajeTotal);
				sem_post(&mutexResultados);
			}
		}
		free(mensajeTotal);
		va_end(parametrosAdicionales);
}

void soloLoggear(int socket, char *mensaje, ...){
	va_list parametrosAdicionales;
	va_start(parametrosAdicionales, mensaje);
	char* mensajeTotal = string_from_vformat(mensaje, parametrosAdicionales);
	if(socket==-1){
		sem_wait(&mutexLoggerConsola);
		log_info(loggerConsola, mensajeTotal);
		sem_post(&mutexLoggerConsola);
	}
	else{
		sem_wait(&mutexLogger);
		log_info(logger, mensajeTotal);
		sem_post(&mutexLogger);
	}
	free(mensajeTotal);
	va_end(parametrosAdicionales);
}
sem_t *devolverSemaforoDeTablaFS(char* nombreTabla){
		bool seEncuentraTabla(void* elemento){
			metadata* unMetadata = (metadata*) elemento;
			return string_equals_ignore_case(unMetadata->nombreTabla,nombreTabla);
		}
	sem_wait(&mutexListaDeTablas);
	metadataConSemaforo* metadataBuscado = list_find(listaDeTablas,seEncuentraTabla);
	sem_post(&mutexListaDeTablas);
	return metadataBuscado->semaforoFS;
}

sem_t *devolverSemaforoDeTablaMemtable(char* nombreTabla){
		bool seEncuentraTabla(void* elemento){
			metadata* unMetadata = elemento;
			return string_equals_ignore_case(unMetadata->nombreTabla,nombreTabla);
		}
	sem_wait(&mutexListaDeTablas);
	metadataConSemaforo* metadataBuscado = list_find(listaDeTablas,seEncuentraTabla);
	sem_post(&mutexListaDeTablas);
	return metadataBuscado->semaforoMemtable;
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
		free(rutaCompleta);
		return;
	}

	t_config* archivo= config_create(rutaCompleta);

	while(archivo==NULL || !config_has_property(archivo, "BLOCKS")){
		soloLoggearError(-1,"Hubo un error al abrir el archivo en la ruta %s se volvera a intentar",rutaCompleta);
		archivo= config_create(rutaCompleta);
	}
	char **bloques = config_get_array_value(archivo,"BLOCKS");

	marcarBloquesComoLibre(bloques);
	liberarDoblePuntero(bloques);
	config_destroy(archivo);
	remove(rutaCompleta);
	free(rutaCompleta);
}


void enviarOMostrarYLogearInfo(int socket, char* mensaje, ...) {
	va_list parametrosAdicionales;
	va_start(parametrosAdicionales, mensaje);
	enviarYOLogearAlgo(socket, mensaje, (void*) log_info, parametrosAdicionales);
	va_end(parametrosAdicionales);
}

void enviarYOLogearAlgo(int socket, char *mensaje, void(*log)(t_log *, char *), va_list parametrosAdicionales){
	char* mensajeTotal = string_from_vformat(mensaje, parametrosAdicionales);
	if(socket != -1) {
		sem_wait(&mutexLogger);
		log(logger, mensajeTotal);
		sem_post(&mutexLogger);
		enviar(socket, mensajeTotal, strlen(mensajeTotal) + 1);
	} else {
		sem_wait(&mutexLoggerConsola);
		log(loggerConsola, mensajeTotal);
		sem_post(&mutexLoggerConsola);
	}
	free(mensajeTotal);
}

void enviarYLogearMensajeError(int socket, char* mensaje, ...) {
	va_list parametrosAdicionales;
	va_start(parametrosAdicionales, mensaje);
	enviarYOLogearAlgo(socket, mensaje, (void*) log_error, parametrosAdicionales);
	va_end(parametrosAdicionales);
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
	free(unMetadata->semaforoFS);
	free(unMetadata->semaforoMemtable);

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

int obtenerCantTemporales(char* nombreTabla){
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
		if(!fputs(info, fp)) {
			printf("Hubo un error cargando la informacion en el archivo");
		}
		fclose(fp);
		return;
	}
	soloLoggearError(-1,"No se pudo guardar la informacion en la ruta %s",ruta);
	fclose(fp);
}

void marcarBloquesComoLibre(char** arrayDeBloques){
	int pos =0;
	while(*(arrayDeBloques+pos)!=NULL){
			int posicionActual = atoi(*(arrayDeBloques+pos));
			char* ruta = string_new();
			string_append(&ruta,puntoMontaje);
			string_append(&ruta,"Bloques/");
			string_append(&ruta,*(arrayDeBloques+pos));
			string_append(&ruta,".bin");
			guardarInfoEnArchivo(ruta,"\0");
			sem_wait(&mutexBitarray);
			bitarray_clean_bit(bitarray, posicionActual);
			sem_post(&mutexBitarray);
			pos++;
			free(ruta);
		}
}

char* devolverBloqueLibre(){
	int i;

	int bloqueEncontrado = 0;
	int encontroBloque = 0;
	char* numero;

	for(i=0; i < cantDeBloques; i++){
		sem_wait(&mutexBitarray);
		bool bit = bitarray_test_bit(bitarray, i);
		sem_post(&mutexBitarray);
		if(bit == 0){
			encontroBloque = 1;
			bloqueEncontrado = i;
			break;
		}

	}

	if (encontroBloque == 1){
		sem_wait(&mutexBitarray);
		bitarray_set_bit(bitarray, bloqueEncontrado);
		sem_post(&mutexBitarray);
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

	while(archivo == -1) {
		soloLoggearError(-1, "No se pudo abrir el file descriptor del archivo %s. error: %d", rutaBloque, errno);
		archivo = open(rutaBloque, O_RDWR);
	}

	fstat(archivo,&sb);
	if (sb.st_size == 0){
		free(rutaBloque);
		close(archivo);
		return NULL;
	}
	char* informacion = mmap(NULL,tamanioBloques,PROT_READ,MAP_PRIVATE,archivo,NULL);
	close(archivo);
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
