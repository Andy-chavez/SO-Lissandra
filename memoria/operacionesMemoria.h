/*
 * operacionesMemoria.c
 *
 *  Created on: 4 may. 2019
 *      Author: utnso
 */

#include <time.h>
#include <inttypes.h>
#include <commons/config.h>
#include <commons/log.h>
#include <stdlib.h>
#include <commons/collections/list.h>
#include <pthread.h>
#include <commons/string.h>
#include <stdbool.h>
#include <stdio.h>

typedef struct {
	t_config* config;
	t_log* logger;
} configYLogs;

typedef enum {
	NO,
	SI
} flagModificado;

typedef struct {
	time_t timestamp;
	uint16_t key;
	char* value;
} registro;

typedef struct {
	int numeroregistro;
	void *unaregistro;
	flagModificado flag;
} paginaEnTabla;

typedef struct {
	char *nombreTabla;
	t_list* tablaregistros;
} segmento;

typedef struct {
	t_list* tablaSegmentos;
	void *base;
	void *limite;
	int tamanioMaximoValue;
	int *seeds;
} memoria;

typedef struct {
	void* buffer;
	int tamanio;
} bufferDeregistro;

typedef struct {
	int tamanio;
} datosInicializacion;

int socketLissandraFS;

// ------------------------------------------------------------------------ //
// OPERACIONES SOBRE MEMORIA PRINCIPAL //

memoria* inicializarMemoria(datosInicializacion* datosParaInicializar, configYLogs* configYLog) {
	memoria* nuevaMemoria = malloc(sizeof(memoria));
	int tamanioMemoria = config_get_int_value(configYLog->config, "TAMANIOMEM");

	nuevaMemoria->base = malloc(tamanioMemoria);
	memset(nuevaMemoria->base, 0, tamanioMemoria);
	nuevaMemoria->limite = nuevaMemoria->base + tamanioMemoria;
	nuevaMemoria->tamanioMaximoValue = datosParaInicializar->tamanio;
	nuevaMemoria->tablaSegmentos = list_create();

	if(nuevaMemoria == NULL || nuevaMemoria->base == NULL) {
		log_error(configYLog->logger, "hubo un error al inicializar la memoria");
		return NULL;
	}

	log_info(configYLog->logger, "Memoria inicializada.");
	return nuevaMemoria;
}

void liberarMemoria(memoria* memoriaPrincipal) {
	void* liberarPaginas(paginaEnTabla* unaregistro) {
		free(unaregistro);
	}

	void* liberarSegmentos(segmento* unSegmento) {
		free(unSegmento->nombreTabla);
		list_destroy_and_destroy_elements(unSegmento->tablaregistros, liberarPaginas);
		free(unSegmento);
	}

	free(memoriaPrincipal->base);
	list_destroy_and_destroy_elements(memoriaPrincipal->tablaSegmentos, liberarSegmentos);
	free(memoriaPrincipal);
}

int calcularEspacio(registroConNombreTabla* unRegistro) {
	int timeStamp = sizeof(time_t);
	int key = sizeof(uint16_t);
	int value = strlen(unRegistro->value) + 1;
	return timeStamp + key + value;
}

void* encontrarEspacio(memoria* memoriaPrincipal) {
	void* espacioLibre = memoriaPrincipal->base;
	while(*((int*) espacioLibre) != 0) {
		espacioLibre++;
	}
	return espacioLibre;
}

bool hayEspacioSuficientePara(memoria* memoria, int espacioPedido){
	return (memoria->limite - encontrarEspacio(memoria)) > espacioPedido;
}

bufferDeregistro *armarBufferDePagina(registroConNombreTabla* unaregistro) {
	bufferDeregistro* buffer = malloc(sizeof(bufferDeregistro));
	buffer->tamanio = sizeof(time_t) + sizeof(uint16_t) + strlen(unaregistro->value) + 1;
	buffer->buffer = malloc(buffer->tamanio);

	memcpy(buffer->buffer, &(unaregistro->timestamp), sizeof(time_t));
	int desplazamiento = sizeof(time_t);

	memcpy(buffer->buffer + desplazamiento, &(unaregistro->key), sizeof(uint16_t));
	desplazamiento += sizeof(uint16_t);

	memcpy(buffer->buffer + desplazamiento, unaregistro->value, strlen(unaregistro->value) + 1);
	return buffer;
}

void liberarBufferDeregistro(bufferDeregistro* buffer) {
	free(buffer->buffer);
	free(buffer);
}

void* guardar(registroConNombreTabla* unaregistro, memoria* memoriaPrincipal) {
	bufferDeregistro *bufferAGuardar = armarBufferDePagina(unaregistro);
	void *guardarDesde = encontrarEspacio(memoriaPrincipal);

	memcpy(guardarDesde, bufferAGuardar->buffer, bufferAGuardar->tamanio);

	liberarBufferDeregistro(bufferAGuardar);

	return guardarDesde;
}

void* guardarEnMemoria(registroConNombreTabla* unRegistro, memoria* memoriaPrincipal) {
	int espacioNecesario = calcularEspacio(unRegistro);

	if(hayEspacioSuficientePara(memoriaPrincipal, espacioNecesario)){
		return guardar(unRegistro, memoriaPrincipal);

	} else {
		return NULL;
	}
}
int agregarSegmento(memoria* memoria,registro* primeraregistro,char* tabla ){

	paginaEnTabla* primerregistro = malloc(sizeof(paginaEnTabla));
	if(!(primerregistro->unaregistro = guardarEnMemoria(primeraregistro, memoria))) {
		// TODO Avisar que no se pudo guardar en memoria.
		return -1;
	};
	primerregistro->numeroregistro = 0;

	segmento* segmentoNuevo = malloc(sizeof(segmento));
	segmentoNuevo->nombreTabla = tabla;
	segmentoNuevo->tablaregistros = list_create();


	list_add(segmentoNuevo->tablaregistros, primerregistro);

	list_add(memoria->tablaSegmentos, segmentoNuevo);

	return 0;
}

registro* leerDatosEnMemoria(paginaEnTabla* unaregistro) {
	registro* registroARetornar = malloc(sizeof(registro));

	memcpy(&(registroARetornar->timestamp),(time_t*) unaregistro->unaregistro, sizeof(time_t));
	int desplazamiento = sizeof(time_t);

	memcpy(&(registroARetornar->key), (uint16_t*) (unaregistro->unaregistro + desplazamiento), sizeof(uint16_t));
	desplazamiento += sizeof(uint16_t);

	int tamanioValue = obtenerTamanioValue((unaregistro->unaregistro + desplazamiento)) + 1;
	registroARetornar->value = malloc(tamanioValue);
	memcpy(registroARetornar->value, (unaregistro->unaregistro + desplazamiento), tamanioValue);

	return registroARetornar;
}

void cambiarDatosEnMemoria(paginaEnTabla* registroACambiar, registro* registroNuevo) {
	bufferDeregistro* bufferParaCambio = armarBufferDePagina(registroNuevo);
	memcpy(registroACambiar->unaregistro, bufferParaCambio->buffer, bufferParaCambio->tamanio);
	liberarBufferDeregistro(bufferParaCambio);
}

// ------------------------------------------------------------------------ //
// OPERACIONES SOBRE LISTAS, TABLAS Y registroS //

registroConNombreTabla* pedirRegistroLFS(operacionLQL *operacion) {
	serializarYEnviarOperacionLQL(socketLissandraFS, operacion);

	void* bufferRespuesta = recibir(socketLissandraFS);
	registroConNombreTabla* paginaEncontradaEnLFS = deserializarRegistro(bufferRespuesta);

	return paginaEncontradaEnLFS;
}


void* obtenerValorDe(char* parametros, int lugarDelParametroBuscado) {
	char** parametrosSpliteados = string_split(parametros, " ");
	char* parametroBuscado = *(parametrosSpliteados + lugarDelParametroBuscado);
	return parametroBuscado;
}

registro* crearRegistroNuevo(char* parametros) {
	registro* nuevaregistro = malloc(sizeof(registro));

	nuevaregistro->timestamp = time(NULL);
	nuevaregistro->key = *(uint16_t*) obtenerValorDe(parametros, 1);
	nuevaregistro->value = *(time_t*) obtenerValorDe(parametros, 2);

	return nuevaregistro;
}

int obtenerTamanioValue(void* valueBuffer) {
	int tamanio = 0;
	char prueba = *((char*) valueBuffer);
	while(prueba != '\0') {
		tamanio++;
		valueBuffer++;
		prueba = *((char*) valueBuffer);
	}
	return tamanio;
}

void liberarregistro(registro* unaregistro) {
	free(unaregistro->value);
	free(unaregistro);
}

bool tienenIgualNombre(char* unNombre,char* otroNombre){
	return string_equals_ignore_case(unNombre, otroNombre);
}

segmento* encontrarSegmentoPorNombre(memoria* memoria,char* tablaNombre){
	bool segmentoDeIgualNombre(segmento* unSegmento) {
		return tienenIgualNombre(unSegmento->nombreTabla, tablaNombre);
	}

	return (segmento*) list_find(memoria->tablaSegmentos,(void*)segmentoDeIgualNombre);
}

bool igualKeyregistro(paginaEnTabla* unaregistro,int keyDada){
	registro* registroReal = leerDatosEnMemoria(unaregistro);
	bool respuesta = registroReal->key == keyDada;

	liberarregistro(registroReal);
	return respuesta;
}

paginaEnTabla* encontrarregistroPorKey(segmento* unSegmento, int keyDada){
	bool tieneIgualKeyQueDada(paginaEnTabla* unaregistro) {
			return igualKeyregistro(unaregistro, keyDada);
	}

	return (paginaEnTabla*) list_find(unSegmento->tablaregistros,(void*)tieneIgualKeyQueDada);
}
char* valueregistro(segmento* unSegmento, int key){
	paginaEnTabla* paginaEncontrada = encontrarregistroPorKey(unSegmento,key);

	registro* registroReal = leerDatosEnMemoria(paginaEncontrada);
	char* value = malloc(strlen(registroReal->value) + 1);
	memcpy(value, registroReal->value, strlen(registroReal->value) + 1);

	liberarregistro(registroReal);
	return value;
}

// ------------------------------------------------------------------------ //
// OPERACIONESLQL //

void selectLQL(operacionLQL *operacionSelect, configYLogs* configYLog, memoria* memoriaPrincipal, int socketKernel){
	char* nombreTabla = (char*) obtenerValorDe(operacionSelect->parametros, 1);
	int key = *(int*) obtenerValorDe(operacionSelect->parametros, 2);
	segmento* unSegmento;
	if(unSegmento = encontrarSegmentoPorNombre(memoriaPrincipal,nombreTabla)){
	paginaEnTabla* paginaEncontrada;
		if(paginaEncontrada = encontrarregistroPorKey(unSegmento,key)){
			char* value = valueregistro(unSegmento,key);
			printf ("El valor es %s\n", value);
			enviar(socketKernel, (void*) value, strlen(value) + 1);
		}
		else {
			registroConNombreTabla* registroLFS = pedirRegistroLFS(operacionSelect);

			if(guardarEnMemoria(registroLFS, memoriaPrincipal)) {
				// TODO enviar(socketKernel, (void*) registroNuevo->value, strlen(registroNuevo->value) + 1);
			} else {
				// TODO Dio error al guardar
			};

			// TODO else journal();
		}
	} else {
		registroConNombreTabla* registroLFS = pedirRegistroLFS(operacionSelect);
		int espacioNecesario = calcularEspacio(registroLFS);
		if(hayEspacioSuficientePara(memoriaPrincipal, espacioNecesario)){
			agregarSegmento(memoriaPrincipal, registroLFS, nombreTabla);
		}
		// TODO enviar(socketKernel, (void*) registroNuevo->value, strlen(registroNuevo->value) + 1);
	}
}

void insertLQL(operacionLQL* operacionInsert, configYLogs* configYLog, memoria* memoriaPrincipal){ //la memoria no se bien como tratarla, por ahora la paso para que "funque"
	char* nombreTabla = (char*) obtenerValorDe(operacionInsert->parametros, 0);
	registro* registroNuevo = crearRegistroNuevo(operacionInsert->parametros);
	segmento* unSegmento;
	if(unSegmento = encontrarSegmentoPorNombre(memoriaPrincipal,nombreTabla)){
		paginaEnTabla* paginaEncontrada;
		if(paginaEncontrada = encontrarregistroPorKey(unSegmento,registroNuevo->key)){
			cambiarDatosEnMemoria(paginaEncontrada, registroNuevo);
			paginaEncontrada->flag = SI;
		}
	}

	else{
		log_info(configYLog->logger, "No existia el segmento, es nuevo!");
		agregarSegmento(memoriaPrincipal,registroNuevo,nombreTabla);
	}
	log_info(configYLog->logger, "capaz lo hizo bien xd");
}

