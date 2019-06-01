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
} pagina;

typedef struct {
	int numeroPagina;
	void *unaPagina;
	flagModificado flag;
} paginaEnTabla;

typedef struct {
	char *nombreTabla;
	t_list* tablaPaginas;
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
} bufferDePagina;

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
	void* liberarPaginas(paginaEnTabla* unaPagina) {
		free(unaPagina);
	}

	void* liberarSegmentos(segmento* unSegmento) {
		free(unSegmento->nombreTabla);
		list_destroy_and_destroy_elements(unSegmento->tablaPaginas, liberarPaginas);
		free(unSegmento);
	}

	free(memoriaPrincipal->base);
	list_destroy_and_destroy_elements(memoriaPrincipal->tablaSegmentos, liberarSegmentos);
	free(memoriaPrincipal);
}

int calcularEspacio(pagina* unaPagina) {
	int timeStamp = sizeof(time_t);
	int key = sizeof(uint16_t);
	int value = strlen(unaPagina->value) + 1;
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

bufferDePagina *bufferPagina(pagina* unaPagina) {
	bufferDePagina* buffer = malloc(sizeof(bufferDePagina));
	buffer->tamanio = sizeof(time_t) + sizeof(uint16_t) + strlen(unaPagina->value) + 1;
	buffer->buffer = malloc(buffer->tamanio);

	memcpy(buffer->buffer, &(unaPagina->timestamp), sizeof(time_t));
	int desplazamiento = sizeof(time_t);

	memcpy(buffer->buffer + desplazamiento, &(unaPagina->key), sizeof(uint16_t));
	desplazamiento += sizeof(uint16_t);

	memcpy(buffer->buffer + desplazamiento, unaPagina->value, strlen(unaPagina->value) + 1);
	return buffer;
}

void liberarBufferDePagina(bufferDePagina* buffer) {
	free(buffer->buffer);
	free(buffer);
}

void* guardar(pagina* unaPagina, memoria* memoriaPrincipal) {
	bufferDePagina *bufferAGuardar = bufferPagina(unaPagina);
	void *guardarDesde = encontrarEspacio(memoriaPrincipal);

	memcpy(guardarDesde, bufferAGuardar->buffer, bufferAGuardar->tamanio);

	liberarBufferDePagina(bufferAGuardar);

	return guardarDesde;
}

void* guardarEnMemoria(pagina* unaPagina, memoria* memoriaPrincipal) {
	int espacioNecesario = calcularEspacio(unaPagina);

	if(hayEspacioSuficientePara(memoriaPrincipal, espacioNecesario)){
		return guardar(unaPagina, memoriaPrincipal);

	} else {
		return NULL;
	}
}
void agregarSegmento(memoria* memoria,pagina* primeraPagina,char* tabla ){

	segmento* segmentoNuevo = malloc(sizeof(segmento));
	segmentoNuevo->nombreTabla = tabla;
	segmentoNuevo->tablaPaginas = list_create();

	paginaEnTabla* primerPagina = malloc(sizeof(paginaEnTabla));
	primerPagina->numeroPagina = 0;
	primerPagina->unaPagina = guardarEnMemoria(primeraPagina, memoria);

	list_add(segmentoNuevo->tablaPaginas, primerPagina);

	list_add(memoria->tablaSegmentos, segmentoNuevo);
}

pagina* leerDatosEnMemoria(paginaEnTabla* unaPagina) {
	pagina* paginaARetornar = malloc(sizeof(pagina));

	memcpy(&(paginaARetornar->timestamp),(time_t*) unaPagina->unaPagina, sizeof(time_t));
	int desplazamiento = sizeof(time_t);

	memcpy(&(paginaARetornar->key), (uint16_t*) (unaPagina->unaPagina + desplazamiento), sizeof(uint16_t));
	desplazamiento += sizeof(uint16_t);

	int tamanioValue = obtenerTamanioValue((unaPagina->unaPagina + desplazamiento)) + 1;
	paginaARetornar->value = malloc(tamanioValue);
	memcpy(paginaARetornar->value, (unaPagina->unaPagina + desplazamiento), tamanioValue);

	return paginaARetornar;
}

void cambiarDatosEnMemoria(paginaEnTabla* paginaACambiar, pagina* paginaNueva) {
	bufferDePagina* bufferParaCambio = bufferPagina(paginaNueva);
	memcpy(paginaACambiar->unaPagina, bufferParaCambio->buffer, bufferParaCambio->tamanio);
	liberarBufferDePagina(bufferParaCambio);
}

// ------------------------------------------------------------------------ //
// OPERACIONES SOBRE LISTAS, TABLAS Y PAGINAS //

operacionLQL *armarOperacionLQL(char* nombreOperacion, char** parametros) {
	operacionLQL *operacionARetornar = malloc(sizeof(operacionLQL));
	operacionARetornar->operacion = nombreOperacion;
	operacionARetornar->parametros = concatenarParametros(parametros);
	return operacionARetornar;
}

pagina* enviarYRecibirRegistro(void *bufferOperacion, char** nombreTabla) {
	enviar(socketLissandraFS, bufferOperacion, 31);
	void* bufferRespuesta = recibir(socketLissandraFS);
	registro* registroADevolver = deserializarRegistro(bufferRespuesta, *(nombreTabla));
	return (pagina*) registroADevolver;
}

pagina* pedirRegistroLFS(char* operacion, char** parametros, char** nombreTabla) {
	operacionLQL *operacionAEnviar = armarOperacionLQL(operacion, parametros);
	void* buffer = serializarOperacionLQL(operacionAEnviar);
	pagina* paginaEncontradaEnLFS = enviarYRecibirRegistro(buffer, nombreTabla);

	return paginaEncontradaEnLFS;
}

pagina* crearPaginaNueva(char* parametros) {
	pagina* nuevaPagina = malloc(sizeof(pagina));

	nuevaPagina->timestamp = time(NULL);
	nuevaPagina->key = obtenerValorDe(parametros, 1);
	nuevaPagina->value = obtenerValorDe(parametros, 2);

	return nuevaPagina;
}

void* obtenerValorDe(char* parametros, int lugarDelParametroBuscado) {
	char** parametrosSpliteados = string_split(parametros, " ");
	char* parametroBuscado = *(parametrosSpliteados + lugarDelParametroBuscado);
	return parametroBuscado;
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

void liberarPagina(pagina* unaPagina) {
	free(unaPagina->value);
	free(unaPagina);
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

bool igualKeyPagina(paginaEnTabla* unaPagina,int keyDada){
	pagina* paginaReal = leerDatosEnMemoria(unaPagina);
	bool respuesta = paginaReal->key == keyDada;

	liberarPagina(paginaReal);
	return respuesta;
}

paginaEnTabla* encontrarPaginaPorKey(segmento* unSegmento, int keyDada){
	bool tieneIgualKeyQueDada(paginaEnTabla* unaPagina) {
			return igualKeyPagina(unaPagina, keyDada);
	}

	return (paginaEnTabla*) list_find(unSegmento->tablaPaginas,(void*)tieneIgualKeyQueDada);
}
char* valuePagina(segmento* unSegmento, int key){
	paginaEnTabla* paginaEncontrada = encontrarPaginaPorKey(unSegmento,key);

	pagina* paginaReal = leerDatosEnMemoria(paginaEncontrada);
	char* value = malloc(strlen(paginaReal->value) + 1);
	memcpy(value, paginaReal->value, strlen(paginaReal->value) + 1);

	liberarPagina(paginaReal);
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
		if(paginaEncontrada = encontrarPaginaPorKey(unSegmento,key)){
			char* value = valuePagina(unSegmento,key);
			printf ("El valor es %s\n", value);
			enviar(socketKernel, (void*) value, strlen(value) + 1);
		}
		else {
			pagina* paginaNueva = pedirRegistroLFS("Select", parametrosDeSelect, NULL);
			int espacioNecesario = calcularEspacio(paginaNueva);
			if(hayEspacioSuficientePara(memoriaPrincipal, espacioNecesario)){
				guardarEnMemoria(paginaNueva, memoriaPrincipal);
			}
			enviar(socketKernel, (void*) paginaNueva->value, strlen(paginaNueva->value) + 1);
			// else journal();
		}
	} else {
		char* nombreTabla;
		pagina* paginaNueva = pedirRegistroLFS("Select", parametrosDeSelect, &nombreTabla);
		int espacioNecesario = calcularEspacio(paginaNueva);
		if(hayEspacioSuficientePara(memoriaPrincipal, espacioNecesario)){
			agregarSegmento(memoriaPrincipal, paginaNueva, nombreTabla);
		}
		enviar(socketKernel, (void*) paginaNueva->value, strlen(paginaNueva->value) + 1);
	}
}


void insertLQL(operacionLQL* operacionInsert, configYLogs* configYLog, memoria* memoriaPrincipal){ //la memoria no se bien como tratarla, por ahora la paso para que "funque"
	char* nombreTabla = (char*) obtenerValorDe(operacionInsert->parametros, 0);
	pagina* paginaNueva = crearPaginaNueva(operacionInsert->parametros);
	segmento* unSegmento;
	if(unSegmento = encontrarSegmentoPorNombre(memoriaPrincipal,nombreTabla)){
		paginaEnTabla* paginaEncontrada;
		if(paginaEncontrada = encontrarPaginaPorKey(unSegmento,paginaNueva->key)){
			cambiarDatosEnMemoria(paginaEncontrada, paginaNueva);
			paginaEncontrada->flag = SI;
		}
	}

	else{
		log_info(configYLog->logger, "No existia el segmento, es nuevo!");
		agregarSegmento(memoriaPrincipal,paginaNueva,nombreTabla);
	}
	log_info(configYLog->logger, "capaz lo hizo bien xd");
}

