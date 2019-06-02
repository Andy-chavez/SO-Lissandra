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
	void *unRegistro;
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
	void* liberarPaginas(paginaEnTabla* unRegistro) {
		free(unRegistro);
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

int calcularEspacioParaUnRegistro(memoria* memoriaPrincipal) {
	// TODO cambiar para que solo calcule una vez, ya que el espacio necesario es unico y no varia.
	int timeStamp = sizeof(time_t);
	int key = sizeof(uint16_t);
	int value = memoriaPrincipal->tamanioMaximoValue;
	return timeStamp + key + value;
}

void* encontrarEspacio(memoria* memoriaPrincipal) {
	// TODO cambiar para que encuentre pagina por pagina.
	void* espacioLibre = memoriaPrincipal->base;
	while(*((int*) espacioLibre) != 0) {
		espacioLibre++;
	}
	return espacioLibre;
}

bool hayEspacioSuficienteParaUnRegistro(memoria* memoria){
	return (memoria->limite - encontrarEspacio(memoria)) > calcularEspacioParaUnRegistro(memoria);
}

bufferDePagina *armarBufferDePagina(registroConNombreTabla* unRegistro, int tamanioValueMaximo) {
	bufferDePagina* buffer = malloc(sizeof(bufferDePagina));
	int tamanioValueRegistro = strlen(unRegistro->value) + 1;
	buffer->tamanio = sizeof(time_t) + sizeof(uint16_t) + tamanioValueMaximo;
	buffer->buffer = malloc(buffer->tamanio);

	memcpy(buffer->buffer, &(unRegistro->timestamp), sizeof(time_t));
	int desplazamiento = sizeof(time_t);

	memcpy(buffer->buffer + desplazamiento, &(unRegistro->key), sizeof(uint16_t));
	desplazamiento += sizeof(uint16_t);

	memcpy(buffer->buffer + desplazamiento, unRegistro->value, tamanioValueRegistro);
	desplazamiento += tamanioValueRegistro;
	memset(buffer->buffer + desplazamiento, 1, tamanioValueMaximo - tamanioValueRegistro);

	return buffer;
}

void liberarbufferDePagina(bufferDePagina* buffer) {
	free(buffer->buffer);
	free(buffer);
}

void* guardar(registroConNombreTabla* unRegistro, memoria* memoriaPrincipal) {
	bufferDePagina *bufferAGuardar = armarBufferDePagina(unRegistro, memoriaPrincipal->tamanioMaximoValue);
	void *guardarDesde = encontrarEspacio(memoriaPrincipal);

	memcpy(guardarDesde, bufferAGuardar->buffer, bufferAGuardar->tamanio);

	liberarbufferDePagina(bufferAGuardar);

	return guardarDesde;
}

void* guardarEnMemoria(registroConNombreTabla* unRegistro, memoria* memoriaPrincipal) {

	if(hayEspacioSuficienteParaUnRegistro(memoriaPrincipal)){
		return guardar(unRegistro, memoriaPrincipal);
	} else {
		return NULL;
	}

}
int agregarSegmento(memoria* memoria,registro* primeraregistro,char* tabla ){

	paginaEnTabla* primerregistro = malloc(sizeof(paginaEnTabla));
	if(!(primerregistro->unRegistro = guardarEnMemoria(primeraregistro, memoria))) {
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

registro* leerDatosEnMemoria(paginaEnTabla* unRegistro) {
	registro* registroARetornar = malloc(sizeof(registro));

	memcpy(&(registroARetornar->timestamp),(time_t*) unRegistro->unRegistro, sizeof(time_t));
	int desplazamiento = sizeof(time_t);

	memcpy(&(registroARetornar->key), (uint16_t*) (unRegistro->unRegistro + desplazamiento), sizeof(uint16_t));
	desplazamiento += sizeof(uint16_t);

	int tamanioValue = obtenerTamanioValue((unRegistro->unRegistro + desplazamiento)) + 1;
	registroARetornar->value = malloc(tamanioValue);
	memcpy(registroARetornar->value, (unRegistro->unRegistro + desplazamiento), tamanioValue);

	return registroARetornar;
}

void cambiarDatosEnMemoria(paginaEnTabla* registroACambiar, registro* registroNuevo, memoria* memoriaPrincipal) {
	bufferDePagina* bufferParaCambio = armarBufferDePagina(registroNuevo, memoriaPrincipal->tamanioMaximoValue);
	memcpy(registroACambiar->unRegistro, bufferParaCambio->buffer, bufferParaCambio->tamanio);
	liberarbufferDePagina(bufferParaCambio);
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
	return (void*) parametroBuscado;
}

registro* crearRegistroNuevo(char* parametros) {
	registro* nuevaregistro = malloc(sizeof(registro));

	nuevaregistro->timestamp = time(NULL);
	nuevaregistro->key = *(uint16_t*) obtenerValorDe(parametros, 1);
	nuevaregistro->value = (char*) obtenerValorDe(parametros, 2);

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

void liberarRegistro(registro* unRegistro) {
	free(unRegistro->value);
	free(unRegistro);
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

bool igualKeyRegistro(paginaEnTabla* unRegistro,int keyDada){
	registro* registroReal = leerDatosEnMemoria(unRegistro);
	bool respuesta = registroReal->key == keyDada;

	liberarRegistro(registroReal);
	return respuesta;
}

paginaEnTabla* encontrarRegistroPorKey(segmento* unSegmento, int keyDada){
	bool tieneIgualKeyQueDada(paginaEnTabla* unRegistro) {
			return igualKeyRegistro(unRegistro, keyDada);
	}

	return (paginaEnTabla*) list_find(unSegmento->tablaregistros,(void*)tieneIgualKeyQueDada);
}
char* valueRegistro(segmento* unSegmento, int key){
	paginaEnTabla* paginaEncontrada = encontrarRegistroPorKey(unSegmento,key);

	registro* registroReal = leerDatosEnMemoria(paginaEncontrada);
	char* value = malloc(strlen(registroReal->value) + 1);
	memcpy(value, registroReal->value, strlen(registroReal->value) + 1);

	liberarRegistro(registroReal);
	return value;
}

// ------------------------------------------------------------------------ //
// OPERACIONESLQL //

void selectLQL(operacionLQL *operacionSelect, configYLogs* configYLog, memoria* memoriaPrincipal, int socketKernel){
	char* nombreTabla = (char*) obtenerValorDe(operacionSelect->parametros, 0);
	int key = *(uint16_t*) obtenerValorDe(operacionSelect->parametros, 1);
	segmento* unSegmento;
	if(unSegmento = encontrarSegmentoPorNombre(memoriaPrincipal,nombreTabla)){
	paginaEnTabla* paginaEncontrada;
		if(paginaEncontrada = encontrarRegistroPorKey(unSegmento,key)){
			char* value = valueRegistro(unSegmento,key);
			printf ("El valor es %s\n", value);
			enviar(socketKernel, (void*) value, strlen(value) + 1);
		}
		else {
			registroConNombreTabla* registroLFS = pedirRegistroLFS(operacionSelect);

			if(guardarEnMemoria(registroLFS, memoriaPrincipal)) {
				// TODO enviar(socketKernel, (void*) registroNuevo->value, strlen(registroNuevo->value) + 1);
			} else {
				// TODO Dio error al guardar la pagina.
			};

			// TODO else journal();
		}
	} else {
		registroConNombreTabla* registroLFS = pedirRegistroLFS(operacionSelect);
		if(agregarSegmento(memoriaPrincipal, registroLFS, nombreTabla)){;
		// TODO enviar(socketKernel, (void*) registroNuevo->value, strlen(registroNuevo->value) + 1);
		} else {
			// TODO dio error al guardar el segmento.
		}
	}
}

void insertLQL(operacionLQL* operacionInsert, configYLogs* configYLog, memoria* memoriaPrincipal){ //la memoria no se bien como tratarla, por ahora la paso para que "funque"
	char* nombreTabla = (char*) obtenerValorDe(operacionInsert->parametros, 0);
	registro* registroNuevo = crearRegistroNuevo(operacionInsert->parametros);
	segmento* unSegmento;
	if(unSegmento = encontrarSegmentoPorNombre(memoriaPrincipal,nombreTabla)){
		paginaEnTabla* paginaEncontrada;
		if(paginaEncontrada = encontrarRegistroPorKey(unSegmento,registroNuevo->key)){
			cambiarDatosEnMemoria(paginaEncontrada, registroNuevo, memoriaPrincipal);
			paginaEncontrada->flag = SI;
		}
	}

	else{
		log_info(configYLog->logger, "No existia el segmento, es nuevo!");
		agregarSegmento(memoriaPrincipal,registroNuevo,nombreTabla);
	}
	log_info(configYLog->logger, "capaz lo hizo bien xd");
}

