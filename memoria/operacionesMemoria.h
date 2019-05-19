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
	int *seeds;
} memoria;

typedef struct {
	void* buffer;
	int tamanio;
} bufferDePagina;

// ------------------------------------------------------------------------ //
// OPERACIONES SOBRE MEMORIA PRINCIPAL //

memoria* inicializarMemoria(int tamanio){
	memoria* nuevaMemoria = malloc(sizeof(memoria));

	nuevaMemoria->base = malloc(tamanio);
	memset(nuevaMemoria->base, 0, tamanio);
	nuevaMemoria->limite = nuevaMemoria->base + tamanio;
	nuevaMemoria->tablaSegmentos = list_create();

	return nuevaMemoria;
}

void agregarSegmento(memoria* memoria,pagina* primeraPagina,char* tabla ){
	segmento segmentoNuevo;
	segmentoNuevo.nombreTabla = tabla;
	segmentoNuevo.tablaPaginas = list_create();
	paginaEnTabla primerPagina;
	primerPagina.unaPagina = primeraPagina;
	primerPagina.numeroPagina = 0;
	//list_add(segmentoNuevo.paginas,(void*) primerPagina);	no estoy sabiendo como meter esto
	// Habría que comprobar que hay espacio
	//list_add(memoria.tablaSegmentos,(void*) segmentoNuevo);    claramente asi no
}

int calcularEspacio(pagina* unaPagina) {
	int timeStamp = sizeof(time_t);
	int key = sizeof(uint16_t);
	int value = strlen(unaPagina->value) + 1;
	return timeStamp + key + value;
}

void* encontrarEspacio(memoria* memoriaPrincipal) {
	void* espacioLibre = memoriaPrincipal->base;
	while((int*) espacioLibre == 0) {
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
	memcpy(buffer, unaPagina->timestamp, sizeof(time_t));
	int desplazamiento = sizeof(time_t);
	memcpy(buffer + desplazamiento, unaPagina->key, sizeof(uint16_t));
	desplazamiento += sizeof(uint16_t);
	memcpy(buffer + desplazamiento, unaPagina->value, strlen(unaPagina->value) + 1);
	return buffer;
}

liberarBufferDePagina(bufferDePagina* buffer) {
	free(buffer->buffer);
	free(buffer);
}

void guardar(pagina* unaPagina, memoria* memoriaPrincipal) {
	bufferDePagina *bufferAGuardar = bufferPagina(unaPagina);
	void *guardarDesde = encontrarEspacio(memoriaPrincipal);
	memcpy(guardarDesde, bufferAGuardar->buffer, bufferAGuardar->tamanio);
	liberarBufferDePagina(bufferAGuardar);
}

bool guardarEnMemoria(pagina* unaPagina, memoria* memoriaPrincipal) {
	int espacioNecesario = calcularEspacio(unaPagina);
	if(hayEspacioSuficientePara(memoriaPrincipal, espacioNecesario)){
		guardar(unaPagina, memoriaPrincipal);
		return true;
	} else {
		return false;
	}
}

pagina* leerDatosEnMemoria(paginaEnTabla* unaPagina) {
	pagina* paginaARetornar = malloc(sizeof(pagina));

	memcpy(&(paginaARetornar->timestamp),(time_t*) unaPagina->unaPagina, sizeof(time_t));
	int desplazamiento = sizeof(time_t);

	memcpy(&(paginaARetornar->key), (uint16_t*) (unaPagina->unaPagina + desplazamiento), sizeof(uint16_t));
	desplazamiento += sizeof(uint16_t);

	int tamanioValue = obtenerTamanioValue((unaPagina->unaPagina + desplazamiento));
	paginaARetornar->value = malloc(tamanioValue);
	memcpy(paginaARetornar->value, (unaPagina->unaPagina + desplazamiento), tamanioValue);

	return paginaARetornar;
}

void cambiarDatosEnMemoria(paginaEnTabla* paginaACambiar, pagina* paginaNueva) {
	bufferDePagina* bufferParaCambio = bufferPagina(paginaNueva);
	memcpy(paginaACambiar->unaPagina, bufferParaCambio->buffer, bufferParaCambio->tamanio);
}

// ------------------------------------------------------------------------ //
// OPERACIONES SOBRE LISTAS, TABLAS Y PAGINAS //

int obtenerTamanioValue(void* valueBuffer) {
	int tamanio = 0;
	while((char*) valueBuffer != '\n') {
		tamanio++;
		valueBuffer++;
	}
	return tamanio;
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
	return paginaReal->key == keyDada;
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
	return paginaReal->value;
}

// ------------------------------------------------------------------------ //
// OPERACIONESLQL //

void selectLQL(char*nombreTabla,int key, memoria* memoriaPrincipal){
	segmento* unSegmento;
	if(unSegmento = encontrarSegmentoPorNombre(memoriaPrincipal,nombreTabla)){
	paginaEnTabla* paginaEncontrada;
		if(paginaEncontrada = encontrarPaginaPorKey(unSegmento,key)){
			printf ("El valor es %s",valuePagina(unSegmento,key));
		}
		//else{
			//paginaEncontrada = pedirRegistroLFS(unSegmento,key);
			//int espacioNecesario = calcularEspacio(paginaEncontrada);
			//if(hayEspacioMemoria(espacioNecesario)){
			//	guardarEnMemoria(paginaEncontrada);
			//
			//};
			// else journal();
	}
}


void insertLQL(char*nombreTabla, pagina* paginaNueva, memoria* memoriaPrincipal){ //la memoria no se bien como tratarla, por ahora la paso para que "funque"
	segmento* unSegmento;
	if(unSegmento = encontrarSegmentoPorNombre(memoriaPrincipal,nombreTabla)){
		paginaEnTabla* paginaEncontrada;
		if(paginaEncontrada = encontrarPaginaPorKey(unSegmento,paginaNueva->key)){
			cambiarDatosEnMemoria(paginaEncontrada, paginaNueva);
			list_replace_and_destroy_element(unSegmento->tablaPaginas,paginaEncontrada->numeroPagina,paginaEncontrada,free);
			guardarEnMemoria(paginaEncontrada->unaPagina, memoriaPrincipal);
			//Todo esto lo deberiamos mandar en una funcioncita aparte de "cambiarPagina" o algo asi?
		}
	}

	else{
		agregarSegmento(memoriaPrincipal,paginaNueva,nombreTabla);
	}
}

