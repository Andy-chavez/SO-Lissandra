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
	void* value;
} registro;

typedef struct {
	int numeroPagina;
	registro *unaPagina;
	flagModificado flag;
} pagina;


typedef struct {
	char *nombreTabla;
	t_list* paginas;
} segmento;

typedef struct {
	t_list* tablaSegmentos;
	void *base;
	void *limite;
	int *seeds;
} memoria;



memoria* inicializarMemoria(int tamanio){
	memoria* nuevaMemoria;

	nuevaMemoria->base = malloc(tamanio);
	nuevaMemoria->limite = nuevaMemoria->base + tamanio;
	nuevaMemoria->tablaSegmentos = list_create();

	return nuevaMemoria;

}

/*
char* select(char*nombreTabla,int key){

	if(existeSegmentoDeTabla(memoriaPrincipal,nombreTabla)){
		segmento segmento = (segmento) encontrarSegmentoPorNombre(memoriaPrincipal,nombreTabla);

		if(segmentoTieneKey(segmento,key){
			return valuePagina(segmento,key);
		}
		else{
			return pedirPaginaLFS(segmento,key);
		}
	}
	else{
	return pedirSegmentoLFS(nombreTabla,key);
	}
}*/



bool* igualNombre(segmento segmento,void* nombreBase){
	char* nombre = (char*)nombreBase;
	return string_equals_ignore_case(segmento.nombreTabla,nombre);
}

//--------------------Definicion de t_list's para dos Argumentos-------------------------------------------------

t_list* list_filter_2(t_list* self, bool(*condition)(void*,void*),void* argumento){
	t_list* filtered = list_create();

	void _add_if_apply(void* element) {
		if (condition(element,argumento)) {
			list_add(filtered, element);
		}
	}

	list_iterate(self, _add_if_apply);
	return filtered;
}

int list_count_satisfying_2(t_list* self, bool(*condition)(void*,void*),void* argumento){
	t_list *satisfying = list_filter_2(self, condition,argumento);
	int result = satisfying->elements_count;
	list_destroy(satisfying);
	return result;
}

bool list_any_satisfy_2(t_list* self, bool(*condition)(void*,void*),void* argumento){
	return list_count_satisfying_2(self, condition,argumento) > 0;
}

static t_link_element* list_find_element_2(t_list *self, bool(*condition)(void*,void*), int* index,void* argumento) {
	t_link_element *element = self->head;
	int position = 0;

	while (element != NULL && !condition(element->data,argumento)) {
		element = element->next;
		position++;
	}

	if (index != NULL) {
		*index = position;
	}

	return element;
}

void* list_find_2(t_list *self, bool(*condition)(void*,void*),void* argumento) {
	t_link_element *element = list_find_element_2(self, condition, NULL,argumento);
	return element != NULL ? element->data : NULL;
}

//---------------------------------------------------------------------------------------------------------------

bool existeSegmentoDeTabla(memoria* memoria,char* tablaNombre){
	return list_any_satisfy_2(memoria->tablaSegmentos,(void*)igualNombre,(void*)tablaNombre);
}

void* encontrarSegmentoPorNombre(memoria* memoria,char* tablaNombre){
	return list_find_2(memoria->tablaSegmentos,(void*)igualNombre,(void*) tablaNombre);
}
