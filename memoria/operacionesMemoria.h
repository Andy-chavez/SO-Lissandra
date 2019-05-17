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

void agregarSegmento(memoria* memoria,registro* primerRegistro,char* tabla ){
	segmento segmentoNuevo;
	segmentoNuevo.nombreTabla = tabla;
	segmentoNuevo.paginas = list_create();
	pagina primerPagina;
	primerPagina.unaPagina = primerRegistro;
	primerPagina.numeroPagina = 0;
	//list_add(segmentoNuevo.paginas,(void*) primerPagina);	no estoy sabiendo como meter esto
	// Habría que comprobar que hay espacio
	//list_add(memoria.tablaSegmentos,(void*) segmentoNuevo);    claramente asi no
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

bool igualNombreSegmento(segmento* segmento,void* nombreBase){
	char* nombre = (char*)nombreBase;
	return string_equals_ignore_case(segmento->nombreTabla,nombre);
}

bool existeSegmentoDeTabla(memoria* memoria,char* tablaNombre){
	return list_any_satisfy_2(memoria->tablaSegmentos,(void*)igualNombreSegmento,(void*)tablaNombre);
}

segmento* encontrarSegmentoPorNombre(memoria* memoria,char* tablaNombre){
	return list_find_2(memoria->tablaSegmentos,(void*)igualNombreSegmento,(void*) tablaNombre);  //esta bien esto? el list_add devuelve *void
}

bool igualKeyPagina(pagina* pagina,void* keyDada){
	int key = (int) keyDada;
	return pagina->unaPagina->key == key;
}

bool segmentoTieneKey(segmento* segmento,int keyDada){
	return list_any_satisfy_2(segmento->paginas,(void*)igualKeyPagina,(void*)keyDada);
}

pagina* encontrarPaginaPorKey(segmento* segmento, int key){
	return list_find_2(segmento->paginas,(void*)igualKeyPagina,(void*) key);
}
char* valuePagina(segmento* segmento, int key){
	pagina* paginaEncontrada = encontrarPaginaPorKey(segmento,key);
	return (char*)paginaEncontrada->unaPagina->value;
}

bool hayEspacioMemoria(memoria* memoria, int espacioPedido){ // Supongo que pedimos espacio porque puede quedar 1 de espacio y nunca se llena llena
	return (memoria->limite-memoria->base) > espacioPedido;
}

/*
void select(char*nombreTabla,int key){

	if(existeSegmentoDeTabla(memoriaPrincipal,nombreTabla)){
		segmento segmento = (segmento) encontrarSegmentoPorNombre(memoriaPrincipal,nombreTabla);

		if(segmentoTieneKey(segmento,key)){
			printf ("El valor es %s",valuePagina(segmento,key));
		}
		else{
			if(hay espacio) pedirPaginaLFS(segmento,key);
			else { journal(); pedirPaginaLFS(segmento,key);}
		}
	}
	error?, supongo que lo pide, no?
}*/

/*
void insert(char*nombreTabla,int key,char*value, memoria* memoriaPrincipal){ //la memoria no se bien como tratarla, por ahora la paso para que "funque"
	if(existeSegmentoDeTabla(memoriaPrincipal,nombreTabla)){
		segmento* segmento = encontrarSegmentoPorNombre(memoriaPrincipal,nombreTabla);

		if(segmentoTieneKey(segmento,key)){
			pagina* paginaEncontrada = encontrarPaginaPorKey(segmento,key);
			paginaEncontrada->unaPagina->value = value;
			paginaEncontrada->unaPagina->timestamp = time(NULL);
			paginaEncontrada->flag = SI;
			list_replace_and_destroy_element(segmento->paginas,paginaEncontrada->numeroPagina,paginaEncontrada,free);
			//Todo esto lo deberiamos mandar en una funcioncita aparte de "cambiarPagina" o algo asi?
		}

	}

	else{
		registro* registroAGuardar;
		registroAGuardar->key = key;
		registroAGuardar->timestamp=time(NULL);
		registroAGuardar->value=(void*)value;
		agregarSegmento(memoriaPrincipal,registroAGuardar,nombreTabla);
	}
}
*/
