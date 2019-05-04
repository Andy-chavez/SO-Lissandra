/*
 * operacionesMemoria.c
 *
 *  Created on: 4 may. 2019
 *      Author: utnso
 */

#include <time.h>
#include <inttypes.h>

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
	pagina *paginas;
} tablaPaginas;

typedef struct {
	char *nombreTabla;
	pagina paginas[];
} segmento;

typedef struct tabla {
	segmento *unSegmento;
	struct tablaSegmentos *siguienteSegmento;
} tablaSegmentos;

typedef struct {
	tablaSegmentos segmentos;
	tablaPaginas paginas;
	void *base;
	void *limite;
	int *seeds;
} memoria;

memoria* inicializarMemoria(int tamanio){
	memoria* nuevaMemoria;
	int cantidadPaginas = 1;


	nuevaMemoria->base = malloc(tamanio);
	nuevaMemoria->limite = nuevaMemoria->base + tamanio;
    nuevaMemoria->segmentos.unSegmento = malloc(1024);
    nuevaMemoria->paginas.paginas = malloc(sizeof(pagina)*1);

	return nuevaMemoria;

}
