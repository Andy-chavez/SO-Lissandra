/*
 * memoria.c
 *
 *  Created on: 7 abr. 2019
 *      Author: whyAreYouRunning?
 */

#include <commons/string.h>
#include <time.h>
#include "conexiones.h"
#include <inttypes.h>
#include "parser.h"
#include <readline/readline.h>
#include <stdio.h>
#define TAMANIOSEGMENTO 10 // esto va a estar en un archivo de config

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

typedef struct {
	segmento *segmentosEnMemoria;
} tablaSegmentos;

typedef struct {
	tablaSegmentos segmentos;
	tablaPaginas paginas;
	void *base;
	void *limite;
	int *seeds;
} memoria;

void interface(operacion unaOperacion) {
	switch(unaOperacion){
	case INSERT:
		break;
	case CREATE:
		break;
	case DESCRIBETABLE:
		break;
	case DESCRIBEALL:
		break;
	case DROP:
		break;
	case SELECT:
		break;
	case JOURNAL:
		break;
	default:
		break;
	}
}

int main() {

}


