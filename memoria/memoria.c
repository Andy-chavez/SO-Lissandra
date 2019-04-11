/*
 * memoria.c
 *
 *  Created on: 7 abr. 2019
 *      Author: whyAreYouRunning?
 */

#include <time.h>

#define TAMANIOSEGMENTO 10 // esto va a estar en un archivo de config
#define CANTIDADSEGMENTOS 5 // tambien esto

typedef enum {
	INSERT,
	CREATE,
	DESCRIBETABLE,
	DESCRIBEALL,
	DROP,
	JOURNAL,
	SELECT
} operacion;

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
	pagina paginas[TAMANIOSEGMENTO];
} tablaPaginas;

typedef struct {
	char *nombreTabla;
	tablaPaginas tabla;
} tablaSegmentos;

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
	} //test
}

int main() {
	/*char* mensaje;
	mensaje = leerMensaje();
*/
	return 0;
}


