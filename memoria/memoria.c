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
#include <pthread.h>
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
		printf("JAJAJA");
		break;
	}
}

void* servidorMemoria(){
	char* IpMemoria = "192.168.1.2"; //numeros de prueba
	char* PuertoMemoria = "8001";
	int socketServidorMemoria = crearSocketServidor(IpMemoria,PuertoMemoria);

	while(1){
		int socketKernel = aceptarCliente(socketServidorMemoria);
		// interface( deserializarOperacion( recibir(socketKernel) , 1 ) )   Seguro que con los cambios de struct cambie,pero es para dar una idea. De donde saca el protocolo?

		cerrarConexion(socketKernel);
	}
}

int main() {
	pthread_t threadServer;
	pthread_create(&threadServer,NULL,servidorMemoria,NULL);
	pthread_detach(threadServer);
}



