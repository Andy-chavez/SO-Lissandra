/*
 * memoria.c
 *
 *  Created on: 7 abr. 2019
 *      Author: whyAreYouRunning?
 */

#include <readline/readline.h>
#include <stdio.h>
#include <time.h>
#include <inttypes.h>
#include <stdlib.h>
#include <commons/string.h>
#include <commons/config.h>
#include <commons/log.h>
#include "conexiones.h"
#include "parser.h"
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

typedef struct {
	t_config* config;
	t_log* logger;
} configYLogs;

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



char* pruebaDeRecepcion(void* buffer) {
	int tamanioMensaje;
	char* mensaje;
	int desplazamiento = 0;
	memcpy(&tamanioMensaje, buffer, sizeof(int));
	desplazamiento += sizeof(int);

	mensaje = malloc(tamanioMensaje*sizeof(char));

	memcpy(mensaje, buffer+desplazamiento, tamanioMensaje*sizeof(char));
	return mensaje;
}

void* envioDePrueba(char* mensajeAEnviar) {

	int tamanioMensajeAEnviar = strlen(mensajeAEnviar) + 1;
	int desplazamiento = 0;
	void *bufferDePrueba = malloc(sizeof(int) + tamanioMensajeAEnviar*sizeof(char));

	memcpy(bufferDePrueba + desplazamiento, &tamanioMensajeAEnviar, sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(bufferDePrueba + desplazamiento, mensajeAEnviar, tamanioMensajeAEnviar*sizeof(char));
	return bufferDePrueba;
}



void liberarConfigYLogs(configYLogs *archivos) {
	log_destroy(archivos->logger);
	config_destroy(archivos->config);
	free(archivos);
}

void* servidorMemoria(void *arg){
	configYLogs *archivosDeConfigYLog = (configYLogs*) arg;
	//char* ipMemoria = "198.168.0.1";
	char* ipMemoria = "127.0.0.1";
	char* puertoMemoria;
	puertoMemoria = config_get_string_value(archivosDeConfigYLog->config, "PUERTO");
	int socketServidorMemoria = crearSocketServidor(ipMemoria,puertoMemoria);

	free(ipMemoria);
	free(puertoMemoria);

	if(socketServidorMemoria == -1) {
		cerrarConexion(socketServidorMemoria);
		pthread_exit(0);
	}

	while(1){
		int socketKernel = aceptarCliente(socketServidorMemoria);

		if(socketKernel == -1) {
			log_error(archivosDeConfigYLog->logger, "Socket Defectuoso");
			continue;
		}

		void* buffer = recibir(socketKernel);
		//void* buffer = envioDePrueba("hola");
		char* mensaje = pruebaDeRecepcion(buffer); // interface( deserializarOperacion( buffer , 1 ) )

		log_info(archivosDeConfigYLog->logger, "Recibi: %s", mensaje);

		free(mensaje);
		cerrarConexion(socketKernel);
		free(buffer);
	}

	cerrarConexion(socketServidorMemoria);

}

int main() {
	pthread_t threadServer; //threadCliente, threadTimedJournal, threadTimedGossiping;
	configYLogs *archivosDeConfigYLog = malloc(sizeof(configYLogs));

	archivosDeConfigYLog->config = config_create("memoria.config");
	archivosDeConfigYLog->logger = log_create("memoria.log", "MEMORIA", 1, LOG_LEVEL_ERROR);

	pthread_create(&threadServer,NULL,servidorMemoria,(void*) archivosDeConfigYLog);
	//pthread_create(&threadCliente, NULL, clienteKernel, archivosDeConfigYLog);
	//pthread_create(&threadTimedJournal, NULL, timedJournal, archivosDeConfigYLog);
	//pthread_create(&threadTimedGossiping, NULL, timedGossip, archivosDeConfigYLog);

	pthread_detach(threadServer);
	//pthread_detach(threadCliente);
	//pthread_detach(threadTimedJournal);
	//pthread_detach(threadTimedGossiping);

	liberarConfigYLogs(archivosDeConfigYLog);
	return 0;
}



