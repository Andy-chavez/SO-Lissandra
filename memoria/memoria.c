/*
 * memoria.c
 *
 *  Created on: 7 abr. 2019
 *      Author: whyAreYouRunning?
 */

#include <readline/readline.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <inttypes.h>
#include <stdlib.h>
#include <commons/string.h>
#include <commons/config.h>
#include <commons/log.h>
#include <commonsPropias/conexiones.h>
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
	return (char*) buffer;
}

void envioDePrueba(int socketServidor, char* mensajeAEnviar) {

	int tamanioMensajeAEnviar = strlen(mensajeAEnviar) + 1;
	enviar(socketServidor, (void*) mensajeAEnviar, tamanioMensajeAEnviar);
}



void liberarConfigYLogs(configYLogs *archivos) {
	log_destroy(archivos->logger);
	config_destroy(archivos->config);
	free(archivos);
}

void *clienteLFS(void* arg) {
	int socketClienteKernel = crearSocketCliente("192.168.0.34","5008");
	char* mensaje = (char*) arg;
	enviar(socketClienteKernel, mensaje, (strlen(mensaje)+1)*sizeof(char));
	cerrarConexion(socketClienteKernel);
}

void *servidorMemoria(void* arg){
	configYLogs *archivosDeConfigYLog = (configYLogs*) arg;


	//configYLogs *archivosDeConfigYLog = (configYLogs*) arg;

	int socketServidorMemoria = crearSocketServidor("8008");

	if(socketServidorMemoria == -1) {
		cerrarConexion(socketServidorMemoria);
		pthread_exit(0);
	}

	log_info(archivosDeConfigYLog->logger, "Servidor Memoria en linea");

	while(1){
		int socketKernel = aceptarCliente(socketServidorMemoria);

		if(socketKernel == -1) {
			log_info(archivosDeConfigYLog->logger, "ERROR: Socket Defectuoso");
			continue;
		}


		//void* buffer = envioDePrueba("hola");
		char* mensaje = (char*) recibir(socketKernel); // interface( deserializarOperacion( buffer , 1 ) )

		clienteLFS((void*) mensaje);


		log_info(archivosDeConfigYLog->logger, "Recibi: %s", mensaje);

		free(mensaje);
		cerrarConexion(socketKernel);
	}

	cerrarConexion(socketServidorMemoria);

}

int main() {
	pthread_t threadServer; //threadCliente, threadTimedJournal, threadTimedGossiping;
	configYLogs *archivosDeConfigYLog = malloc(sizeof(configYLogs));

	archivosDeConfigYLog->config = config_create("memoria.config");

	archivosDeConfigYLog->logger = log_create("memoria.log", "MEMORIA", 1, LOG_LEVEL_INFO);

	pthread_create(&threadServer,NULL,servidorMemoria,(void*) archivosDeConfigYLog);
	//pthread_create(&threadCliente, NULL, clienteKernel, archivosDeConfigYLog);
	//pthread_create(&threadTimedJournal, NULL, timedJournal, archivosDeConfigYLog);
	//pthread_create(&threadTimedGossiping, NULL, timedGossip, archivosDeConfigYLog);

	pthread_join(threadServer, NULL);
	//pthread_detach(threadCliente);
	//pthread_detach(threadTimedJournal);
	//pthread_detach(threadTimedGossiping);

	//liberarConfigYLogs(archivosDeConfigYLog); Esta dando segfault
	return 0;
}



