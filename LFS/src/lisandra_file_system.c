/*
 ============================================================================
 Name        : lisandra_file_system.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description :
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h> //malloc,alloc,realloc
#include <string.h>
#include<readline/readline.h>
#include <time.h>
#include <commons/string.h>
#include <pthread.h>
#include <commons/config.h>
#include <commons/log.h>
#include "conexiones.h"


#define CANTPARTICIONES 5 // esto esta en el metadata

typedef enum{
	SELECT, //Select
	INSERT, //Insert
	CREATE, //Create
	DESCRIBEALL, //Describe All
	DESCRIBETABLE, //Describe Table
	DROP //Drop
}casos;

typedef enum {
	SC,
	SH,
	EC
}consistencia;

typedef struct {
	time_t timestamp;
	u_int16_t key;
	char* value;  //no seria siempre un char*?
	struct registro *sigRegistro;
} registroLisandra;

typedef struct {
	time_t timestamp;
	u_int16_t key;
	char* value;  //no seria siempre un char*?
} registro;

typedef struct {
	t_config* config;
	t_log* logger;
} configYLogs;

typedef struct {
	int numeroBloque;
	int sizeDeBloque;

} bloque;

typedef struct {
	int size;
	int numeroParticion; // para saber que keys estan ahi,por el modulo
	registroLisandra *registros;
	bloque block[/*CANTIDADBLOQUES*/];
} particion;

typedef struct {
	consistencia tipoConsistencia;
	int cantParticiones;
	int tiempoCompactacion;
} metadata;

typedef struct {
	char* nombre;
	particion particiones[CANTPARTICIONES]; //HAY QUE VER COMO HACER QUE DE CADA PARTICION SALGAN SUS REGISTROS.
	consistencia tipoDeConsistencia;
	metadata *metadataAsociada;
} tabla;

void api(casos caso){
	//leerConsola();
	switch (caso){
		case SELECT:
			//Select
			break;
		case INSERT:
			//Insert
			break;
		case CREATE:
			//Create
			break;
		case DESCRIBEALL:
			//Describe todas las tablas(All)
			break;
		case DESCRIBETABLE:
			//Describe una tabla
			break;
		case DROP:
			//Drop table
			break;
		default:
			printf("Error del header");
			//agregar al archivo de log
	}
}



//void agregarRegistro(tabla unaTabla,registro unRegistro){
//
//}

void* servidorLisandra(void *arg){
	configYLogs *archivosDeConfigYLog = (configYLogs*) arg;
	char* puertoLisandra = "5008";
	//puertoLisandra = config_get_string_value(archivosDeConfigYLog->config, "PUERTO_ESCUCHA");
	//char* ipLisandra = config_get_string_value(archivosDeConfigYLog->config, "IP_LISANDRA");
	int socketServidorLisandra = crearSocketServidor(puertoLisandra);

//	free(ipMemoria);
//	free(puertoMemoria);
	if(socketServidorLisandra == -1) {
		cerrarConexion(socketServidorLisandra);
		pthread_exit(0);
	}

	while(1){
		int socketMemoria = aceptarCliente(socketServidorLisandra);

		if(socketMemoria == -1) {
			log_error(archivosDeConfigYLog->logger, "Socket Defectuoso");
			continue;
		}

		char* buffer = (char*)recibir(socketMemoria);

		//char* mensaje = "hola";

		//int tamanio = strlen(mensaje) + 1;

		//enviar(socketMemoria,(void*) mensaje,tamanio);
		//char* mensaje = pruebaDeRecepcion(buffer); // interface( deserializarOperacion( buffer , 1 ) )

		log_info(archivosDeConfigYLog->logger, "Recibi: %s", buffer);

		free(buffer);
		cerrarConexion(socketMemoria);
	}

	cerrarConexion(socketServidorLisandra);

}

void liberarConfigYLogs(configYLogs *archivos) {
	log_destroy(archivos->logger);
	config_destroy(archivos->config);
	free(archivos);
}


int main(int argc, char* argv[]) {

//	iniciar_logger();
	pthread_t threadServer;
//	pthread_detach(threadServer);
	//pthread_t threadServer; //threadCliente, threadTimedJournal, threadTimedGossiping;
	configYLogs *archivosDeConfigYLog = malloc(sizeof(configYLogs));

	archivosDeConfigYLog->config = config_create("LISANDRA.CONFIG");
	archivosDeConfigYLog->logger = log_create("lisandra.log", "LISANDRA", 1, LOG_LEVEL_ERROR);

	pthread_create(&threadServer, NULL, servidorLisandra, (void*) archivosDeConfigYLog);

	pthread_join(threadServer);
	//servidorLisandra(archivosDeConfigYLog);

	liberarConfigYLogs(archivosDeConfigYLog);
	return EXIT_SUCCESS;
}

