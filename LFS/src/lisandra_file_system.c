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



void agregarRegistro(tabla unaTabla,registro unRegistro){

}

void* pruebaServidor(){
	 t_config *CONFIG_LISANDRA;
	 CONFIG_LISANDRA = config_create("ejemploConfig");//A modificar esto dependiendo del config que se quiera usar
	 char* IpLisandra;
	 IpLisandra= config_get_string_value(CONFIG_LISANDRA ,"IP_LISANDRA");
	 char* PuertoLisandra;
	 PuertoLisandra= config_get_string_value(CONFIG_LISANDRA ,"PUERTO_ESCUCHA");
	 int socketServidor = crearSocketServidor(IpLisandra,PuertoLisandra);
	 while(1){
		 int socketCliente = aceptarCliente(socketServidor);
		 cerrarConexion(socketCliente);
	 }
	 //config_destroy(CONFIG_LISANDRA);

}

void iniciar_logger(void)
{
	t_log *loggerLisandra;
	loggerLisandra = log_create("tp0.log","tp0.c",1,LOG_LEVEL_INFO);
	log_info(loggerLisandra,"el mensaje");
}

int main(int argc, char* argv[]) {

	iniciar_logger();
	pthread_t threadServer;
	pthread_create(&threadServer, NULL,pruebaServidor, NULL);
	pthread_detach(threadServer);
	//destruirLogYConfig();
	return EXIT_SUCCESS;
}

