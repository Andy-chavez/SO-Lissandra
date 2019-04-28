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
#include "parser.h"

#define CANTPARTICIONES 5 // esto esta en el metadata

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

/* SELECT: FACU , INSERT: PABLO
 * verificarExistencia(char* nombreTabla); //select e insert. FACU
 * metadata obtenerMetadata(char* nombreTabla); //select e insert. PABLO
 * int calcularParticion(int cantidadParticiones, int key); //select e insert. key hay que pasarlo a int. FACU
 * int leerRegistro(int particion, char* nombreTabla); //te devuelve el key. FACU
 * void guardarRegistro(registro unRegistro, int particion, char* nombreTabla); //te guarda el registro en la memtable. PABLO
 * registro devolverRegistroDeLaMemtable(int key); //select e insert. PABLO
 * registro devolverRegistroDelFileSystem(int key); //select e insert FACU
 * Fijarse que te devuelva el timestamp con epoch unix
 * No olvidar de hacer la comparacion final
*/


//void agregarRegistro(tabla unaTabla,registro unRegistro){
//
//}

metadata obtenerMetadata(char* nombreTabla){
	t_config* configMetadata;
	int cantParticiones;
	int tiempoCompactacion;
	consistencia tipoConsistencia;
	metadata unaMetadata;
	char* str1 = "../src/Directorio/";
	char* str2 = "/metadata";

	char* ruta = (char *) malloc(1 + strlen(str1) + strlen(str2) + strlen(nombreTabla));
	strcpy(ruta, str1);
	strcat(ruta, nombreTabla);
	strcat(ruta, str2);

	configMetadata = config_create(ruta);

	cantParticiones = atoi(config_get_string_value(configMetadata, "PARTITIONS"));
	//como hago con esto te lo tome siendo que es un enum? con esto toma siempre 0
	tipoConsistencia = atoi(config_get_string_value(configMetadata, "CONSISTENCY"));
	tiempoCompactacion = atoi(config_get_string_value(configMetadata, "COMPACTACION_TIME"));

	unaMetadata.cantParticiones = cantParticiones;
	unaMetadata.tipoConsistencia = tipoConsistencia;
	unaMetadata.tiempoCompactacion = tiempoCompactacion;

	return unaMetadata;


}


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

void* leerConsola() {

		char *linea = NULL;  // forces getline to allocate with malloc
	    size_t len = 0;     // ignored when line = NULL
	    ssize_t leerConsola;

	    printf ("Ingresa operacion\n");

	    while ((leerConsola = getline(&linea, &len, stdin)) != -1){  //hay que hacer CTRL + D para salir del while
	    parserGeneral(linea);
	    }

	    free (linea);  // free memory allocated by getline
}


int main(int argc, char* argv[]) {

	obtenerMetadata("tablaA");
	leerConsola();


	pthread_t threadLeerConsola;
    pthread_create(&threadLeerConsola, NULL, leerConsola, NULL);
    pthread_join(threadLeerConsola,NULL);


//	iniciar_logger();

	/*
	pthread_t threadServer;
//	pthread_detach(threadServer);
	//pthread_t threadServer; //threadCliente, threadTimedJournal, threadTimedGossiping;


	configYLogs *archivosDeConfigYLog = malloc(sizeof(configYLogs));

	archivosDeConfigYLog->config = config_create("LISANDRA.CONFIG");
	archivosDeConfigYLog->logger = log_create("lisandra.log", "LISANDRA", 1, LOG_LEVEL_ERROR);

	pthread_create(&threadServer, NULL, servidorLisandra, (void*) archivosDeConfigYLog);

	pthread_join(threadServer,NULL);
	//servidorLisandra(archivosDeConfigYLog);

	liberarConfigYLogs(archivosDeConfigYLog);

	*/
	return EXIT_SUCCESS;
}

