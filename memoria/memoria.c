/*
 * memoria.c
 *
 *  Created on: 7 abr. 2019
 *      Author: whyAreYouRunning?
 */

#include <readline/readline.h>
#include <string.h>
#include <stdio.h>
#include <commonsPropias/conexiones.h>
#include <commonsPropias/serializacion.h>
#include "operacionesMemoria.h"
#include "structsYVariablesGlobales.h"

int APIProtocolo(void* buffer, int socket) {
	operacionProtocolo operacion = empezarDeserializacion(&buffer);

	switch(operacion) {
	case OPERACIONLQL:
		log_info(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "Recibi una operacionLQL");
		APIMemoria(deserializarOperacionLQL(buffer), socket);
		return 1;
	// TODO hacer un case donde se quiere cerrar el socket, cerrarConexion(socketKernel);
	// por ahora va a ser el default, ver como arreglarlo
	case DESCONEXION:
		log_info(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "Se cerro una conexion con el socket");
		cerrarConexion(socket);
		return 0;
	}
	free(buffer);
}

void APIMemoria(operacionLQL* operacionAParsear, int socketKernel) {
	if(string_starts_with(operacionAParsear->operacion, "INSERT")) {
		log_info(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "Recibi un INSERT");
		insertLQL(operacionAParsear, socketKernel);
	}
	else if (string_starts_with(operacionAParsear->operacion, "SELECT")) {
		log_info(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "Recibi un SELECT");
		selectLQL(operacionAParsear, socketKernel);
	}
	else if (string_starts_with(operacionAParsear->operacion, "DESCRIBE")) {
		log_info(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "Recibi un DESCRIBE");
	}
	else if (string_starts_with(operacionAParsear->operacion, "CREATE")) {
		log_info(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "Recibi un CREATE");
		//createLQL();
	}
	else if (string_starts_with(operacionAParsear->operacion, "DROP")) {
		log_info(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "Recibi un DROP");
	}
	else if (string_starts_with(operacionAParsear->operacion, "JOURNAL")) {
		log_info(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "Recibi un JOURNAL");
		}
	else {
		log_error(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "No pude entender la operacion");
	}
	liberarOperacionLQL(operacionAParsear);
}

//------------------------------------------------------------------------

void liberarConfigYLogs() {
	log_destroy(ARCHIVOS_DE_CONFIG_Y_LOG->logger);
	config_destroy(ARCHIVOS_DE_CONFIG_Y_LOG->config);
	free(ARCHIVOS_DE_CONFIG_Y_LOG);
}

void* trabajarConConexion(void* socket) {
	int socketKernel = *(int*) socket;
	sem_post(&BINARIO_SOCKET_KERNEL);
	int hayMensaje = 1;

	while(hayMensaje) {
		void* bufferRecepcion = recibir(socketKernel);
		sem_wait(&MUTEX_OPERACION); // Region critica GIGANTE, ver donde es donde se necesita este mutex.
		hayMensaje = APIProtocolo(bufferRecepcion, socketKernel);
		sem_post(&MUTEX_OPERACION);
	}

	pthread_exit(0);
}

datosInicializacion* realizarHandshake() {
	SOCKET_LFS = crearSocketLFS();
	if(SOCKET_LFS == -1) {
		log_error(ARCHIVOS_DE_CONFIG_Y_LOG->logger,"No se pudo conectar con LFS");
		return NULL;
	}
	operacionProtocolo operacionHandshake = HANDSHAKE;
	serializarYEnviarHandshake(SOCKET_LFS, 0);

	void* bufferHandshake = recibir(SOCKET_LFS);

	datosInicializacion* datosImportantes = malloc(sizeof(datosInicializacion));
	datosImportantes->tamanio = deserializarHandshake(bufferHandshake);
	return datosImportantes;
	/*datosInicializacion* datosImportantes = malloc(sizeof(datosInicializacion));
	datosImportantes->tamanio = 2048;
	return datosImportantes;*/
}

void liberarDatosDeInicializacion(datosInicializacion* datos) {
	free(datos);
}

int crearSocketLFS() {
	char* fileSystemIP = config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "IPFILESYSTEM");
	char* fileSystemPuerto = config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "PUERTOFILESYSTEM");

	int socketClienteLFS = crearSocketCliente(fileSystemIP,fileSystemPuerto);
	return socketClienteLFS;
}

void *servidorMemoria(){
	int socketServidorMemoria = crearSocketServidor(config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "IPMEMORIA"), config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "PUERTO"));
	pthread_t threadID;
	int socketKernel;
	if(socketServidorMemoria == -1) {
		cerrarConexion(socketServidorMemoria);
		log_error(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "No se pudo inicializar el servidor de memoria");
		return NULL;
	}

	log_info(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "Servidor Memoria en linea");

	int valgrind = 1;
	while(valgrind){
		sem_wait(&BINARIO_SOCKET_KERNEL);
		socketKernel = aceptarCliente(socketServidorMemoria);
		if(socketKernel == -1) {
			log_error(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "ERROR: Socket Defectuoso");
			valgrind = 0;
			continue;
		}
		if(pthread_create(&threadID, NULL, trabajarConConexion, &socketKernel) < 0) {
			log_error(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "No se pudo crear un hilo para trabajar con el socket");
		}
		pthread_detach(threadID);
	}

	cerrarConexion(socketServidorMemoria);

}

void inicializarArchivos() {
	ARCHIVOS_DE_CONFIG_Y_LOG = malloc(sizeof(configYLogs));
	ARCHIVOS_DE_CONFIG_Y_LOG->logger = log_create("memoria.log", "MEMORIA", 1, LOG_LEVEL_INFO);
	ARCHIVOS_DE_CONFIG_Y_LOG->config = config_create("../memoria.config");
}

void inicializarSemaforos() {
	sem_init(&MUTEX_OPERACION, 0, 1);
	sem_init(&BINARIO_SOCKET_KERNEL, 0, 1);
	sem_init(&MUTEX_LOG, 0, 1);
	sem_init(&MUTEX_SOCKET_LFS, 0, 1);
}

int main() {
	pthread_t threadServer; // threadTimedJournal, threadTimedGossiping;
	inicializarArchivos();
	inicializarSemaforos();

	datosInicializacion* datosDeInicializacion;
	if(!(datosDeInicializacion = realizarHandshake())) {
		liberarConfigYLogs();
		return -1;
	};

	MEMORIA_PRINCIPAL = inicializarMemoria(datosDeInicializacion, ARCHIVOS_DE_CONFIG_Y_LOG);
	liberarDatosDeInicializacion(datosDeInicializacion);

	servidorMemoria();
	//pthread_create(&threadServer,NULL,servidorMemoria,(void*) ARCHIVOS_DE_CONFIG_Y_LOG);
	//pthread_create(&threadTimedJournal, NULL, timedJournal, ARCHIVOS_DE_CONFIG_Y_LOG);
	//pthread_create(&threadTimedGossiping, NULL, timedGossip, ARCHIVOS_DE_CONFIG_Y_LOG);

	//pthread_join(threadServer, NULL);
	//pthread_detach(threadTimedJournal);
	//pthread_detach(threadTimedGossiping);

	liberarMemoria(MEMORIA_PRINCIPAL);
	liberarConfigYLogs();
	return 0;

}



