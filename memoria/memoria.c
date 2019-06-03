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
		log_info(archivosDeConfigYLog->logger, "Recibi una operacionLQL");
		APIMemoria(deserializarOperacionLQL(buffer), socket);
		return 1;
	// TODO hacer un case donde se quiere cerrar el socket, cerrarConexion(socketKernel);
	// por ahora va a ser el default, ver como arreglarlo
	case DESCONEXION:
		log_info(archivosDeConfigYLog->logger, "Se cerro una conexion con el socket");
		cerrarConexion(socket);
		return 0;
	}
	free(buffer);
}

void APIMemoria(operacionLQL* operacionAParsear, int socketKernel) {
	if(string_starts_with(operacionAParsear->operacion, "INSERT")) {
		log_info(archivosDeConfigYLog->logger, "Recibi un INSERT");
		insertLQL(operacionAParsear, socketKernel);
	}
	else if (string_starts_with(operacionAParsear->operacion, "SELECT")) {
		log_info(archivosDeConfigYLog->logger, "Recibi un SELECT");
		selectLQL(operacionAParsear, socketKernel);
	}
	else if (string_starts_with(operacionAParsear->operacion, "DESCRIBE")) {
		log_info(archivosDeConfigYLog->logger, "Recibi un DESCRIBE");
	}
	else if (string_starts_with(operacionAParsear->operacion, "CREATE")) {
		log_info(archivosDeConfigYLog->logger, "Recibi un CREATE");
	}
	else if (string_starts_with(operacionAParsear->operacion, "DROP")) {
		log_info(archivosDeConfigYLog->logger, "Recibi un DROP");
	}
	else if (string_starts_with(operacionAParsear->operacion, "JOURNAL")) {
		log_info(archivosDeConfigYLog->logger, "Recibi un JOURNAL");
		}
	else {
		log_error(archivosDeConfigYLog->logger, "No pude entender la operacion");
	}
	liberarOperacionLQL(operacionAParsear);
}
//pthread_mutex_t mutexBufferLFS = 0; Podriamos usar semaphore.h que es mejor!

//------------------------------------------------------------------------

void liberarConfigYLogs() {
	log_destroy(archivosDeConfigYLog->logger);
	config_destroy(archivosDeConfigYLog->config);
	free(archivosDeConfigYLog);
}

void* trabajarConConexion(void* socket) {
	int socketKernel = *(int*) socket;
	sem_post(&binario_socket);
	int hayMensaje = 1;

	while(hayMensaje) {
		sem_wait(&mutex_operacion);
		void* bufferRecepcion = recibir(socketKernel);
		hayMensaje = APIProtocolo(bufferRecepcion, socketKernel);
		sem_post(&mutex_operacion);
	}

	pthread_exit(0);
}

datosInicializacion* realizarHandshake() {
	socketLissandraFS = crearSocketLFS();
	if(socketLissandraFS == -1) {
		log_error(archivosDeConfigYLog->logger,"No se pudo conectar con LFS");
		return NULL;
	}
	operacionProtocolo operacionHandshake = HANDSHAKE;
	serializarYEnviarHandshake(socketLissandraFS, 0);

	void* bufferHandshake = recibir(socketLissandraFS);

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
	char* fileSystemIP = config_get_string_value(archivosDeConfigYLog->config, "IPFILESYSTEM");
	char* fileSystemPuerto = config_get_string_value(archivosDeConfigYLog->config, "PUERTOFILESYSTEM");

	int socketClienteLFS = crearSocketCliente(fileSystemIP,fileSystemPuerto);
	return socketClienteLFS;
}

void *servidorMemoria(){
	int socketServidorMemoria = crearSocketServidor(config_get_string_value(archivosDeConfigYLog->config, "IPMEMORIA"), config_get_string_value(archivosDeConfigYLog->config, "PUERTO"));
	pthread_t threadID;
	int socketKernel;
	if(socketServidorMemoria == -1) {
		cerrarConexion(socketServidorMemoria);
		log_error(archivosDeConfigYLog->logger, "No se pudo inicializar el servidor de memoria");
		return NULL;
	}

	log_info(archivosDeConfigYLog->logger, "Servidor Memoria en linea");

	int valgrind = 1;
	while(valgrind){
		sem_wait(&binario_socket);
		socketKernel = aceptarCliente(socketServidorMemoria);
		if(socketKernel == -1) {
			log_error(archivosDeConfigYLog->logger, "ERROR: Socket Defectuoso");
			valgrind = 0;
			continue;
		}
		if(pthread_create(&threadID, NULL, trabajarConConexion, &socketKernel) < 0) {
			log_error(archivosDeConfigYLog->logger, "No se pudo crear un hilo para trabajar con el socket");
		}
		pthread_detach(threadID);
	}

	cerrarConexion(socketServidorMemoria);

}

void pedirALFS(operacionLQL* operacion){

	//pthread_mutex_lock(&mutexBufferLFS);
		// bufferConLFS = serializarOperacionLQL(operacion);
		//socketLFS(bufferConLFS);
	//pthread_mutex_unlock(&mutexBufferLFS);

}

void inicializarArchivosDeConfigYLog() {
	archivosDeConfigYLog = malloc(sizeof(configYLogs));
	archivosDeConfigYLog->logger = log_create("memoria.log", "MEMORIA", 1, LOG_LEVEL_INFO);
	archivosDeConfigYLog->config = config_create("memoria.config");
}

int main() {
	pthread_t threadServer; // threadTimedJournal, threadTimedGossiping;
	inicializarArchivosDeConfigYLog();
	sem_init(&mutex_operacion, 0, 1);
	sem_init(&binario_socket, 0, 1);

	datosInicializacion* datosDeInicializacion;
	if(!(datosDeInicializacion = realizarHandshake())) {
		liberarConfigYLogs();
		return -1;
	};

	memoriaPrincipal = inicializarMemoria(datosDeInicializacion, archivosDeConfigYLog);
	liberarDatosDeInicializacion(datosDeInicializacion);

	servidorMemoria();
	//pthread_create(&threadServer,NULL,servidorMemoria,(void*) archivosDeConfigYLog);
	//pthread_create(&threadTimedJournal, NULL, timedJournal, archivosDeConfigYLog);
	//pthread_create(&threadTimedGossiping, NULL, timedGossip, archivosDeConfigYLog);

	//pthread_join(threadServer, NULL);
	//pthread_detach(threadTimedJournal);
	//pthread_detach(threadTimedGossiping);

	liberarMemoria(memoriaPrincipal);
	liberarConfigYLogs();
	return 0;

}



