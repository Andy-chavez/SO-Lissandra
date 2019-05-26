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

void APIMemoria(char* operacionAParsear, configYLogs* configYLog) {
	if(string_starts_with(operacionAParsear, "INSERT")) {
		log_info(configYLog->logger, "Recibi un INSERT");
	}
	else if (string_starts_with(operacionAParsear, "SELECT")) {
		log_info(configYLog->logger, "Recibi un SELECT");
	}
	else if (string_starts_with(operacionAParsear, "DESCRIBE")) {
		log_info(configYLog->logger, "Recibi un DESCRIBE");
	}
	else if (string_starts_with(operacionAParsear, "CREATE")) {
		log_info(configYLog->logger, "Recibi un CREATE");
	}
	else if (string_starts_with(operacionAParsear, "DROP")) {
		log_info(configYLog->logger, "Recibi un DROP");
	}
	else if (string_starts_with(operacionAParsear, "JOURNAL")) {
		log_info(configYLog->logger, "Recibi un JOURNAL");
		}
	else {
		log_error(configYLog->logger, "No pude entender la operacion");
	}
}

int maximoValue;
void* bufferConLFS;
pthread_mutex_t mutexBufferLFS = 0;

//------------------------------------------------------------------------

char* pruebaDeRecepcion(void* buffer) {
	return (char*) buffer;
}

void envioDePrueba(int socketServidor, char* mensajeAEnviar) {

	int tamanioMensajeAEnviar = strlen(mensajeAEnviar) + 1;
	enviar(socketServidor, (void*) mensajeAEnviar, tamanioMensajeAEnviar);
}


void liberarConfigYLogs(configYLogs *archivos) {
	config_destroy(archivos->config);
	log_destroy(archivos->logger);
	free(archivos);
}

datosInicializacion* realizarHandshake(configYLogs* configYLog) {
	datosInicializacion* datosMock = malloc(sizeof(datosInicializacion));
	datosMock->tamanio = 2048;
	return datosMock;
}

void liberarDatosDeInicializacion();

void *clienteLFS(void* arg) {
	configYLogs *archivosDeConfigYLog = (configYLogs*) arg;

	char* fileSystemIP = config_get_string_value(archivosDeConfigYLog->config, "IPFILESYSTEM");
	char* fileSystemPuerto = config_get_string_value(archivosDeConfigYLog->config, "PUERTOFILESYSTEM");

	int socketClienteLFS = crearSocketCliente(fileSystemIP,fileSystemPuerto);

	char* mensaje = (char*) arg;
	enviar(socketClienteLFS, mensaje, (strlen(mensaje)+1)*sizeof(char));

	cerrarConexion(socketClienteLFS);
}

void *servidorMemoria(void* arg){
	configYLogs *archivosDeConfigYLog = (configYLogs*) arg;

	int socketServidorMemoria = crearSocketServidor(config_get_string_value(archivosDeConfigYLog->config, "PUERTO"));

	if(socketServidorMemoria == -1) {
		cerrarConexion(socketServidorMemoria);
		log_error(archivosDeConfigYLog->logger, "No se pudo inicializar el servidor de memoria");
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
		void* mensaje = recibir(socketKernel); // interface( deserializarOperacion( buffer , 1 ) )



		log_info(archivosDeConfigYLog->logger, "Recibi: %s", mensaje);

		free(mensaje);
		cerrarConexion(socketKernel);
	}

	cerrarConexion(socketServidorMemoria);

}

void handshake(void *arg){
	configYLogs *archivosDeConfigYLog = (configYLogs*) arg;

	char* mensajeALFS = "Hola Lissandra, soy Memoria, un placer.";
	clienteLFS((void*) mensajeALFS);

	int socketServidorMemoria = crearSocketServidor(config_get_string_value(archivosDeConfigYLog->config, "PUERTOFILESYSTEM"));

	log_info(archivosDeConfigYLog->logger, "Esperando recibir Value de LFS.");

	int socketLFS = aceptarCliente(socketServidorMemoria);

	bufferConLFS = recibir(socketLFS);
	// maximoValue = deserializarMaxValue(*bufferConLFS);
	log_info(archivosDeConfigYLog->logger, "Establecido el tamanio maximo value de Pagina como: %d", maximoValue);

	pthread_mutex_lock(&mutexBufferLFS);
	free(mensajeALFS);
}

void pedirALFS(operacionLQL* operacion){

	pthread_mutex_lock(&mutexBufferLFS);
		bufferConLFS = serializarOperacionLQL(operacion);
		clienteLFS(bufferConLFS);
	pthread_mutex_unlock(&mutexBufferLFS);

}

int main() {
	pthread_t threadServer; //threadCliente, threadTimedJournal, threadTimedGossiping;
	configYLogs *archivosDeConfigYLog = malloc(sizeof(configYLogs));

	archivosDeConfigYLog->config = config_create("memoria.config");
	archivosDeConfigYLog->logger = log_create("memoria.log", "MEMORIA", 1, LOG_LEVEL_INFO);

	datosInicializacion* datosDeInicializacion = realizarHandshake(archivosDeConfigYLog);

	memoria* memoriaPrincipal = inicializarMemoria(datosDeInicializacion, archivosDeConfigYLog);
	liberarDatosDeInicializacion(datosDeInicializacion);

	pthread_create(&threadServer,NULL,servidorMemoria,(void*) archivosDeConfigYLog);
	//pthread_create(&threadCliente, NULL, clienteKernel, archivosDeConfigYLog);
	//pthread_create(&threadTimedJournal, NULL, timedJournal, archivosDeConfigYLog);
	//pthread_create(&threadTimedGossiping, NULL, timedGossip, archivosDeConfigYLog);

	//pthread_join(threadServer, NULL);
	//pthread_detach(threadCliente);
	//pthread_detach(threadTimedJournal);
	//pthread_detach(threadTimedGossiping);

	liberarMemoria(memoriaPrincipal);
	liberarConfigYLogs(archivosDeConfigYLog);
	return 0;
}



