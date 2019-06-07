/*
 * memoria.c
 *
 *  Created on: 7 abr. 2019
 *      Author: whyAreYouRunning?
 */

#include <readline/readline.h>
#include <string.h>
#include <stdio.h>
#include <conexiones.h>
#include <serializacion.h>
#include "operacionesMemoria.h"
#include "structsYVariablesGlobales.h"

int APIProtocolo(void* buffer, int socket) {
	operacionProtocolo operacion = empezarDeserializacion(&buffer);

	switch(operacion) {
	case OPERACIONLQL:
		enviarOMostrarYLogearInfo(-1, "Recibi una operacionLQL");
		APIMemoria(deserializarOperacionLQL(buffer), socket);
		return 1;
	// TODO hacer un case donde se quiere cerrar el socket, cerrarConexion(socketKernel);
	// por ahora va a ser el default, ver como arreglarlo
	case DESCONEXION:
		enviarOMostrarYLogearInfo(-1, "Se cerro una conexion con el socket");
		cerrarConexion(socket);
		return 0;
	}
	free(buffer);
}

void APIMemoria(operacionLQL* operacionAParsear, int socketKernel) {
	if(string_starts_with(operacionAParsear->operacion, "INSERT")) {
		enviarOMostrarYLogearInfo(-1, "Recibi un INSERT");
		if(esInsertOSelectEjecutable(operacionAParsear->parametros)) {
			insertLQL(operacionAParsear, socketKernel);
		}
		else {
			enviarYLogearMensajeError(socketKernel, "ERROR: La operacion no se pudo realizar porque no es un insert ejecutable.");
		}
	}
	else if (string_starts_with(operacionAParsear->operacion, "SELECT")) {
		enviarOMostrarYLogearInfo(-1, "Recibi un SELECT");
		if(esInsertOSelectEjecutable(operacionAParsear->parametros)) selectLQL(operacionAParsear, socketKernel);
		else enviarYLogearMensajeError(socketKernel, "ERROR: La operacion no se pudo realizar porque no es un insert ejecutable.");
	}
	else if (string_starts_with(operacionAParsear->operacion, "DESCRIBE")) {
		enviarOMostrarYLogearInfo(-1, "Recibi un DESCRIBE");
		// describeLQL(operacionAParsear, socketKernel);
	}
	else if (string_starts_with(operacionAParsear->operacion, "CREATE")) {
		enviarOMostrarYLogearInfo(-1, "Recibi un CREATE");
		// createLQL(operacionAParsear, socketKernel);
	}
	else if (string_starts_with(operacionAParsear->operacion, "DROP")) {
		enviarOMostrarYLogearInfo(-1, "Recibi un DROP");
	}
	else if (string_starts_with(operacionAParsear->operacion, "JOURNAL")) {
		enviarOMostrarYLogearInfo(-1, "Recibi un JOURNAL");
		}
	else {
		enviarYLogearMensajeError(socketKernel, "No pude entender la operacion");
	}
	liberarOperacionLQL(operacionAParsear);
	sleep((config_get_int_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "RETARDO_MEM") + 500) / 1000); // pasaje a segundos del retardo
}

//------------------------------------------------------------------------

void* trabajarConConexion(void* socket) {
	int socketKernel = *(int*) socket;
	sem_post(&BINARIO_SOCKET_KERNEL);
	recibir(socketKernel);
	enviar(socketKernel, (void*) config_get_int_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "MEMORY_NUMBER"), sizeof(int));
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
		enviarYLogearMensajeError(-1, "No se pudo conectar con LFS");
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
	char* fileSystemPuerto = config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "PUERTO_FS");

	int socketClienteLFS = crearSocketCliente(fileSystemIP,fileSystemPuerto);
	return socketClienteLFS;
}

void* manejarConsola() {
	enviarOMostrarYLogearInfo(-1, "Memoria lista para ser utilizada.");
	while(1) {
		char* comando = NULL;
		enviarOMostrarYLogearInfo(-1, "Por favor, ingrese un comando LQL:");
		comando = readline(">");
		sem_wait(&MUTEX_OPERACION); // Region critica GIGANTE, ver donde es donde se necesita este mutex.
		APIMemoria(splitear_operacion(comando), -1);
		sem_post(&MUTEX_OPERACION);
		free(comando);
	}
}

void *servidorMemoria() {
	int socketServidorMemoria = crearSocketServidor(config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "IP_MEMORIA"), config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "PUERTO"));
	pthread_t threadConexion;
	int socketKernel;
	if(socketServidorMemoria == -1) {
		cerrarConexion(socketServidorMemoria);

		enviarYLogearMensajeError(-1, "No se pudo inicializar el servidor de memoria");
		pthread_exit(0);
	}

	enviarOMostrarYLogearInfo(-1, "Servidor Memoria en linea");
	while(1){

		sem_wait(&BINARIO_SOCKET_KERNEL);
		socketKernel = aceptarCliente(socketServidorMemoria);
		if(socketKernel == -1) {
			enviarYLogearMensajeError(-1, "ERROR: Socket Defectuoso");
			continue;
		}
		if(pthread_create(&threadConexion, NULL, trabajarConConexion, &socketKernel) < 0) {
			enviarYLogearMensajeError(socketKernel, "No se pudo crear un hilo para trabajar con el socket");
		}
		pthread_detach(threadConexion);
	}

	cerrarConexion(socketServidorMemoria);
	pthread_exit(0);

}

int main() {
	pthread_t threadServer, threadConsola; // threadTimedJournal, threadTimedGossiping;
	inicializarArchivos();
	inicializarSemaforos();

	datosInicializacion* datosDeInicializacion;
	if(!(datosDeInicializacion = realizarHandshake())) {
		liberarConfigYLogs();
		return -1;
	};

	MEMORIA_PRINCIPAL = inicializarMemoria(datosDeInicializacion, ARCHIVOS_DE_CONFIG_Y_LOG);
	liberarDatosDeInicializacion(datosDeInicializacion);

	pthread_create(&threadServer, NULL, servidorMemoria, NULL);
	pthread_create(&threadConsola, NULL, manejarConsola, NULL);
	//pthread_create(&threadTimedJournal, NULL, timedJournal, ARCHIVOS_DE_CONFIG_Y_LOG);
	//pthread_create(&threadTimedGossiping, NULL, timedGossip, ARCHIVOS_DE_CONFIG_Y_LOG);
	pthread_join(threadServer, NULL);
	pthread_join(threadConsola, NULL);
	//pthread_detach(threadTimedJournal);
	//pthread_detach(threadTimedGossiping);

	liberarMemoria(MEMORIA_PRINCIPAL);
	liberarConfigYLogs();
	return 0;

}



