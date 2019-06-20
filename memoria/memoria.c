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
#include <sys/types.h>
#include <sys/inotify.h>
#include <signal.h>

// Defines para el inotify del config
#define EVENT_SIZE_CONFIG (sizeof(struct inotify_event) + 15)
#define BUF_LEN_CONFIG (1024 * EVENT_SIZE_CONFIG) // Lo dejo asi de grande porque sino segfaultea

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
	case TABLAGOSSIP:
		enviarOMostrarYLogearInfo(-1, "llego una memoria. guardando en tabla de gossip y enviando lo que tengo...");
		intercambiarTablasGossip(socket);
		return 1;
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
		describeLQL(operacionAParsear, socketKernel);
	}
	else if (string_starts_with(operacionAParsear->operacion, "CREATE")) {
		enviarOMostrarYLogearInfo(-1, "Recibi un CREATE");
		createLQL(operacionAParsear, socketKernel);
	}
	else if (string_starts_with(operacionAParsear->operacion, "DROP")) {
		enviarOMostrarYLogearInfo(-1, "Recibi un DROP");
	}
	else if (string_starts_with(operacionAParsear->operacion, "JOURNAL")) {
		enviarOMostrarYLogearInfo(-1, "Recibi un JOURNAL");
		}
	else if(string_starts_with(operacionAParsear->operacion, "HEXDUMP")) {
		size_t length = config_get_int_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "TAM_MEM");
		mem_hexdump(MEMORIA_PRINCIPAL->base, length);
	}
	else {
		enviarYLogearMensajeError(socketKernel, "No pude entender la operacion");
	}
	liberarOperacionLQL(operacionAParsear);

	sem_wait(&MUTEX_RETARDO_MEMORIA);
	int retardoMemoria = RETARDO_MEMORIA * 1000;
	sem_post(&MUTEX_RETARDO_MEMORIA);

	usleep(retardoMemoria);
}

//------------------------------------------------------------------------

void* trabajarConConexion(void* socket) {
	int socketKernel = *(int*) socket;
	sem_post(&BINARIO_SOCKET_KERNEL);
	/*
	recibir(socketKernel);
	int numeroMemoria = config_get_int_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "MEMORY_NUMBER");
	enviar(socketKernel, (void*) &numeroMemoria, sizeof(int));
	*/
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

void* cambiosConfig() {
	char buffer[BUF_LEN_CONFIG];
	int fdConfig = inotify_init();
	char* path = "memoria.config";

	if(fdConfig < 0) {
		enviarOMostrarYLogearInfo(-1, "hubo un error con el inotify_init");
	}

	int watchDescriptorConfig = inotify_add_watch(fdConfig, path, IN_MODIFY);

	while(1) {
		int size = read(fdConfig, buffer, BUF_LEN_CONFIG);

		if(size<0) {
			enviarOMostrarYLogearInfo(-1, "hubo un error al leer modificaciones del config");
		}

		t_config* configConNuevosDatos = config_create(path);

		if(!configConNuevosDatos) {
			enviarOMostrarYLogearInfo(-1, "hubo un error al abrir el archivo de config");
		}

		int desplazamiento = 0;

		while(desplazamiento < size) {
			struct inotify_event *event = (struct inotify_event *) &buffer[desplazamiento];

			if (event->mask & IN_MODIFY) {
				enviarOMostrarYLogearInfo(-1, "hubieron cambios en el archivo de config. Analizando y realizando cambios a retardos...");

				sem_wait(&MUTEX_RETARDO_GOSSIP);
				RETARDO_GOSSIP = config_get_int_value(configConNuevosDatos, "RETARDO_GOSSIPING");
				sem_post(&MUTEX_RETARDO_GOSSIP);
				sem_wait(&MUTEX_RETARDO_JOURNAL);
				RETARDO_JOURNAL = config_get_int_value(configConNuevosDatos, "RETARDO_JOURNAL");
				sem_post(&MUTEX_RETARDO_JOURNAL);
				sem_wait(&MUTEX_RETARDO_MEMORIA);
				RETARDO_MEMORIA = config_get_int_value(configConNuevosDatos, "RETARDO_MEM");
				sem_post(&MUTEX_RETARDO_MEMORIA);
			}

			config_destroy(configConNuevosDatos);
			desplazamiento += sizeof (struct inotify_event) + event->len;
		}
	}
}

void cambiarValor() {
	DEBO_TERMINAR = 1;
}

int main() {
	printf("%d %d\n", sizeof(time_t), sizeof(uint16_t));
	pthread_t threadServer, threadConsola, threadCambiosConfig, threadTimedGossiping, threadTimedJournal;
	inicializarProcesoMemoria();

	datosInicializacion* datosDeInicializacion;
	if(!(datosDeInicializacion = realizarHandshake())) {

		liberarConfigYLogs();
		return -1;
	};

	MEMORIA_PRINCIPAL = inicializarMemoria(datosDeInicializacion, ARCHIVOS_DE_CONFIG_Y_LOG);
	inicializarTablaMarcos();
	liberarDatosDeInicializacion(datosDeInicializacion);

	pthread_create(&threadServer, NULL, servidorMemoria, NULL);
	pthread_create(&threadConsola, NULL, manejarConsola, NULL);
	pthread_create(&threadCambiosConfig, NULL, cambiosConfig, NULL);
	pthread_create(&threadTimedJournal, NULL, timedJournal, ARCHIVOS_DE_CONFIG_Y_LOG);
	pthread_create(&threadTimedGossiping, NULL, timedGossip, ARCHIVOS_DE_CONFIG_Y_LOG);

	struct sigaction terminar;
	terminar.sa_handler = cambiarValor;
	sigemptyset(&terminar.sa_mask);
	terminar.sa_flags = SA_RESTART;
	sigaction(SIGINT, &terminar, NULL);

	while(!DEBO_TERMINAR) {
		sem_wait(&MUTEX_RETARDO_FIN_PROCESO);
		int retardoFinalizacionProceso = RETARDO_FIN_PROCESO * 1000;
		sem_post(&MUTEX_RETARDO_FIN_PROCESO);

		usleep(retardoFinalizacionProceso);
	}

	sem_wait(&MUTEX_LOG);
	log_info(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "Finalizando el proceso memoria...");
	// no libero mutex ya que quiero que sea lo ultimo que se loguee.

	pthread_cancel(threadServer);
	pthread_cancel(threadConsola);
	pthread_cancel(threadCambiosConfig);
	pthread_cancel(threadTimedGossiping);
	pthread_cancel(threadTimedJournal);

	pthread_join(threadServer, NULL);
	pthread_join(threadConsola, NULL);
	pthread_join(threadCambiosConfig, NULL);
	pthread_join(threadTimedJournal, NULL);
	pthread_join(threadTimedGossiping, NULL);

	liberarMemoria();
	liberarConfigYLogs();
	liberarTablaMarcos();
	return 0;

}



