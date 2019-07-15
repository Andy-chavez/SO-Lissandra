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

// DEFINICIONES //
bool APIProtocolo(void* buffer, int socket, int* esGossip);
void APIMemoria(operacionLQL* operacionAParsear, int socketKernel);
void* trabajarConConexion(void* socket);
datosInicializacion* realizarHandshake();
void liberarDatosDeInicializacion(datosInicializacion* datos);
int crearSocketLFS();
void* manejarConsola();
void* servidorMemoria();
void* cambiosConfig();
void cambiarValor();


bool APIProtocolo(void* buffer, int socket, int *fueGossip) {
	operacionProtocolo operacion = empezarDeserializacion(&buffer);

	switch(operacion) {
	case OPERACIONLQL:
		enviarOMostrarYLogearInfo(-1, "Recibi una operacionLQL");
		APIMemoria(deserializarOperacionLQL(buffer), socket);
		return true;
	// TODO hacer un case donde se quiere cerrar el socket, cerrarConexion(socketKernel);
	// por ahora va a ser el default, ver como arreglarlo
	case DESCONEXION:
		if(!fueGossip){
			enviarOMostrarYLogearInfo(-1, "Se cerro una conexion con el socket");
		}
		free(buffer);
		cerrarConexion(socket);
		return false;
	case TABLAGOSSIP:
		sem_wait(&MUTEX_TABLA_GOSSIP);
		recibirYGuardarEnTablaGossip(socket, 0);
		serializarYEnviarTablaGossip(socket, TABLA_GOSSIP);
		sem_post(&MUTEX_TABLA_GOSSIP);
		free(buffer);
		*fueGossip = 1;
		return 1;
	case PAQUETEOPERACIONES:
	case UNREGISTRO:
	case METADATA:
	case PAQUETEMETADATAS:
	case HANDSHAKE:
	default:
		enviarOMostrarYLogearInfo(-1, "Ha llegado una operacion que la memoria no puede operar en general, pero capaz si de una manera en particular.");
		return 1;
	}
}

void APIMemoria(operacionLQL* operacionAParsear, int socketKernel) {
	if(string_starts_with(operacionAParsear->operacion, "INSERT")) {
		enviarOMostrarYLogearInfo(-1, "Recibi un INSERT %s", operacionAParsear->parametros);
		insertLQL(operacionAParsear, socketKernel);
	}
	else if (string_starts_with(operacionAParsear->operacion, "SELECT")) {
		enviarOMostrarYLogearInfo(-1, "Recibi un SELECT %s", operacionAParsear->parametros);
		selectLQL(operacionAParsear, socketKernel);
	}
	else if (string_starts_with(operacionAParsear->operacion, "DESCRIBE")) {
		enviarOMostrarYLogearInfo(-1, "Recibi un DESCRIBE %s", operacionAParsear->parametros);
		describeLQL(operacionAParsear, socketKernel);
	}
	else if (string_starts_with(operacionAParsear->operacion, "CREATE")) {
		enviarOMostrarYLogearInfo(-1, "Recibi un CREATE %s", operacionAParsear->parametros);
		createLQL(operacionAParsear, socketKernel);
	}
	else if (string_starts_with(operacionAParsear->operacion, "DROP")) {
		enviarOMostrarYLogearInfo(-1, "Recibi un DROP %s", operacionAParsear->parametros);
		dropLQL(operacionAParsear, socketKernel);
	}
	else if (string_starts_with(operacionAParsear->operacion, "JOURNAL")) {
		enviarOMostrarYLogearInfo(-1, "Recibi un JOURNAL");
		journalLQL(socketKernel);
	}
	else if(string_starts_with(operacionAParsear->operacion, "HEXDUMP")) {
		size_t length = config_get_int_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "TAM_MEM");
		sem_wait(&MUTEX_TABLA_MARCOS);
		mem_hexdump(MEMORIA_PRINCIPAL->base, length);
		sem_post(&MUTEX_TABLA_MARCOS);
	}
	else if(string_starts_with(operacionAParsear->operacion, "CERRAR")) {
		sem_post(&BINARIO_FINALIZACION_PROCESO);
	}
	else {
		enviarYLogearMensajeError(socketKernel, "No pude entender la operacion");
	}
	liberarOperacionLQL(operacionAParsear);

}

//------------------------------------------------------------------------

void* trabajarConConexion(void* socket) {
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
	agregarHiloAListaDeHilos();

	int socketKernel = *(int*) socket;
	sem_post(&BINARIO_SOCKET_KERNEL);

	int hayMensaje = 1;
	int esGossip = 0;

	while(hayMensaje) {
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
		void* bufferRecepcion = recibir(socketKernel);
		sem_wait(&MUTEX_RETARDO_MEMORIA);
		int retardoMemoria = RETARDO_MEMORIA * 1000;
		sem_post(&MUTEX_RETARDO_MEMORIA);
		usleep(retardoMemoria);
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

		hayMensaje = APIProtocolo(bufferRecepcion, socketKernel, &esGossip);
	}

	eliminarHiloDeListaDeHilos();
	pthread_detach(pthread_self());
	pthread_exit(0);
}

datosInicializacion* realizarHandshake() {
	SOCKET_LFS = crearSocketLFS();
	if(SOCKET_LFS == -1) {
		enviarYLogearMensajeError(-1, "No se pudo conectar con LFS");
		return NULL;
	}
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

//--------------------------------------------------------

void* manejarConsola() {
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
	agregarHiloAListaDeHilos();

	pthread_cleanup_push(eliminarHiloDeListaDeHilos, NULL);

	system("clear");
	printf("------ ¡Bienvenid@ al proceso Memoria! ------\n\n");
	printf("------ Esta es la memoria N°%d ------\n\n", config_get_int_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "MEMORY_NUMBER"));
	printf("------ Para ver los resultados de las operaciones, vea el log \"memoria_consola.log\" ------\n\n");
	printf("Por favor, ingrese un comando LQL:\n");

	enviarOMostrarYLogearInfo(-1, "Memoria lista para ser utilizada.");
	while(1) {

		char* comando = NULL;

		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
		comando = readline(">");
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);


		if(!esOperacionEjecutable(comando)) {
			enviarOMostrarYLogearInfo(-1, "El comando ingresado no es un comando LQL ejecutable.");
			free(comando);
			continue;
		}

		sem_wait(&MUTEX_RETARDO_MEMORIA);
		int retardoMemoria = RETARDO_MEMORIA * 1000;
		sem_post(&MUTEX_RETARDO_MEMORIA);
		usleep(retardoMemoria);

		APIMemoria(splitear_operacion(comando), -1);
		free(comando);

		printf("Por favor, ingrese otro comando LQL:\n");
	}
	pthread_cleanup_pop(0);
	// TODO ver como hacer la funcion para cancelar thread y liberar el hiloPropio
}

void cancelarServidor(void* bufferSocket) {
	int socket = *(int*) bufferSocket;
	cerrarConexion(socket);
	cancelarListaHilos();
}

void *servidorMemoria() {
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
	int socketServidorMemoria = crearSocketServidor(config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "IP_MEMORIA"), config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "PUERTO"));
	pthread_t threadConexion;
	int socketKernel;
	if(socketServidorMemoria == -1) {
		cerrarConexion(socketServidorMemoria);

		enviarYLogearMensajeError(-1, "No se pudo inicializar el servidor de memoria");
		pthread_exit(0);
	}

	pthread_cleanup_push(cancelarServidor, &socketServidorMemoria);

	enviarOMostrarYLogearInfo(-1, "Servidor Memoria en linea");
	while(1){
		sem_wait(&BINARIO_SOCKET_KERNEL);

		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
		socketKernel = aceptarCliente(socketServidorMemoria);
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

		if(socketKernel == -1) {
			enviarYLogearMensajeError(-1, "ERROR: Socket Defectuoso");
			continue;
		}
		if(pthread_create(&threadConexion, NULL, trabajarConConexion, (void*) &socketKernel) < 0) {
			enviarYLogearMensajeError(socketKernel, "No se pudo crear un hilo para trabajar con el socket");
		}

	}

	pthread_cleanup_pop(cancelarListaHilos);
	pthread_exit(0);

}

void* cambiosConfig() {
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
	char buffer[BUF_LEN_CONFIG];
	int fdConfig = inotify_init();

	if(fdConfig < 0) {
		enviarOMostrarYLogearInfo(-1, "hubo un error con el inotify_init");
	}

	int watchDescriptor = inotify_add_watch(fdConfig, "memoria.config", IN_MODIFY);

	if(watchDescriptor == -1) {
		printf("Add watch fallo \n");
	}

	while(1) {
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
		int size = read(fdConfig, buffer, BUF_LEN_CONFIG);
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

		if(size<0) {
			enviarOMostrarYLogearInfo(-1, "hubo un error al leer modificaciones del config");
		}

		t_config* configConNuevosDatos = config_create("memoria.config");

		if(!configConNuevosDatos->path) {
			enviarOMostrarYLogearInfo(-1, "hubo un error al abrir el archivo de config");
		}

		int desplazamiento = 0;

		while(desplazamiento < size) {
			struct inotify_event *event = (struct inotify_event *) &buffer[desplazamiento];


			if ((event->mask & IN_MODIFY)) {
				enviarOMostrarYLogearInfo(-1, "hubieron cambios en el archivo de config. Analizando y realizando cambios a retardos...");


				if(config_has_property(configConNuevosDatos, "RETARDO_GOSSIPING")) {
					sem_wait(&MUTEX_RETARDO_GOSSIP);
					RETARDO_GOSSIP = config_get_int_value(configConNuevosDatos, "RETARDO_GOSSIPING");
					sem_post(&MUTEX_RETARDO_GOSSIP);
				} else {
					printf("Hubo un problema leyendo el config del dato RETARDO_GOSSIPING\n");
				}

				if(config_has_property(configConNuevosDatos, "RETARDO_JOURNAL")) {
					sem_wait(&MUTEX_RETARDO_JOURNAL);
					RETARDO_JOURNAL = config_get_int_value(configConNuevosDatos, "RETARDO_JOURNAL");
					sem_post(&MUTEX_RETARDO_JOURNAL);
				} else {
					printf("Hubo un problema leyendo el config del dato RETARDO_JOURNAL\n");
				}

				if(config_has_property(configConNuevosDatos, "RETARDO_MEM")) {
					sem_wait(&MUTEX_RETARDO_MEMORIA);
					RETARDO_MEMORIA = config_get_int_value(configConNuevosDatos, "RETARDO_MEM");
					sem_post(&MUTEX_RETARDO_MEMORIA);
				} else {
					printf("Hubo un problema leyendo el config del dato RETARDO_MEM\n");
				}

				if(config_has_property(configConNuevosDatos, "RETARDO_FS")) {
					sem_wait(&MUTEX_RETARDO_FS);
					RETARDO_FS = config_get_int_value(configConNuevosDatos, "RETARDO_FS");
					sem_post(&MUTEX_RETARDO_FS);
				} else {
					printf("Hubo un problema leyendo el config del dato RETARDO_FS\n");
				}

			} else {
				printf("No es un evento de modificacion\n");
			}

			config_destroy(configConNuevosDatos);
			desplazamiento += sizeof (struct inotify_event) + event->len;
		}
	}
}

void cerrarMemoria() {
	sem_wait(&BINARIO_FINALIZACION_PROCESO);
	sem_wait(&MUTEX_CERRANDO_MEMORIA);
	CERRANDO_MEMORIA = 1;
	sem_post(&MUTEX_CERRANDO_MEMORIA);

	sem_wait(&MUTEX_LOG);
	log_info(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "Finalizando el proceso memoria...");
	sem_post(&MUTEX_LOG);

	pthread_cancel(threadConsola);
	pthread_join(threadConsola, NULL);

	pthread_cancel(threadServer);
	pthread_join(threadServer, NULL);

	pthread_cancel(threadCambiosConfig);
	pthread_cancel(threadTimedGossiping);
	pthread_cancel(threadTimedJournal);
	pthread_join(threadCambiosConfig, NULL);
	pthread_join(threadTimedJournal, NULL);
	pthread_join(threadTimedGossiping, NULL);
}

void cerrarYLiberarMemoria(){
	cerrarMemoria();
	liberarMemoria();
	liberarConfigYLogs();
	liberarTablaMarcos();
	liberarTablaGossip();
}

void ping() {
	char* ipLFS = config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "IPFILESYSTEM");
	char* puertoLFS = config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "PUERTO_FS");
	int socketPingLFS = crearSocketCliente(ipLFS, puertoLFS);

	if(socketPingLFS == -1) {
		printf("No se pudo crear el socket para realizar el ping con LFS. Se supone LFS desconectado\n");
		// TODO ver que hacer si el ping da mal
		return;
	}


}

int empezarMemoria(){
	inicializarProcesoMemoria();
	datosInicializacion* datosDeInicializacion;

	if(!(datosDeInicializacion = realizarHandshake())) {
		liberarConfigYLogs();
		return -1;
	}

	MEMORIA_PRINCIPAL = inicializarMemoria(datosDeInicializacion);
	inicializarTablaMarcos();
	liberarDatosDeInicializacion(datosDeInicializacion);
	pthread_create(&threadServer, NULL, servidorMemoria, NULL);
	pthread_create(&threadConsola, NULL, manejarConsola, NULL);
	pthread_create(&threadCambiosConfig, NULL, cambiosConfig, NULL);
	pthread_create(&threadTimedJournal, NULL, timedJournal, ARCHIVOS_DE_CONFIG_Y_LOG);
	pthread_create(&threadTimedGossiping, NULL, timedGossip, ARCHIVOS_DE_CONFIG_Y_LOG);
	return 0;
}

int main() {
	if(empezarMemoria()==-1){
		return 1;
	}
	cerrarYLiberarMemoria();
	system("reset");
	return 0;
}
