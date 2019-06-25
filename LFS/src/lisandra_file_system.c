
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
#include <signal.h>
#include<readline/readline.h>
#include <time.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <commons/string.h>
#include <pthread.h>
#include <commons/config.h>
#include <commons/log.h>
#include "dump.h"
#include <semaphore.h>

#include <sys/inotify.h>
#include "configuraciones.h"


//para el inotify
#define EVENT_SIZE_CONFIG (sizeof(struct inotify_event) + 15)
#define BUF_LEN_CONFIG (1024 * EVENT_SIZE_CONFIG)


void parserGeneral(operacionLQL* operacionAParsear,int socket) { //cambio parser para que ignore uppercase
	if(string_equals_ignore_case(operacionAParsear->operacion, "INSERT")) {
			enviarOMostrarYLogearInfo(-1,"Se recibio un INSERT\n");
				funcionInsert(operacionAParsear->parametros,socket);
			}
			else if (string_equals_ignore_case(operacionAParsear->operacion, "SELECT")) {
				enviarOMostrarYLogearInfo(-1,"Se recibio un SELECT\n");
				funcionSelect(operacionAParsear->parametros,socket);
			}
			else if (string_equals_ignore_case(operacionAParsear->operacion, "DESCRIBE")) {
				enviarOMostrarYLogearInfo(-1,"Se recibio un DESCRIBE\n");
				funcionDescribe(operacionAParsear->parametros,socket);
			}
			else if (string_equals_ignore_case(operacionAParsear->operacion, "CREATE")) {
				enviarOMostrarYLogearInfo(-1,"Se recibio un CREATE\n");
				funcionCreate(operacionAParsear->parametros,socket);
			}
			else if (string_equals_ignore_case(operacionAParsear->operacion, "DROP")) {
				enviarOMostrarYLogearInfo(-1,"Se recibio un DROP\n");
				funcionDrop(operacionAParsear->parametros,socket);
			}
			else if(string_equals_ignore_case(operacionAParsear->operacion, "DUMP")) {
				enviarOMostrarYLogearInfo(-1,"Se recibio un DUMP");
				dump();
			}
	else {
		printf("no entendi xD\n");
	}
	liberarOperacionLQL(operacionAParsear);
}

void realizarHandshake(int socket){
	void* buffer = recibir(socket);
	int esCero= deserializarHandshake(buffer);
	if(esCero==0){
		serializarYEnviarHandshake(socket, tamanioValue);
	}
	else {
		enviarOMostrarYLogearInfo(-1,"No se pudo realizar handshake");
	}
}

int APIProtocolo(void* buffer, int socket) {
		void operacionLQLSola(operacionLQL* unaOperacionLQL){
			funcionInsert(unaOperacionLQL->parametros,socket);
		}
	operacionProtocolo operacion = empezarDeserializacion(&buffer);

	switch(operacion){
	case OPERACIONLQL:
		pthread_mutex_lock(&mutexLogger);
		log_info(logger, "Recibi una operacion");
		pthread_mutex_unlock(&mutexLogger);
		parserGeneral(deserializarOperacionLQL(buffer), socket);
		return 1;
	// TODO hacer un case donde se quiere cerrar el socket, cerrarConexion(socketKernel);
	// por ahora va a ser el default, ver como arreglarlo
	case PAQUETEOPERACIONES:
		pthread_mutex_lock(&mutexLogger);
		log_info(logger, "Recibi un paquete de operaciones");
		pthread_mutex_unlock(&mutexLogger);
		recibirYDeserializarPaqueteDeOperacionesLQLRealizando(socket,(void*) operacionLQLSola);
		return 1;
	case DESCONEXION:
		pthread_mutex_lock(&mutexLogger);
		log_error(logger, "Se cierra la conexion");
		pthread_mutex_unlock(&mutexLogger);
		cerrarConexion(socket);
		return 0;
	}
	return 0;
	free(buffer);
}

void trabajarConexion(void* socket){
	int socketMemoria = *(int*) socket;
	int hayMensaje = 1;
	while(hayMensaje) {
			void* bufferRecepcion = recibir(socketMemoria);
			pthread_mutex_lock(&mutexRetardo);
			usleep(retardo*1000);
			pthread_mutex_unlock(&mutexRetardo);
			hayMensaje = APIProtocolo(bufferRecepcion, socketMemoria);
		}

		pthread_exit(0);
}

void* servidorLisandra(){

	int socketServidorLisandra = crearSocketServidor(ipLisandra,puertoLisandra);
	if(socketServidorLisandra == -1) {
		cerrarConexion(socketServidorLisandra);
		pthread_exit(0);
		log_error(logger, "No se pudo crear el servidor lissandra");
	}


	while(1){
		int socketMemoria = aceptarCliente(socketServidorLisandra);

		if(socketMemoria == -1) {
			enviarYLogearMensajeError(-1,"No se pudo crear socket para memoria");
			continue;
		}
		enviarOMostrarYLogearInfo(-1,"Se conecto una nueva Memoria");
		realizarHandshake(socketMemoria);

		pthread_t threadMemoria;
		if(pthread_create(&threadMemoria,NULL,(void*) trabajarConexion,&socketMemoria)<0){
			enviarYLogearMensajeError(socketMemoria,"No se pudo crear socket para memoria");
			continue;
		}
		pthread_detach(threadMemoria);
	}

	cerrarConexion(socketServidorLisandra);

}

void leerConsola() {
		int socket = -1;
		char *linea = NULL;

	    printf("------------------------API LISSANDRA FILE SYSTEM --------------------\n");
	    printf("-------SELECT [NOMBRE_TABLA] [KEY]---------\n");
	    printf("-------INSERT [NOMBRE_TABLA] [KEY] '[VALUE]'(entre comillas) [TIMESTAMP]---------\n");
	    printf("-------CREATE [NOMBRE_TABLA] [TIPO_CONSISTENCIA] [NUMERO_PARTICIONES] [NUMERO_PARTICIONES] [COMPACTION_TIME]---------\n");
	    printf("-------DESCRIBE [NOMBRE_TABLA] ---------\n");
	    printf("-------DROP [NOMBRE_TABLA]---------\n");
	    printf ("Ingresa operacion\n");

	    while ((linea = readline(""))){
	    	parserGeneral(splitear_operacion(linea),socket);
	    }

	    free (linea);
}

void* cambiosConfig() {
	char buffer[BUF_LEN_CONFIG];
	int fdConfig = inotify_init();
	char* path = "lisandra.config";

	if(fdConfig < 0) {
		enviarOMostrarYLogearInfo(-1, "hubo un error con el inotify_init");
	}

	int watchDescriptorConfig = inotify_add_watch(fdConfig, path, IN_MODIFY);

	while(1) {
		int size = read(fdConfig, buffer, BUF_LEN_CONFIG);

		if(size<0) {
			soloLoggear(-1, "hubo un error al leer modificaciones del config");
		}

		t_config* configConNuevosDatos = config_create(path);

		if(!configConNuevosDatos) {
			soloLoggear(-1, "hubo un error al abrir el archivo de config");
		}

		int desplazamiento = 0;

		while(desplazamiento < size) {
			struct inotify_event *event = (struct inotify_event *) &buffer[desplazamiento];

			if (event->mask & IN_MODIFY) {

				soloLoggear(-1,"hubieron cambios en el archivo de config. Analizando y realizando cambios a retardos y tiempo de dump...");

				pthread_mutex_lock(&mutexTiempoDump);
				tiempoDump = config_get_int_value(archivoDeConfig,"TIEMPO_DUMP");
				pthread_mutex_unlock(&mutexTiempoDump);
				pthread_mutex_lock(&mutexRetardo);
				retardo = config_get_int_value(archivoDeConfig,"RETARDO");
				pthread_mutex_unlock(&mutexRetardo);
			}

			config_destroy(configConNuevosDatos);
			desplazamiento += sizeof (struct inotify_event) + event->len;
		}
	}
}

void terminarTodo() {
	sem_post(&binarioLFS);
}

int main(int argc, char* argv[]) {

		//leerConfig("../lisandra.config"); esto es para la entrega pero por eclipse rompe
		leerConfig("/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/LFS/lisandra.config");
		leerMetadataFS();
		inicializarListas();
		inicializarLog("lisandraConsola.log");

		log_info(loggerConsola,"Inicializando FS");

		inicializarBloques();
		inicializarSemaforos();

		funcionDescribe("ALL",-1); //ver las tablas que hay en el FS

		inicializarArchivoBitmap(); //sacar despues
		inicializarBitmap();
		inicializarRegistroError();

		log_info(loggerConsola,"El tamanio maximo del bitarray es de: %d\n",bitarray_get_max_bit(bitarray));

		pthread_t threadConsola;
		pthread_t threadServer;
		pthread_t threadDump;
		pthread_t threadCambiosConfig;

		pthread_create(&threadServer, NULL, servidorLisandra, NULL);
		pthread_create(&threadConsola, NULL,(void*) leerConsola, NULL);
		pthread_create(&threadDump, NULL,(void*) dump, NULL);
		pthread_create(&threadCambiosConfig, NULL, cambiosConfig, NULL);

		pthread_join(threadServer,NULL);
		pthread_join(threadConsola,NULL);
		pthread_join(threadDump,NULL);
		pthread_join(threadCambiosConfig,NULL);


		//ver de liberar la memtable al final

		leerConsola();

		sem_init(&binarioLFS, 0, 1);

		struct sigaction terminar;
			terminar.sa_handler = terminarTodo;
			sigemptyset(&terminar.sa_mask);
			terminar.sa_flags = SA_RESTART;
			sigaction(SIGINT, &terminar, NULL);


		sem_wait(&binarioLFS);



		//que mas quedaría liberar x aca?

		//create y despues cancel y join
		//enviar logear mensaje de error
		liberarSemaforos();
		liberarMemtable();
		liberarListaDeTablas();

		liberarConfigYLogs();
		return EXIT_SUCCESS;
}
