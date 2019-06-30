
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
				soloLoggear(socket,"Se recibio un INSERT\n");
				funcionInsert(operacionAParsear->parametros,socket);
			}
			else if (string_equals_ignore_case(operacionAParsear->operacion, "SELECT")) {
				soloLoggear(socket,"Se recibio un SELECT\n");
				funcionSelect(operacionAParsear->parametros,socket);
			}
			else if (string_equals_ignore_case(operacionAParsear->operacion, "DESCRIBE")) {
				soloLoggear(socket,"Se recibio un DESCRIBE\n");
				funcionDescribe(operacionAParsear->parametros,socket);
			}
			else if (string_equals_ignore_case(operacionAParsear->operacion, "CREATE")) {
				soloLoggear(socket,"Se recibio un CREATE\n");
				funcionCreate(operacionAParsear->parametros,socket);
			}
			else if (string_equals_ignore_case(operacionAParsear->operacion, "DROP")) {
				soloLoggear(socket,"Se recibio un DROP\n");
				funcionDrop(operacionAParsear->parametros,socket);
			}
			else if(string_equals_ignore_case(operacionAParsear->operacion, "DUMP")) {
				soloLoggear(socket,"Se recibio un DUMP");
				dump();
			}
	else {
		soloLoggear(socket,"no entendi xD\n");
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
		soloLoggear(1,"No se pudo realizar handshake");
	}
}

int APIProtocolo(void* buffer, int socket) {
		void operacionLQLSola(operacionLQL* unaOperacionLQL){
			funcionInsert(unaOperacionLQL->parametros,socket);
		}
	operacionProtocolo operacion = empezarDeserializacion(&buffer);

	switch(operacion){
	case OPERACIONLQL:
		soloLoggear(socket,"Recibi una operacion");
		parserGeneral(deserializarOperacionLQL(buffer), socket);
		return 1;
	// TODO hacer un case donde se quiere cerrar el socket, cerrarConexion(socketKernel);
	// por ahora va a ser el default, ver como arreglarlo
	case PAQUETEOPERACIONES:
		soloLoggear(socket,"Recibi un paquete de operacion");
		recibirYDeserializarPaqueteDeOperacionesLQLRealizando(socket,(void*) operacionLQLSola);
		return 1;
	case DESCONEXION:
		soloLoggearError(1,"Se cierra la conexion");
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
			soloLoggearError(1,"No se pudo crear socket para memoria");
			continue;
		}
		soloLoggear(socketMemoria,"Se conecto una nueva Memoria");
		realizarHandshake(socketMemoria);

		pthread_t threadMemoria;
		if(pthread_create(&threadMemoria,NULL,(void*) trabajarConexion,&socketMemoria)<0){
			soloLoggearError(socketMemoria,"No se pudo crear socket para memoria");
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
	    printf("-------CREATE [NOMBRE_TABLA] [TIPO_CONSISTENCIA] [NUMERO_PARTICIONES] [COMPACTION_TIME]---------\n");
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

		inicializarBloques();
		inicializarSemaforos();

		funcionDescribe("ALL",-1); //ver las tablas que hay en el FS

		inicializarArchivoBitmap(); //sacar despues
		inicializarBitmap();
		inicializarRegistroError();

		log_info(loggerConsola,"Inicializando FS");
/*
		funcionCreate("PELICULAS SC 5 10000", -1);

		funcionInsert("PELICULAS 10 \"Toy story\"", -1);
		funcionInsert("PELICULAS 100 \"quieroqocupe2blocks\"", -1);

				funcionInsert("PELICULAS 163 \"Nemo\"", -1);
				funcionInsert("PELICULAS 1110 \"harryPuter\"", -1);
				funcionInsert("PELICULAS 13535 \"Titanic\"", -1);
				funcionInsert("PELICULAS 922 \"RATATOULI\"", -1);
				funcionInsert("PELICULAS 4829 \"Aladdin\"", -1);
				funcionInsert("PELICULAS 2516 \"Godzilla\"", -1);
				funcionInsert("PELICULAS 3671 \"Avatarrrrrrr\"", -1);



		funcionCreate("PELICULAS2 SC 5 10000", -1);
						funcionInsert("PELICULAS2 10 \"Toy story\"", -1);
						funcionInsert("PELICULAS2 163 \"Nemo\"", -1);
						funcionInsert("PELICULAS2 1110 \"harryPuter\"", -1);
						funcionInsert("PELICULAS2 13535 \"Titanic\"", -1);
						funcionInsert("PELICULAS2 922 \"RATATOULI\"", -1);
						funcionInsert("PELICULAS2 4829 \"Aladdin\"", -1);
						funcionInsert("PELICULAS2 2516 \"Godzilla\"", -1);
						funcionInsert("PELICULAS2 3671 \"Avatarrrrrrr\"", -1);

		dump();
		compactar("PELICULAS");
		compactar("PELICULAS2");


		funcionInsert("PELICULAS 10 \"Story2\"", -1);
		funcionInsert("PELICULAS 10 \"Story3\"", -1);
		funcionInsert("PELICULAS 1110 \"Harryyy2\"", -1);

		funcionInsert("PELICULAS2 10 \"Story2\"", -1);
		funcionInsert("PELICULAS2 10 \"Story3\"", -1);
		funcionInsert("PELICULAS2 1110 \"Harryyy2\"", -1);

		dump();

		funcionInsert("PELICULAS 2516 \"MORCILLA\"", -1);
		funcionInsert("PELICULAS2 2516 \"MORCILLA\"", -1);

		dump();

		compactar("PELICULAS");
		compactar("PELICULAS2");
*/

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
		liberarVariablesGlobales();
		return EXIT_SUCCESS;
}
