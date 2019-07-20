
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
#include <commons/config.h>//hacerlo por tabla!! y que reciba el nombre de la tabla
#include <commons/log.h>
#include "dump.h"
#include <semaphore.h>

#include <sys/inotify.h>
#include "configuraciones.h"


//para el inotify
#define EVENT_SIZE_CONFIG (sizeof(struct inotify_event) + 15)
#define BUF_LEN_CONFIG (1024 * EVENT_SIZE_CONFIG)

typedef struct {
	pthread_t thread;
} hiloMemoria;


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

int realizarHandshake(int socket){
	void* buffer = recibir(socket);
	int esCero= deserializarHandshake(buffer);
	if(esCero==0){
		soloLoggear(1,"Se realizo handshake correctamente");
		serializarYEnviarHandshake(socket, tamanioValue);
		return 1;
	}
	else if(esCero==1){
		soloLoggear(1,"Se recibio un Ping");
		serializarYEnviarHandshake(socket,tamanioValue);
		return 0;
	}
	else {
		soloLoggear(1,"No se pudo realizar handshake");
		return 0;
	}
}

void agregarHiloAListaDeHilos() {

	hiloMemoria* hiloPropio = malloc(sizeof(hiloMemoria));
	hiloPropio->thread = pthread_self();
	sem_wait(&MUTEX_TABLA_THREADS);
	list_add(TABLA_THREADS, hiloPropio);
	sem_post(&MUTEX_TABLA_THREADS);

}
void cancelarHilo(hiloMemoria* hilo){
	pthread_cancel(hilo->thread);
	pthread_join(hilo->thread, NULL);
	free(hilo);
}

void eliminarHiloDeListaDeHilos() {
	bool esElPropioThread(hiloMemoria* hiloEnLaTabla) {
		return pthread_equal(hiloEnLaTabla->thread, pthread_self());
	}

	sem_wait(&MUTEX_TABLA_THREADS);
	list_remove_and_destroy_by_condition(TABLA_THREADS, esElPropioThread,(void*)cancelarHilo);
	sem_post(&MUTEX_TABLA_THREADS);
	return;
}
void cancelarListaHilos(){
	sem_wait(&MUTEX_TABLA_THREADS);
	list_destroy_and_destroy_elements(TABLA_THREADS,(void*)cancelarHilo);
	sem_post(&MUTEX_TABLA_THREADS);
}

int APIProtocolo(void* buffer, int socket) {
		void operacionLQLSola(operacionLQL* unaOperacionLQL){
			funcionInsert(unaOperacionLQL->parametros,socket);
		}
	operacionProtocolo operacion = empezarDeserializacion(&buffer);

	switch(operacion){
	case OPERACIONLQL:
		soloLoggear(socket,"Recibi una operacion\n");
		parserGeneral(deserializarOperacionLQL(buffer), socket);
		return 1;
	case PAQUETEOPERACIONES:
		soloLoggear(socket,"Recibi un paquete de operacion\n");
		recibirYDeserializarPaqueteDeOperacionesLQLRealizando(socket,(void*) operacionLQLSola);
		free(buffer);
		return 1;
	case DESCONEXION:
		soloLoggearError(1,"Se cierra la conexion\n");
		eliminarHiloDeListaDeHilos();
		cerrarConexion(socket);
		return 0;
	}
	return 0;
	free(buffer);
}

void trabajarConexion(void* socket){

	agregarHiloAListaDeHilos();

	int socketMemoria = *(int*) socket;
	sem_post(&binarioSocket);
	int hayMensaje = 1;
	while(hayMensaje) {
			void* bufferRecepcion = recibir(socketMemoria);
			sem_wait(&mutexRetardo);
			int retardoActual= retardo;
			sem_post(&mutexRetardo);
			usleep(retardoActual*1000);
			hayMensaje = APIProtocolo(bufferRecepcion, socketMemoria);
		}
	pthread_exit(0);
}

void cerrarSocketLisandra(void* bufferSocket) {
	int socket = *(int*) bufferSocket;
	cerrarConexion(socket);
}

void* servidorLisandra(){

	int socketServidorLisandra = crearSocketServidor(ipLisandra,puertoLisandra);
	if(socketServidorLisandra == -1) {
		cerrarConexion(socketServidorLisandra);
		log_error(logger, "No se pudo crear el servidor lissandra");
		pthread_exit(0);
	}

	pthread_cleanup_push(cerrarSocketLisandra, &socketServidorLisandra);

	while(1){
		sem_wait(&binarioSocket);
		int socketMemoria = aceptarCliente(socketServidorLisandra);

		if(socketMemoria == -1) {
			soloLoggearError(1,"No se pudo crear socket para memoria");
			continue;
		}
		soloLoggear(socketMemoria,"Se conecto una nueva Memoria");
		int resultado =realizarHandshake(socketMemoria);
		if(resultado==0){
			cerrarConexion(socketMemoria);
			sem_post(&binarioSocket);
			continue;
		} //esto es por el ping y si no se realiza handshake
		pthread_t threadMemoria;
		if(pthread_create(&threadMemoria,NULL,(void*) trabajarConexion,&socketMemoria)<0){

			soloLoggearError(socketMemoria,"No se pudo crear socket para memoria");
			continue;
		}
		pthread_detach(threadMemoria);
	}

	pthread_cleanup_pop(0);

}

void cerrar() {
	sem_post(&binarioLFS);
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
	    printf("-------CERRAR---------\n");
	    printf ("Ingresa operacion\n");
	    while ((linea = readline(""))){
	    	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
	    	string_to_upper(linea);
	    			if(string_equals_ignore_case(linea,"Cerrar"))
	    			{
	    				free(linea);
	    				cerrar();
	    				pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
	    			}
	    			else if(esOperacionEjecutable(linea)){
	    				pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);
	    				parserGeneral(splitear_operacion(linea),socket);
	    				free (linea);
	    				pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
	    	    	}
	    	    		else{
	    	    			free (linea);
	    	    			soloLoggearError(-1,"No es una operacion valida la ingresada por consola\n");
	    	    			pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
	    	    		}
	    	    }
	    free (linea);
}


void* cambiosConfig() {
	char buffer[BUF_LEN_CONFIG];
	int fdConfig = inotify_init();
	char* path = "../lisandra.config";
	//char* path = "lisandra.config";

	if(fdConfig < 0) {
		soloLoggearError(-1, "Hubo un error con el inotify_init");
	}

	int watchDescriptorConfig = inotify_add_watch(fdConfig, path, IN_MODIFY);

	while(1) {
		int size = read(fdConfig, buffer, BUF_LEN_CONFIG);

		if(size<0) {
			soloLoggearError(-1, "hubo un error al leer modificaciones del config");
		}

		t_config* configConNuevosDatos = config_create(path);

		if(!configConNuevosDatos) {
			soloLoggearError(-1, "hubo un error al abrir el archivo de config");
		}

		int desplazamiento = 0;

		while(desplazamiento < size) {
			struct inotify_event *event = (struct inotify_event *) &buffer[desplazamiento];

			if (event->mask & IN_MODIFY) {

				soloLoggear(-1,"Cambios en config, cambiando retardo y dump");

				sem_wait(&mutexTiempoDump);
				tiempoDump = config_get_int_value(archivoDeConfig,"TIEMPO_DUMP");
				sem_post(&mutexTiempoDump);
				sem_wait(&mutexRetardo);
				retardo = config_get_int_value(archivoDeConfig,"RETARDO");
				sem_post(&mutexRetardo);
			}

			config_destroy(configConNuevosDatos);
			desplazamiento += sizeof (struct inotify_event) + event->len;
		}
	}
}

void terminarTodo() {
	sem_post(&binarioLFS);
}

typedef struct{
	char* operacion;
	t_list* instruccion;
}runearScripts;

void runearScript(){

		FILE *archivoALeer;
//		archivoALeer= fopen("/home/utnso/Escritorio/PruebasFinales/nintendo_playstation.lql", "r");
		archivoALeer= fopen("/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/Kernel/compactacion_larga.lql", "r");

	//	archivoALeer= fopen("/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/ArchivosTest/peliculas.lql", "r");

		char *lineaLeida;
		size_t limite = 250;
		ssize_t leer;
		lineaLeida = NULL;
//		runearScripts* instrucciones = malloc(sizeof(runearScripts));
//		instrucciones->instruccion = list_create();


		while((leer = getline(&lineaLeida, &limite, archivoALeer)) != -1){
			operacionLQL* operacion = malloc(sizeof(operacionLQL));
			if(*(lineaLeida + leer - 1) == '\n') {
						*(lineaLeida + leer - 1) = '\0';
					}
			char** operacionLQL= string_n_split(lineaLeida,2,  " ");
			operacion->operacion = *(operacionLQL + 0);
			operacion->parametros = *(operacionLQL + 1);
			//puts(operacion->operacion);
			//puts(operacion->parametros);
			parserGeneral(operacion, -1);
//			liberarDoblePuntero(operacionLQL);
			free(operacionLQL);
	//		free(lineaLeida);
			usleep(100000);

		}



}




int main(int argc, char* argv[]) {

		leerConfig("../lisandra.config"); //esto es para la entrega pero por eclipse rompe
		//leerConfig("/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/LFS/lisandra.config");
		leerMetadataFS();
		inicializarListas();
		inicializarLog();

		inicializarBloques();
		inicializarSemaforos();

		inicializarArchivoBitmap(); //sacar despues
		inicializarBitmap();


		log_info(loggerConsola,"Inicializando FS");
		log_info(loggerResultadosConsola,"Aqui iran los resultados de la consola");
		log_info(loggerResultados,"Aqui iran los resultados de las requests");
		log_info(logger,"Logger de memoria");
		funcionDescribe("ALL",-1); //ver las tablas que hay en el FS

		pthread_t threadConsola;
		pthread_t threadServer;
		pthread_t threadDump;
		pthread_t threadCambiosConfig;

		pthread_create(&threadServer, NULL, servidorLisandra, NULL);
		pthread_create(&threadConsola, NULL,(void*) leerConsola, NULL);
		pthread_create(&threadDump, NULL,(void*) dump, NULL);
		pthread_create(&threadCambiosConfig, NULL, cambiosConfig, NULL);

		sem_init(&binarioLFS, 0, 0);

		struct sigaction terminar;
		terminar.sa_handler = terminarTodo;
		sigemptyset(&terminar.sa_mask);
		terminar.sa_flags = SA_RESTART;
		sigaction(SIGINT, &terminar, NULL);

		sem_wait(&binarioLFS);


		pthread_cancel(threadServer);
		pthread_cancel(threadConsola);
		pthread_cancel(threadDump);
		pthread_cancel(threadCambiosConfig);

		pthread_join(threadServer,NULL);
		pthread_join(threadConsola,NULL);
		pthread_join(threadDump,NULL);
		pthread_join(threadCambiosConfig,NULL);


		cancelarListaHilos();

		liberarVariablesGlobales();
		system("reset");
return EXIT_SUCCESS;
}
