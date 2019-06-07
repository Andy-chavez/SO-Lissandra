
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
#include <readline/readline.h>
#include <readline/history.h>
#include <commons/string.h>
#include <pthread.h>
#include <commons/config.h>
#include <commons/log.h>
#include "compactador.h"
#include <semaphore.h>


void parserGeneral(operacionLQL* operacionAParsear,int socket) { //cambio parser para que ignore uppercase
	if(string_equals_ignore_case(operacionAParsear->operacion, "INSERT")) {
				printf("INSERT\n");
				//	parserGeneral(*opYArg,*(opYArg+1));
				funcionInsert(operacionAParsear->parametros,socket);
			}
			else if (string_equals_ignore_case(operacionAParsear->operacion, "SELECT")) {
				puts("SELECT\n");
				funcionSelect(operacionAParsear->parametros,socket);
			}
			else if (string_equals_ignore_case(operacionAParsear->operacion, "DESCRIBE")) {
				printf("DESCRIBE\n");
				funcionDescribe(operacionAParsear->parametros,socket);
			}
			else if (string_equals_ignore_case(operacionAParsear->operacion, "CREATE")) {
				printf("CREATE\n");
				funcionCreate(operacionAParsear->parametros,socket);
			}
			else if (string_equals_ignore_case(operacionAParsear->operacion, "DROP")) {
				printf("DROP\n");
			}
	else {
		printf("no entendi xD");
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
		log_error(logger, "no se pudo realizar Handshake");//poner semaforo
	}
}

int APIProtocolo(void* buffer, int socket) {
	operacionProtocolo operacion = empezarDeserializacion(&buffer);

	switch(operacion) {
	case OPERACIONLQL:
		pthread_mutex_lock(&mutexLogger);
		log_info(logger, "Recibi una operacion");
		pthread_mutex_unlock(&mutexLogger);
		parserGeneral(deserializarOperacionLQL(buffer), socket);
		return 1;
	// TODO hacer un case donde se quiere cerrar el socket, cerrarConexion(socketKernel);
	// por ahora va a ser el default, ver como arreglarlo
	case DESCONEXION:
		pthread_mutex_lock(&mutexLogger);
		log_error(logger, "Se cierra la conexion");
		pthread_mutex_unlock(&mutexLogger);
		cerrarConexion(socket);
		return 0;
	}
	free(buffer);
}

void trabajarConexion(void* socket){
	int socketMemoria = *(int*) socket;
	int hayMensaje = 1;
	while(hayMensaje) {
			void* bufferRecepcion = recibir(socketMemoria); //quizas vaya semaforo
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
			pthread_mutex_lock(&mutexLogger);
			log_error(logger, "Socket Defectuoso"); //ver de hacer algun lock al logger
			pthread_mutex_unlock(&mutexLogger);
			continue;
		}

		//char* mensajeRecibido = recibir(socketMemoria);
		realizarHandshake(socketMemoria);

		pthread_t threadMemoria;
		if(pthread_create(&threadMemoria,NULL,(void*) trabajarConexion,&socketMemoria)<0){
			enviarYLogearMensajeError(socketMemoria,"No se pudo crear socket para memoria");
		}
		pthread_join(threadMemoria,NULL);
		//else{
			/*char* mensajeRecibido = recibir(socketMemoria);
			operacionLQL* operacion = deserializarOperacionLQL((void*)mensajeRecibido); //hay que fijarse de hacer protocolo para esto y no mandarlo al parser
			registroConNombreTabla* registroASerializar= funcionSelect(operacion->parametros);
			char* nombreTabla = "TABLA1";
			int tamanioBuffer;
			void* registroMandar = serializarRegistro(registroASerializar,&tamanioBuffer);
			enviar(socketMemoria,registroMandar, 98);*/

		//}


		//char* mensaje = "hola";

		//int tamanio = strlen(mensaje) + 1;

		//void* mensaje = serializarHandshake(tamanioValue);

//		enviar(socketMemoria, mensaje, 2*sizeof(int));

		//char* mensaje = pruebaDeRecepcion(buffer); // interface( deserializarOperacion( buffer , 1 ) )

		//log_info(logger, "Recibi: %s", mensaje);

		//free(mensaje);
		//cerrarConexion(socketMemoria);
	}

	cerrarConexion(socketServidorLisandra);

}

void leerConsola() {
		int socket = -1;
		char *linea = NULL;  // forces getline to allocate with malloc

	    printf("------------------------API LISSANDRA FILE SYSTEM --------------------\n");
	    printf("-------SELECT [NOMBRE_TABLA] [KEY]---------\n");
	    printf("-------INSERT [NOMBRE_TABLA] [KEY] '[VALUE]'(entre comillas) [TIMESTAMP]---------\n");
	    printf("-------CREATE [NOMBRE_TABLA] [TIPO_CONSISTENCIA] [NUMERO_PARTICIONES] [NUMERO_PARTICIONES] [COMPACTATION_TIME]---------\n");
	    printf("-------DESCRIBE [NOMBRE_TABLA] ---------\n");
	    printf("-------DROP [NOMBRE_TABLA]---------\n");
	    printf ("Ingresa operacion\n");

	    while ((linea = readline(""))){  //hay que hacer CTRL + D para salir del while
	    //guardiola con el describe all porque puede tirar basura
	    	parserGeneral(splitear_operacion(linea),socket);
	    }

	    free (linea);  // free memory allocated by getline
}

int main(int argc, char* argv[]) {


	leerConfig("/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/LFS/lisandra.config");
	leerMetadataFS();
	inicializarMemtable();
	inicializarLog("lisandraConsola.log");

	inicializarArchivoBitmap();
	inicializarBitmap();

	pthread_t threadConsola;
	pthread_t threadServer ; //habria que ver tambien thread dumping.
		//pthread_create(&threadServer, NULL, servidorLisandra, NULL);
	pthread_create(&threadConsola, NULL,(void*) leerConsola, NULL);
	pthread_create(&threadServer, NULL, servidorLisandra, NULL);
	//pthread_t threadDump;
	//pthread_create(&threadDump, NULL,(void*) dump, NULL);
	pthread_join(threadConsola,NULL);
	pthread_join(threadServer,NULL);
	//pthread_join(threadDump,NULL);
	//pthread_join(threadDump,NULL);
	//servidorLisandra();
	//leerConsola();



//	registro* registroParaMemoria = funcionSelect("TABLA1 56");

	//funcionInsert("TABLA1 56 alo");

	//funcionInsert("tablaA", 13, "alo", 8000);

	//ver de liberar la memtable al final
	/*obtenerMetadata("tablaA");
	int particion=calcularParticion(1,3); esto funca, primero le pasas la key y despues la particion
	pthread_mutex_init(&mutexLog,NULL);
	pthread_t threadLeerConsola;
    pthread_create(&threadLeerConsola, NULL,(void*) leerConsola, NULL); //haces el casteo para solucionar lo del void*
    pthread_join(threadLeerConsola,NULL);

    pthread_mutex_destroy(&mutexLog);
    liberarConfigYLogs(archivosDeConfigYLog);*/

	//pthread_t threadServer ; //habria que ver tambien thread dumping.
	//pthread_create(&threadServer, NULL, servidorLisandra, NULL);
	//pthread_join(threadServer,NULL);
	//servidorLisandra(archivosDeConfigYLog);

	liberarConfigYLogs();
	return EXIT_SUCCESS;
}



