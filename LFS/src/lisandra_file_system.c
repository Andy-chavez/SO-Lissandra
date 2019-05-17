
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
#include <commons/string.h>
#include <pthread.h>
#include <commons/config.h>
#include <commons/log.h>
#include <commonsPropias/conexiones.h>
#include "funcionesLFS.h"
#include "parser.h"


/* SELECT: FACU , INSERT: PABLO
 * verificarExistencia(char* nombreTabla); //select e insert. FACU
 * metadata obtenerMetadata(char* nombreTabla); //select e insert. PABLO
 * int calcularParticion(int cantidadParticiones, int key); //select e insert. key hay que pasarlo a int. FACU
 * int leerRegistro(int particion, char* nombreTabla); //te devuelve el key. FACU
 * void guardarRegistro(registro unRegistro, int particion, char* nombreTabla); //te guarda el registro en la memtable. PABLO
 * registro devolverRegistroDeLaMemtable(int key); //select e insert. PABLO
 * registro devolverRegistroDelFileSystem(int key); //select e insert FACU
 * Fijarse que te devuelva el timestamp con epoch unix
 * No olvidar de hacer la comparacion final
*/



void* servidorLisandra(void *arg){
	configYLogs *archivosDeConfigYLog = (configYLogs*) arg;
	char* puertoLisandra = "5008";
	//puertoLisandra = config_get_string_value(archivosDeConfigYLog->config, "PUERTO_ESCUCHA");
	//char* ipLisandra = config_get_string_value(archivosDeConfigYLog->config, "IP_LISANDRA");
	int socketServidorLisandra = crearSocketServidor(puertoLisandra);

//	free(ipMemoria);
//	free(puertoMemoria);
	if(socketServidorLisandra == -1) {
		cerrarConexion(socketServidorLisandra);
		pthread_exit(0);
	}

	while(1){
		int socketMemoria = aceptarCliente(socketServidorLisandra);

		if(socketMemoria == -1) {
			log_error(archivosDeConfigYLog->logger, "Socket Defectuoso");
			continue;
		}

		char* buffer = (char*)recibir(socketMemoria);

		//char* mensaje = "hola";

		//int tamanio = strlen(mensaje) + 1;

		//enviar(socketMemoria,(void*) mensaje,tamanio);
		//char* mensaje = pruebaDeRecepcion(buffer); // interface( deserializarOperacion( buffer , 1 ) )

		log_info(archivosDeConfigYLog->logger, "Recibi: %s", buffer);

		free(buffer);
		cerrarConexion(socketMemoria);
	}

	cerrarConexion(socketServidorLisandra);

}

void liberarConfigYLogs(configYLogs *archivos) {
	log_destroy(archivos->logger);
	config_destroy(archivos->config);
	free(archivos);
}

//void lisandra_consola(){
//	printf("Ingrese comando para lisandra con <OPERACION> seguido de los parametros");
//	char* linea;
//	linea = readline("");
//	char** opYArg;
//	opYArg = string_n_split(linea,2," ");
//	parserGeneral(*opYArg,*(opYArg+1));
//} magic veamos de hacer una cosa asi para la consola que nos va a ser mas facil tambien para cuando venga un descri

void leerConsola() {

		char *linea = NULL;  // forces getline to allocate with malloc
	    size_t len = 0;     // ignored when line = NULL
	    ssize_t leerConsola;

	    printf ("Ingresa operacion\n");

	    while ((leerConsola = getline(&linea, &len, stdin)) != -1){  //hay que hacer CTRL + D para salir del while
	 //   parserGeneral(linea);
	    	//parserGeneral(linea,"nada");
	    }

	    free (linea);  // free memory allocated by getline
}

void funcionInsert(char* nombreTabla, int key, char* value, int timestamp) {

	//queda todo medio desordenado ahora, se va a ir ordenando en la medida que vayamos discutiendo

	//int existeTabla= verificarExistenciaDirectorioTabla(nombreTabla,archivosDeConfigYLog);
	//obtenerMetadata(nombreTabla);

	t_list* memtable = list_create();

	//para probar si anda el devolver registro
	registro* unRegistro = devolverRegistroDeLaMemtable(memtable, nombreTabla, key);
	printf("Printeo el value del registro encontrado: %d \n", unRegistro->key);

	//para probar si anda el agregar registro
    guardarRegistro(memtable, unRegistro, nombreTabla);


}



int main(int argc, char* argv[]) {

	//funcionInsert("tablaA", 13, "alo", 8000);

	//obtenerMetadata("tablaA");
	//int particion=calcularParticion(1,3); esto funca, primero le pasas la key y despues la particion
	//pthread_mutex_init(&mutexLog,NULL);
	char* nombreTabla="Tabla1"; //para probar si existe la tabla(la tengo en mi directorio)
	configYLogs *archivosDeConfigYLog = malloc(sizeof(configYLogs));

	archivosDeConfigYLog->config = config_create("/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/LFS/lisandra.config");
	archivosDeConfigYLog->logger = log_create("lisandra.log", "LISANDRA", 1, LOG_LEVEL_ERROR);
	buscarEnBloque(56,"1",archivosDeConfigYLog);

	int existeTabla= verificarExistenciaDirectorioTabla(nombreTabla,archivosDeConfigYLog); //devuelve un int
	puts(existeTabla);
//	pthread_t threadLeerConsola;
//    pthread_create(&threadLeerConsola, NULL,(void*) leerConsola, NULL); //haces el casteo para solucionar lo del void*
//    pthread_join(threadLeerConsola,NULL);
//
//    pthread_mutex_destroy(&mutexLog);
//    liberarConfigYLogs(archivosDeConfigYLog);
	/*
	pthread_t threadServer ; //habria que ver tambien thread dumping.
	configYLogs *archivosDeConfigYLog = malloc(sizeof(configYLogs));
	archivosDeConfigYLog->config = config_create("LISANDRA.CONFIG");
	archivosDeConfigYLog->logger = log_create("lisandra.log", "LISANDRA", 1, LOG_LEVEL_ERROR);
	pthread_create(&threadServer, NULL, servidorLisandra, (void*) archivosDeConfigYLog);
	pthread_join(threadServer,NULL);
	//servidorLisandra(archivosDeConfigYLog);
	liberarConfigYLogs(archivosDeConfigYLog);
	*/
	return EXIT_SUCCESS;
}

