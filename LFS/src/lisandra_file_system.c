
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

void parserGeneral(char* operacionAParsear,char* argumentos) { //cambio parser para que ignore uppercase
	if(string_equals_ignore_case(operacionAParsear, "INSERT")) {
				printf("INSERT\n");
			}
			else if (string_equals_ignore_case(operacionAParsear, "SELECT")) {
				puts("SELECT\n");
				funcionSelect(argumentos);
			}
			else if (string_equals_ignore_case(operacionAParsear, "DESCRIBE")) {
				printf("DESCRIBE\n");
			}
			else if (string_equals_ignore_case(operacionAParsear, "CREATE")) {
				printf("CREATE\n");
			}
			else if (string_equals_ignore_case(operacionAParsear, "DROP")) {
				printf("DROP\n");
			}
	else {
		printf("no entendi xD");
	}
}

void* servidorLisandra(){
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
			log_error(logger, "Socket Defectuoso"); //ver de hacer algun lock al logger
			continue;
		}

		char* buffer = (char*)recibir(socketMemoria);

		//char* mensaje = "hola";

		//int tamanio = strlen(mensaje) + 1;

		//enviar(socketMemoria,(void*) mensaje,tamanio);
		//char* mensaje = pruebaDeRecepcion(buffer); // interface( deserializarOperacion( buffer , 1 ) )

		log_info(logger, "Recibi: %s", buffer);

		free(buffer);
		cerrarConexion(socketMemoria);
	}

	cerrarConexion(socketServidorLisandra);

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
	    char** opYArg;

	    printf ("Ingresa operacion\n");
	    printf("------------------------API LISSANDRA FILE SYSTEM --------------------");
	    printf("-------SELECT [NOMBRE_TABLA] [KEY]---------");
	    printf("-------INSERT [NOMBRE_TABLA] [KEY] '[VALUE]'(entre comillas) [TIMESTAMP]---------");
	    printf("-------CREATE [NOMBRE_TABLA] [TIPO_CONSISTENCIA] [NUMERO_PARTICIONES] [NUMERO_PARTICIONES] [COMPACTATION_TIME]---------");
	    printf("-------DESCRIBE [NOMBRE_TABLA] ---------");
	    printf("-------DROP [NOMBRE_TABLA]---------");

	    while ((leerConsola = getline(&linea, &len, stdin)) != -1){  //hay que hacer CTRL + D para salir del while
	    opYArg = string_n_split(linea,2," ");
	    parserGeneral(*(opYArg+0),*(opYArg+1));
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

	leerConfig("/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/LFS/lisandra.config");
	leerMetadataFS();
	inicializarMemtable();
	inicializarLog("lisandra.log");
//	char* saludo = "hoekSJKls";
//	string_to_upper("HOdalA");
//	puts(saludo);
//	verificarExistenciaDirectorioTabla("TaBlA1"); ver despues esto del uppercase del nombre de las tablas
	funcionSelect("Tabla1 ");
	//funcionInsert("tablaA", 13, "alo", 8000);

	//obtenerMetadata("tablaA");
	//int particion=calcularParticion(1,3); esto funca, primero le pasas la key y despues la particion
	//pthread_mutex_init(&mutexLog,NULL);
	char* nombreTabla="Tabla1"; //para probar si existe la tabla(la tengo en mi directorio)
	t_config* archivoParticion;
	archivoParticion= config_create("/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/LFS/Tables/Tabla1/Part1.bin");
	int sizeParticion=config_get_int_value(archivoParticion,"SIZE");
	buscarEnBloque2(56,"1",sizeParticion);
	//buscarEnBloque(56,"1",archivosDeConfigYLog);

	int existeTabla= verificarExistenciaDirectorioTabla(nombreTabla); //devuelve un int
//	pthread_t threadLeerConsola;
//    pthread_create(&threadLeerConsola, NULL,(void*) leerConsola, NULL); //haces el casteo para solucionar lo del void*
//    pthread_join(threadLeerConsola,NULL);
//
//    pthread_mutex_destroy(&mutexLog);
//    liberarConfigYLogs(archivosDeConfigYLog);
	/*
	pthread_t threadServer ; //habria que ver tambien thread dumping.
	pthread_create(&threadServer, NULL, servidorLisandra, (void*) archivosDeConfigYLog);
	pthread_join(threadServer,NULL);
	//servidorLisandra(archivosDeConfigYLog);
	*/
	liberarConfigYLogs();
	return EXIT_SUCCESS;
}

