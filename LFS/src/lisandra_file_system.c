
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
#include <commonsPropias/conexiones.h>
#include <commonsPropias/serializacion.h>
#include "compactador.h"


void parserGeneral(char* operacionAParsear,char* argumentos) { //cambio parser para que ignore uppercase
	if(string_equals_ignore_case(operacionAParsear, "INSERT")) {
				printf("INSERT\n");
				funcionInsert(argumentos);
			}
			else if (string_equals_ignore_case(operacionAParsear, "SELECT")) {
				puts("SELECT\n");
				funcionSelect(argumentos);
			}
			else if (string_equals_ignore_case(operacionAParsear, "DESCRIBE")) {
				printf("DESCRIBE\n");
				funcionDescribe(argumentos);
			}
			else if (string_equals_ignore_case(operacionAParsear, "CREATE")) {
				printf("CREATE\n");
				funcionCreate(argumentos);
			}
			else if (string_equals_ignore_case(operacionAParsear, "DROP")) {
				printf("DROP\n");
			}
	else {
		printf("no entendi xD");
	}
}

void* serializarHandshake(int tamanioValue){

	int desplazamiento = 0;
	void *buffer= malloc((sizeof(int))*2);
	int tamanioInt= sizeof(int);
	memcpy(buffer + desplazamiento, &tamanioInt, sizeof(int));
	desplazamiento+= sizeof(int);
	memcpy(buffer + desplazamiento, &tamanioValue, sizeof(int));
	desplazamiento+= sizeof(int);

	return buffer;
}


int deserializarHandshake(void* bufferHandshake){

	int desplazamiento = 0;
	int tamanioDelInt;
	int tamanioDelValue;

	memcpy(&tamanioDelInt, bufferHandshake + desplazamiento, sizeof(int));
	desplazamiento+= sizeof(int);
	memcpy(&tamanioDelValue, bufferHandshake + desplazamiento, sizeof(int));

	return tamanioDelValue;
}

void* servidorLisandra(){

	//char* puertoLisandra = "5008";
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
		int i =0;
		//char* mensajeRecibido = recibir(socketMemoria);
		if(i==0){ //en realidad hay que deserializar handshake
			void* mensaje = serializarHandshake(tamanioValue);
			enviar(socketMemoria, mensaje, 2*sizeof(int));
			i++;
		}
		else{
			char* mensajeRecibido = recibir(socketMemoria);
			operacionLQL* operacion = deserializarOperacionLQL((void*)mensajeRecibido); //hay que fijarse de hacer protocolo para esto y no mandarlo al parser
			registro* registroASerializar= funcionSelect(operacion->parametros);
			char* nombreTabla = "TABLA1";
			void* registroMandar = serializarRegistro(registroASerializar, nombreTabla);
			enviar(socketMemoria,registroMandar, 98);

		}


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
	    char** opYArg;

	    printf("------------------------API LISSANDRA FILE SYSTEM --------------------\n");
	    printf("-------SELECT [NOMBRE_TABLA] [KEY]---------\n");
	    printf("-------INSERT [NOMBRE_TABLA] [KEY] '[VALUE]'(entre comillas) [TIMESTAMP]---------\n");
	    printf("-------CREATE [NOMBRE_TABLA] [TIPO_CONSISTENCIA] [NUMERO_PARTICIONES] [NUMERO_PARTICIONES] [COMPACTATION_TIME]---------\n");
	    printf("-------DESCRIBE [NOMBRE_TABLA] ---------\n");
	    printf("-------DROP [NOMBRE_TABLA]---------\n");
	    printf ("Ingresa operacion\n");

	    while ((linea = readline(""))){  //hay que hacer CTRL + D para salir del while
	    //guardiola con el describe all porque puede tirar basura
	    opYArg = string_n_split(linea,2," ");
	    parserGeneral(*(opYArg+0), *(opYArg+1));

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

	inicializarArchivoBitmap();
	inicializarBitmap();

	leerConsola();
	pthread_t threadConsola;
	pthread_create(&threadConsola, NULL,(void*) leerConsola, NULL);

	pthread_t threadDump;
	pthread_create(&threadDump, NULL,(void*) dump, NULL);

	pthread_join(threadConsola,NULL);
	pthread_join(threadDump,NULL);
	//pthread_join(threadDump,NULL);
	//servidorLisandra();
	//leerConsola();
	/*
	void* bufferHandshake = serializarHandshake(tamanioValue);
	int tamanioRecibido = deserializarHandshake(bufferHandshake);
*/


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

