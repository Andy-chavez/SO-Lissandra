#include <commons/config.h>
#include <commons/log.h>
#include <commons/string.h>
#include <commons/collections/list.h>
#include <stdio.h>
#include <stdlib.h> //malloc,alloc,realloc
#include <string.h>
#include <unistd.h> //mirar clave ACCESS para verificar existencia
#include <sys/stat.h> //para ver si existe un directorio

/* SELECT: FACU , INSERT: PABLO
 * verificarExistencia(char* nombreTabla); //select e insert. FACU
 * metadata obtenerMetadata(char* nombreTabla); //select e insert. PABLO
 * void guardarRegistro(registro unRegistro, int particion, char* nombreTabla); //te guarda el registro en la memtable. PABLO
 * registro devolverRegistroDeLaMemtable(int key); //select e insert. PABLO
 * registro devolverRegistroDelFileSystem(int key); //select e insert FACU
 * dumpear() PABLO
 * registro devolverRegistroArchivoTemporal() PENDIENTE
 * Fijarse que te devuelva el timestamp con epoch unix
 * No olvidar de hacer la comparacion final, entre memtable,archivo temporal y FS
 *
*/
#define CANTPARTICIONES 5 // esto esta en el metadata
pthread_mutex_t mutexLog;
typedef enum {
	SC,
	SH,
	EC
}consistencia;

typedef struct {
	t_config* config;
	t_log* logger;
} configYLogs;

/*
typedef struct {
	time_t timestamp;
	u_int16_t key;
	char* value;
	struct registro *sigRegistro;
} registroLisandra;

typedef struct {
	time_t timestamp;
	u_int16_t key;
	char* value;
} registro;
*/

//AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA


typedef struct {
	time_t timestamp;
	u_int16_t key;
	char* value;
} registro;
/*
typedef struct {
	registro* unRegistro;
	struct listaRegistros *sgte;
} listaRegistros;
*/
//AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA

typedef struct {
	int numeroBloque;
	int sizeDeBloque;
} bloque;
/*
typedef struct {
	int size;
	int numeroParticion; // para saber que keys estan ahi,por el modulo
	registroLisandra *registros;
	bloque block[CANTIDADBLOQUES];
} particion;
*/
typedef struct {
	consistencia tipoConsistencia;
	int cantParticiones;
	int tiempoCompactacion;
} metadata;

/*
typedef struct {
	char* nombre;
	particion particiones[CANTPARTICIONES]; //HAY QUE VER COMOelemento.nombre HACER QUE DE CADA PARTICION SALGAN SUS REGISTROS.
	consistencia tipoDeConsistencia;
	metadata *metadataAsociada; //esto es raro, no creo que vaya en la estructura, preguntar A memoria
} tabla; //probable solo para serializar

*/

/*
typedef struct {
	char* nombre;
	registroLisandra* sigRegistro;
} tablaMem;
*/
//AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
typedef struct {
	char* nombre;
	t_list* lista;
} tablaMem;
//AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA


//t_log* g_logger = log_create("lisandra.log", "LISANDRA", 1, LOG_LEVEL_ERROR);
//t_config* g_config= config_create("LISANDRA.CONFIG"); //Por ahora lo dejo global deberiamos ver despues, y habria que liberarlos


//Funciones



int verificarExistenciaDirectorioTabla(char* nombreTabla,void* arg);
metadata obtenerMetadata(char* nombreTabla); //habria que ver de pasarle la ruta de la tabla y de ahi buscar el metadata
int calcularParticion(int key,int cantidadParticiones);// Punto_Montaje/Tables/Nombre_tabla/Metadata
registro devolverRegistroDelFileSystem(int key,int particion,char* nombreTabla);
registro buscarEnBloque(int key,char* numeroDeBloque,void* arg);

//registro buscarEnBloque(int key,char* numeroDeBloque,void* arg){ //despues agregar argumento para config y log
//	char* rutaBloque = string_new();
//	configYLogs *archivosDeConfigYLog = (configYLogs*) arg;
//	char* puntoMontaje= config_get_string_value(archivosDeConfigYLog->config,"PUNTO_MONTAJE");
//	string_append(&rutaBloque,puntoMontaje);
//	string_append(&rutaBloque,"Bloques/");
//	string_append(&rutaBloque,numeroDeBloque);
//	string_append(&rutaBloque,".bin");
//	FILE* archivo = fopen(rutaBloque,"rb");
//	char* value=config_get_string_value(archivo,key);
//
//	fclose(archivo);
//
//}

int verificarExistenciaDirectorioTabla(char* nombreTabla,void* arg){
	int validacion;
	configYLogs *archivosDeConfigYLog = (configYLogs*) arg;
	char* puntoMontaje= config_get_string_value(archivosDeConfigYLog->config,"PUNTO_MONTAJE");
	//char* puntoMontaje = "/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/LFS/";
	char* rutaDirectorio= string_new();
	//puts("estoy por salir");
	string_append(&rutaDirectorio,puntoMontaje); //OJO ACA HAY QUE VER QUE EN EL CONFIG NO TE VENGA CON "" EL PUNTO DE MONTAJE
	string_append(&rutaDirectorio,"Tables/"); //habria que ver esto, es lo mejor que se me ocurrio porque en el select solo te dan el nombre
	string_append(&rutaDirectorio,nombreTabla);
	printf("%s\n",rutaDirectorio);
	struct stat sb;
	//pthread_mutex_lock(&mutexLog);
	log_info(archivosDeConfigYLog->logger,"Determinando existencia de tabla en la ruta: %s",rutaDirectorio);
	    if (stat(rutaDirectorio, &sb) == 0 && S_ISDIR(sb.st_mode))
	    {
	    	log_info(archivosDeConfigYLog->logger,"La tabla existe en el FS");
	    	//pthread_mutex_unlock(&mutexLog); revisar tema semaforos creo que me esta quedando muy grande la region critica,nose si son tan necesarios en este caso
	    	printf("existe la tabla en la direccion");
	    	validacion=1;
	    }
	    else
	    {
	    	log_info(archivosDeConfigYLog->logger,"Error no existe tabla en ruta indicada %s \n",rutaDirectorio);
	    	//pthread_mutex_unlock(&mutexLog);
	    	puts("no entro\n");
	    	validacion=0;
	    	printf("No se ha encontrado el directorio de la tabla en la ruta: %s \n",rutaDirectorio);
	    }
	free(rutaDirectorio);
	return validacion;
}

int calcularParticion(int key,int cantidadParticiones){
	int particion= key%cantidadParticiones;
	return particion;
}

//registro devolverRegistroDelFileSystem(int key,int particion,char* nombreTabla){ //devolver registro completo por el timestamp
//	registro registroBuscado = (registro*) malloc(sizeof(registro));
//	ir particion, sacar bloques,
//	return registroBuscado;
//}



 bool agregarRegistro(char* nombreTabla, registro* unRegistro, void * elemento){
		tablaMem* tabla = elemento;
		if (string_equals_ignore_case(tabla->nombre, nombreTabla)){
			list_add(tabla->lista, unRegistro);

			return true;
		}else{
				return false;
		}
}

//Guarda un registro en la memtable
void guardarRegistro(t_list* memtable, registro* unRegistro, char* nombreTabla) {

	bool buscarPorNombre(void *elemento){
		return agregarRegistro(nombreTabla, unRegistro, elemento);
	}

	tablaMem* tablaEncontrada = list_find(memtable, buscarPorNombre);

}



bool estaLaKey(int key,void* elemento){
	registro* unRegistro = elemento;

		return (unRegistro->key == key);
}

//encontrar el nombre de la tabla, la tabla
//find y encontras la key

bool esIgualAlNombre(char* nombreTabla,void * elemento){
		tablaMem* tabla = elemento;

		return string_equals_ignore_case(tabla->nombre, nombreTabla);
}

registro* devolverRegistroDeLaMemtable(t_list* memtable, char* nombreTabla, int key){

	//esto no va por cada procedimiento obviamente, primero termino este par de funciones y ya lo pongo para q sea global

	bool encontrarLaKey(void *elemento){
		return estaLaKey(key, elemento);
	}

	bool tieneElNombre(void *elemento){
		return esIgualAlNombre(nombreTabla, elemento);
	}


	registro* registroDePrueba = malloc(sizeof(registro));
			registroDePrueba -> key = 13;
			registroDePrueba -> value= string_duplicate("aloo");
			registroDePrueba -> timestamp = 8000;


			tablaMem* tablaDePrueba = malloc(sizeof(tablaMem));
			tablaDePrueba-> nombre = string_duplicate("tablaA");
			tablaDePrueba->lista = list_create();

			list_add(tablaDePrueba->lista, registroDePrueba);

			tablaMem* tablaDePrueba2 = malloc(sizeof(tablaMem));
						tablaDePrueba2-> nombre = string_duplicate("tablaB");
						tablaDePrueba2->lista = list_create();
						list_add(tablaDePrueba2->lista, registroDePrueba);

	list_add(memtable, tablaDePrueba);
	list_add(memtable, tablaDePrueba2);

	tablaMem* encuentraLista =  list_find(memtable, tieneElNombre);

	registro* registroEncontrado = list_find(encuentraLista->lista, encontrarLaKey);


//	printf("No se ha encontrado el directorio de la tabla en la ruta: %d \n",registroEncontrado->key);

	return registroEncontrado;

}



metadata obtenerMetadata(char* nombreTabla){
	t_config* configMetadata;
	int cantParticiones;
	int tiempoCompactacion;
	consistencia tipoConsistencia;
	metadata unaMetadata;
	char* str1 = "../src/Directorio/";
	char* str2 = "/metadata";

	char* ruta = string_new();
	string_append(&ruta, str1);
	string_append(&ruta,nombreTabla);
	string_append(&ruta,str2); //revisar magic string_append y string_new esto parece medio feo

	configMetadata = config_create(ruta);

	cantParticiones = atoi(config_get_string_value(configMetadata, "PARTITIONS"));
	//como hago con esto te lo tome siendo que es un enum? con esto toma siempre 0
	tipoConsistencia = atoi(config_get_string_value(configMetadata, "CONSISTENCY")); //delegar a funcion con strcmp
	tiempoCompactacion = atoi(config_get_string_value(configMetadata, "COMPACTACION_TIME"));

	unaMetadata.cantParticiones = cantParticiones;
	unaMetadata.tipoConsistencia = tipoConsistencia;
	unaMetadata.tiempoCompactacion = tiempoCompactacion;

	free(ruta);

	return unaMetadata;


}
