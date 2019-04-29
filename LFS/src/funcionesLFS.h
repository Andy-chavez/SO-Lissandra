#include <commons/config.h>
#include <commons/log.h>
#include <commons/string.h>
#include <stdio.h>
#include <stdlib.h> //malloc,alloc,realloc
#include <string.h>
#include <unistd.h> //mirar clave ACCESS para verificar existencia
#include <sys/stat.h> //para ver si existe un directorio

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

typedef struct {
	time_t timestamp;
	u_int16_t key;
	char* value;  //no seria siempre un char*?
	struct registro *sigRegistro;
} registroLisandra;

typedef struct {
	time_t timestamp;
	u_int16_t key;
	char* value;  //no seria siempre un char*?
} registro;

typedef struct {
	int numeroBloque;
	int sizeDeBloque;

} bloque;

typedef struct {
	int size;
	int numeroParticion; // para saber que keys estan ahi,por el modulo
	registroLisandra *registros;
	bloque block[/*CANTIDADBLOQUES*/];
} particion;

typedef struct {
	consistencia tipoConsistencia;
	int cantParticiones;
	int tiempoCompactacion;
} metadata;

typedef struct {
	char* nombre;
	char* rutaTabla; //de la forma Tables/Nombre_tabla, quizas es sin punto_montaje que lo deberias asignar despues
	particion particiones[CANTPARTICIONES]; //HAY QUE VER COMO HACER QUE DE CADA PARTICION SALGAN SUS REGISTROS.
	consistencia tipoDeConsistencia;
	metadata *metadataAsociada; //esto es raro, no creo que vaya en la estructura
} tabla;

//t_log* g_logger = log_create("lisandra.log", "LISANDRA", 1, LOG_LEVEL_ERROR);
//t_config* g_config= config_create("LISANDRA.CONFIG"); //Por ahora lo dejo global deberiamos ver despues, y habria que liberarlos


//Funciones

int verificarExistenciaDirectorioTabla(char* nombreTabla,void* arg);
metadata obtenerMetadata(char* nombreTabla); //habria que ver de pasarle la ruta de la tabla y de ahi busca el metadata
											// Punto_Montaje/Tables/Nombre_tabla/Metadata


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

	configMetadata = config_create(ruta); //tengo duda aca si lo estas creando cuando en realidad tenes que fijarte que exista...

	cantParticiones = atoi(config_get_string_value(configMetadata, "PARTITIONS"));
	//como hago con esto te lo tome siendo que es un enum? con esto toma siempre 0
	tipoConsistencia = atoi(config_get_string_value(configMetadata, "CONSISTENCY"));
	tiempoCompactacion = atoi(config_get_string_value(configMetadata, "COMPACTACION_TIME"));

	unaMetadata.cantParticiones = cantParticiones;
	unaMetadata.tipoConsistencia = tipoConsistencia;
	unaMetadata.tiempoCompactacion = tiempoCompactacion;

	free(ruta);

	return unaMetadata;


}
