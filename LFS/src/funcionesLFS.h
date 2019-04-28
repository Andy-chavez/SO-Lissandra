#include <commons/config.h>
#include <commons/log.h>
#include <commons/string.h>
#include <stdio.h>
#include <stdlib.h> //malloc,alloc,realloc
#include <string.h>

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

typedef enum {
	SC,
	SH,
	EC
}consistencia;

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
	t_config* config;
	t_log* logger;
} configYLogs;

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
	char* rutaTabla;
	particion particiones[CANTPARTICIONES]; //HAY QUE VER COMO HACER QUE DE CADA PARTICION SALGAN SUS REGISTROS.
	consistencia tipoDeConsistencia;
	metadata *metadataAsociada;
} tabla;



int verificarExistenciaTabla(char* rutaTabla);
metadata obtenerMetadata(char* nombreTabla);


int verificarExistenciaTabla(char* nombreTabla){

}

metadata obtenerMetadata(char* nombreTabla){
	t_config* configMetadata;
	int cantParticiones;
	int tiempoCompactacion;
	consistencia tipoConsistencia;
	metadata unaMetadata;
	char* str1 = "../src/Directorio/";
	char* str2 = "/metadata";

	char* ruta = (char *) malloc(1 + strlen(str1) + strlen(str2) + strlen(nombreTabla));
	strcpy(ruta, str1);
	strcat(ruta, nombreTabla);
	strcat(ruta, str2); //revisar magic string_append y string_new esto parece medio feo

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
