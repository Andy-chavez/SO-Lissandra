#include <commons/config.h>
#include <commons/log.h>
#include <commons/string.h>
#include <commons/collections/list.h>
#include <stdio.h>
#include <stdlib.h> //malloc,alloc,realloc
#include <string.h>
#include <unistd.h> //mirar clave ACCESS para verificar existencia
#include <sys/stat.h> //para ver si existe un directorio
#include "configuraciones.h"
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/io.h>
#include <fcntl.h>

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
	INSERT,
	CREATE,
	DESCRIBETABLE,
	DESCRIBEALL,
	DROP,
	JOURNAL,
	SELECT
} operacion;

typedef enum {
	SC,
	SH,
	EC
}consistencia;


typedef struct {
	time_t timestamp;
	u_int16_t key;
	char* value;
} registro;

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
typedef struct {
	char* nombre;
	t_list* lista;
} tablaMem;



//t_log* g_logger = log_create("lisandra.log", "LISANDRA", 1, LOG_LEVEL_ERROR);
//t_config* g_config= config_create("LISANDRA.CONFIG"); //Por ahora lo dejo global deberiamos ver despues, y habria que liberarlos


//Funciones



int verificarExistenciaDirectorioTabla(char* nombreTabla);
metadata obtenerMetadata(char* nombreTabla); //habria que ver de pasarle la ruta de la tabla y de ahi buscar el metadata
int calcularParticion(int key,int cantidadParticiones);// Punto_Montaje/Tables/Nombre_tabla/Metadata
registro devolverRegistroDelFileSystem(int key,int particion,char* nombreTabla);
int buscarEnBloque(int key,char* numeroDeBloque); //cambiar despues de nuevo a registro buscarEnBloque
void agregarALista(char* timestamp,char* key,char* value,t_list* head);
void buscarEnBloque2(int key,char* numeroBloque,int sizeTabla);
void parserGeneral(char* operacionAParsear,char* argumentos);


//crearTabla(char* ruta){
//
//}

void agregarALista(char* timestamp,char* key,char* value,t_list* head){
	registro* guardarRegistro = malloc (sizeof(registro)) ;
	guardarRegistro->timestamp = atoi(timestamp);
	guardarRegistro->key = atoi(key);
	guardarRegistro->value = malloc (sizeof(tamanioValue));
	guardarRegistro->value = value;
	list_add(head,guardarRegistro);
	puts(guardarRegistro->value);
}

void buscarEnBloque2(int key,char* numeroBloque,int sizeTabla){ //pasarle el tamanio de la particion, o ver que onda (rutaTabla)
	//ver que agarre toda la info de los bloques correspondientes a esa tabla
	char* rutaBloque = string_new();
	string_append(&rutaBloque,puntoMontaje);
	string_append(&rutaBloque,"Bloques/");
	string_append(&rutaBloque,numeroBloque);
	string_append(&rutaBloque,".bin");
	int archivo = open(rutaBloque,O_RDWR);
	t_list* listaRegistros;
	// []

	struct stat sb;
	fstat(archivo,&sb);
	//while para leer todos los bloques
	char* informacion = mmap(NULL,tamanioBloques,PROT_READ,MAP_PRIVATE,archivo,0);
	//char* informacion = mmap(NULL,sb.st_size,PROT_READ,MAP_PRIVATE,archivo,0);
	//parsear informacion
	char** separarRegistro = string_split(informacion,"\n");
//	int length = sizeof(separarRegistro)/sizeof(separarRegistro[0]);
	int i;
	for(i=0;*(separarRegistro+i)!=NULL;i++){
		char **aCargar =string_split(*(separarRegistro+i),";");
		agregarALista(*(aCargar+0),*(aCargar+1),*(aCargar+2),listaRegistros);
	}
	//list_find()
	free(rutaBloque);
	//free(informacion);
}

int buscarEnBloque(int key,char* numeroDeBloque){ //despues agregar argumento para config y log
	char* rutaBloque = string_new();
	int tam=0;
	long posicionArchivo;
	char* buffer= (char*) malloc(sizeof(char)*100);
	string_append(&rutaBloque,puntoMontaje);
	string_append(&rutaBloque,"Bloques/");
	string_append(&rutaBloque,numeroDeBloque);
	string_append(&rutaBloque,".bin");
	registro *registroBloque = malloc(sizeof(registro));
	//registroBloque->value = malloc (sizeof(char)*100);
	FILE* archivo = fopen(rutaBloque,"rb");
	if (archivo == NULL)
		{
			log_info(logger,"No se pudo abrir el bloque %s",numeroDeBloque);
			return -1;
		}

	while(fread(&(registroBloque->timestamp), sizeof(time_t), 1, archivo)){
		fread(&(registroBloque->key), sizeof(u_int16_t), 1, archivo);
		posicionArchivo = ftell(archivo); //para usar despues
		fread((buffer+tam),sizeof(char),1,archivo);
		//*(buffer+tam) = getc(archivo);

		while(*(buffer+tam)!= '\n'){ //tomo como valor centinela por ahora la '*'
			//unica manera que se me ocurre por ahora para guardar en algo que es variable el tamanio
			tam++;
			fread((buffer+tam),sizeof(char),1,archivo);
		}
		tam ++;
//		strcpy(registroBloque->value,buffer);
		if(registroBloque->key== key){
			fseek(archivo, posicionArchivo, SEEK_SET ); //volver atras a la posicion donde empieza el value
			registroBloque->value = malloc (sizeof(char)*tam);
			fread(registroBloque->value, sizeof(char)*tam, 1, archivo);
			//apenas aca cargate el value
			puts("lo encontre");
			puts(registroBloque->value);
			//agregarAListaSelect(registroBloque); probablemente hacer algo asi porque son las de la memtable,la de los bloques y de los temporales.
			break;
		}
		for(int i=0;i<100;i++) *(buffer+i) = '\0'; //libero buffer
		tam = 0;
	}
	//puts("no lo encontre");
	free(buffer);
	fclose(archivo);
	free(rutaBloque);
	return 1;

}

int verificarExistenciaDirectorioTabla(char* nombreTabla){
	int validacion;
	string_to_upper(nombreTabla);
	char* rutaDirectorio= string_new();
	string_append(&rutaDirectorio,puntoMontaje); //OJO ACA HAY QUE VER QUE EN EL CONFIG NO TE VENGA CON "" EL PUNTO DE MONTAJE
	string_append(&rutaDirectorio,"Tables/"); //habria que ver esto, es lo mejor que se me ocurrio porque en el select solo te dan el nombre
	string_append(&rutaDirectorio,nombreTabla);
	struct stat sb;
	//pthread_mutex_lock(&mutexLog);
	log_info(logger,"Determinando existencia de tabla en la ruta: %s",rutaDirectorio);
	    if (stat(rutaDirectorio, &sb) == 0 && S_ISDIR(sb.st_mode))
	    {
	    	log_info(logger,"La tabla existe en el FS");
	    	//pthread_mutex_unlock(&mutexLog); revisar tema semaforos creo que me esta quedando muy grande la region critica,nose si son tan necesarios en este caso
	    	printf("existe la tabla en la direccion");
	    	validacion=1;
	    }
	    else
	    {
	    	log_info(logger,"Error no existe tabla en ruta indicada %s \n",rutaDirectorio);
	    	//pthread_mutex_unlock(&mutexLog);
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

void* devolverMayor(registro* registro1, registro* registro2){
	if (registro1->timestamp > registro2->timestamp){
			return registro1;
		}else{
			return registro2;
		}
}

registro* devolverRegistroDeLaMemtable(t_list* memtable, char* nombreTabla, int key){

	//esto no va por cada procedimiento obviamente, primero termino este par de funciones y ya lo pongo para q sea global

	bool encontrarLaKey(void *elemento){
		return estaLaKey(key, elemento);
	}

	bool tieneElNombre(void *elemento){
		return esIgualAlNombre(nombreTabla, elemento);
	}

	void* cualEsElMayor(void *elemento1, void *elemento2){
		registro* primerElemento = elemento1;
		registro* segundoElemento = elemento2;

		return devolverMayor(primerElemento, segundoElemento);

	}

	registro* registroDePrueba = malloc(sizeof(registro));
			registroDePrueba -> key = 13;
			registroDePrueba -> value= string_duplicate("aloo");
			registroDePrueba -> timestamp = 8000;

	registro* registroDePrueba2 = malloc(sizeof(registro));
			  registroDePrueba2 -> key = 13;
			  registroDePrueba2 -> value= string_duplicate("aloo");
			  registroDePrueba2 -> timestamp = 10000;

	registro* registroDePrueba3 = malloc(sizeof(registro));
			  registroDePrueba3 -> key = 13;
			  registroDePrueba3 -> value= string_duplicate("aloo");
			  registroDePrueba3 -> timestamp = 9000;

	tablaMem* tablaDePrueba = malloc(sizeof(tablaMem));
			tablaDePrueba-> nombre = string_duplicate("tablaA");
			tablaDePrueba->lista = list_create();

			list_add(tablaDePrueba->lista, registroDePrueba);
			list_add(tablaDePrueba->lista, registroDePrueba2);

	tablaMem* tablaDePrueba2 = malloc(sizeof(tablaMem));
			  tablaDePrueba2->nombre = string_duplicate("tablaB");
			  tablaDePrueba2->lista = list_create();

	list_add(tablaDePrueba2->lista, registroDePrueba);
	list_add(memtable, tablaDePrueba);
	list_add(memtable, tablaDePrueba2);

	tablaMem* encuentraLista =  list_find(memtable, tieneElNombre);

	t_list* registrosConLaKey = list_filter(encuentraLista->lista, encontrarLaKey);

	//bilardo se sentiria orgulloso(?, pero no se me ocurrio otra forma por ahora de plantear la semilla
	registro* registroDeMayorTimestamp = list_fold(registrosConLaKey, list_get(registrosConLaKey,0), cualEsElMayor);

//	printf("Esto no va a funcionar a la primera: %ld \n",registroDeMayorTimestamp->timestamp);
//	printf("No se ha encontrado el directorio de la tabla en la ruta: %d \n",registroEncontrado->key);

	return registroDeMayorTimestamp;

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

void funcionSelect(char* argumentos){ //en la pos 0 esta el nombre y en la segunda la key
	char** argSeparados = string_n_split(argumentos,2," ");
	int particion;
	int key = atoi(*(argSeparados+0));
	//metadata *metadataTabla = malloc (sizeof(metadata));
	if(verificarExistenciaDirectorioTabla(*(argSeparados+0)) ==0) return; //primero verificas existencia
	metadata metadataTabla = obtenerMetadata(*(argSeparados+0));
	puts(metadataTabla.cantParticiones);
	particion=calcularParticion(key,3);

}
