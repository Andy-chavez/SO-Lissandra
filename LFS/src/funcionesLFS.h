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
char* buscarEnBloque2(int key,char* numeroBloque,int sizeTabla,t_list* listaRegistros);
void parserGeneral(char* operacionAParsear,char* argumentos);
int largoBloque(char* numeroDeBloque);


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
	    	printf("existe la tabla en el directorio\n");
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


bool agregarRegistro(char* nombreTabla, registro* unRegistro, void * elemento){
		tablaMem* tabla = elemento;

		if (string_equals_ignore_case(tabla->nombre, nombreTabla)){
			list_add(tabla->lista, unRegistro);
			log_info(logger, "Se añadio registro");
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

	if(!list_find(memtable, buscarPorNombre)){

		tablaMem* nuevaTabla = malloc(sizeof(tablaMem));
						nuevaTabla->nombre = string_duplicate(nombreTabla);
						nuevaTabla->lista = list_create();
						list_add(nuevaTabla->lista, unRegistro);
						list_add(memtable, nuevaTabla);
						log_info(logger, "Se añadio la tabla a la memtable");
	}



}


registro* devolverRegistroDeMayorTimestampDeLaMemtable(t_list* listaRegistros, t_list* memtable, char* nombreTabla, int key){


	bool encontrarLaKey(void *elemento){
		return estaLaKey(key, elemento);
	}

	void* cualEsElMayorTimestamp(void *elemento1, void *elemento2){
			registro* primerElemento = elemento1;
			registro* segundoElemento = elemento2;
			return devolverMayor(primerElemento, segundoElemento);
	}

	bool tieneElNombre(void *elemento){
	return esIgualAlNombre(nombreTabla, elemento);
	}

	registro* registroDePrueba = malloc(sizeof(registro));
			registroDePrueba -> key = 13;
			registroDePrueba -> value= string_duplicate("aloo");
			registroDePrueba -> timestamp = 8000;

	registro* registroDePrueba2 = malloc(sizeof(registro));
			  registroDePrueba2 -> key = 56;
			  registroDePrueba2 -> value= string_duplicate("aloo");
			  registroDePrueba2 -> timestamp = 1548421509;

	registro* registroDePrueba3 = malloc(sizeof(registro));
			  registroDePrueba3 -> key = 13;
			  registroDePrueba3 -> value= string_duplicate("aloo");
			  registroDePrueba3 -> timestamp = 9000;

	tablaMem* tablaDePrueba = malloc(sizeof(tablaMem));
			tablaDePrueba-> nombre = string_duplicate("TABLA1");
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

	t_list* registrosConLaKeyEnMemtable = list_filter(encuentraLista->lista, encontrarLaKey);

	if (registrosConLaKeyEnMemtable->elements_count == 0){
		log_info(logger, "La key buscada no se encuentra la key en la memtable");
		printf("No se encuentra la key en la memtable\n");
		return NULL;
	}

	registro* registroDeMayorTimestamp= list_fold(registrosConLaKeyEnMemtable, list_get(registrosConLaKeyEnMemtable,0), cualEsElMayorTimestamp);

	log_info(logger, "Registro encontrado en la memtable");

return registroDeMayorTimestamp;

}

char* buscarEnBloque2(int key,char* numeroBloque,int sizeTabla,t_list* listaRegistros){ //pasarle el tamanio de la particion, o ver que onda (rutaTabla)
	//ver que agarre toda la info de los bloques correspondientes a esa tabla

	/*bool encontrarLaKey(void *elemento){
			return estaLaKey(key, elemento);
		}

	void* cualEsElMayorTimestamp(void *elemento1, void *elemento2){
		registro* primerElemento = elemento1;
		registro* segundoElemento = elemento2;

		return devolverMayor(primerElemento, segundoElemento);

	}*/
	char* rutaBloque = string_new();
	string_append(&rutaBloque,puntoMontaje);
	string_append(&rutaBloque,"Bloques/");
	string_append(&rutaBloque,numeroBloque);
	string_append(&rutaBloque,".bin");
	int archivo = open(rutaBloque,O_RDWR);
	//char* informacion = malloc(tamanioBloques);
	//fread(informacion,tamanioBloques,1,archivo);
	char* informacion = mmap(NULL,tamanioBloques,PROT_READ,MAP_PRIVATE,archivo,NULL);
	//char* informacion = mmap(NULL,sb.st_size,PROT_READ,MAP_PRIVATE,archivo,0);
	//parsear informacion

//	t_list* registrosDeLaTabla = list_filter(listaRegistros, encontrarLaKey);
//
//	registro* registroADevolver = list_fold(registrosDeLaTabla, list_get(registrosDeLaTabla,0), cualEsElMayorTimestamp);

	//puts(informacion);
	free(rutaBloque);
	return informacion;
}



metadata obtenerMetadata(char* nombreTabla){
	t_config* configMetadata;
	int cantParticiones;
	int tiempoCompactacion;
	consistencia tipoConsistencia;
	metadata unaMetadata;
	char* str2 = "/metadata";

	char* ruta = string_new();
	string_append(&ruta, puntoMontaje);
	string_append(&ruta,"Tables/");
	string_append(&ruta,nombreTabla);
	string_append(&ruta,str2);

	configMetadata = config_create(ruta);

	cantParticiones = atoi(config_get_string_value(configMetadata, "PARTITIONS"));
	//como hago con esto te lo tome siendo que es un enum? con esto toma siempre 0
	tipoConsistencia = atoi(config_get_string_value(configMetadata, "CONSISTENCY")); //delegar a funcion con strcmp
	tiempoCompactacion = atoi(config_get_string_value(configMetadata, "COMPACTION_TIME"));

	unaMetadata.cantParticiones = cantParticiones;
	unaMetadata.tipoConsistencia = tipoConsistencia;
	unaMetadata.tiempoCompactacion = tiempoCompactacion;

	config_destroy(configMetadata);
	free(ruta);

	return unaMetadata;


}

int largoBloque(char* numeroBloque){
	char* rutaBloque = string_new();
	string_append(&rutaBloque,puntoMontaje);
	string_append(&rutaBloque,"Bloques/");
	string_append(&rutaBloque,numeroBloque);
	string_append(&rutaBloque,".bin");
	struct stat sb;
	int archivo = open(rutaBloque,O_RDWR);
	fstat(archivo,&sb);
	return sb.st_size+1;
}

registro* funcionSelect(char* argumentos){ //en la pos 0 esta el nombre y en la segunda la key
	char** argSeparados = string_n_split(argumentos,2," ");
	int i=0;
	char* particion;
	t_config* part;
	char* ruta = string_new();
	t_list* listaRegistros = list_create();
	int key = atoi(*(argSeparados+1));
	int desplazamiento=0;
	bool encontrarLaKey(void *elemento){
			return estaLaKey(key, elemento);
		}

	void* cualEsElMayorTimestamp(void *elemento1, void *elemento2){
		registro* primerElemento = elemento1;
		registro* segundoElemento = elemento2;

		return devolverMayor(primerElemento, segundoElemento);

	}
	if(verificarExistenciaDirectorioTabla(*(argSeparados+0)) ==0) return NULL; //primero verificas existencia, ver despues de si es NULL de tirar/enviar error

	log_info(logger, "Directorio de tabla valido");

	metadata metadataTabla = obtenerMetadata(*(argSeparados+0));

	log_info(logger, "Metadata cargado");

	particion = string_itoa(calcularParticion(key,metadataTabla.cantParticiones)); //cant de particiones de la tabla
	string_append(&ruta,puntoMontaje);
	string_append(&ruta,"Tables/");
	string_append(&ruta,*(argSeparados+0));
	string_append(&ruta,"/Part"); //vamos a usar la convension PartN.bin
	string_append(&ruta,particion);
	string_append(&ruta,".bin");
	part = config_create(ruta);
	char** arrayDeBloques = config_get_array_value(part,"BLOCKS");
	int sizeParticion=config_get_int_value(part,"SIZE");
	//char* buffer = malloc (sizeof(char)*sizeParticion);
	char* buffer = string_new();
	int largoDeBloque;
//	int bloquesLeidos=0;
	while(*(arrayDeBloques+i)!= NULL){
		char* informacion = buscarEnBloque2(key,*(arrayDeBloques+i),sizeParticion,listaRegistros);
		string_append(&buffer, informacion);
		//largoDeBloque = largoBloque(*(arrayDeBloques+i));
		//memcpy(buffer+desplazamiento,informacion,largoDeBloque);
		//desplazamiento += largoDeBloque;
		i++;
	}

	log_info(logger, "Informacion de bloques cargada");

	char** separarRegistro = string_split(buffer,"\n");
	int j =0;
	for(j=0;*(separarRegistro+j)!=NULL;j++){
		char **aCargar =string_split(*(separarRegistro+j),";");
		agregarALista(*(aCargar+0),*(aCargar+1),*(aCargar+2),listaRegistros);
	}
	//habria que hacer el mismo while si hay temporales if(hayTemporales) habria que ver el tema de cuantos temporales hay, quizas convendria agregarlo en el metadata tipo array
	//puts(buffer);
	//y aca afuera haria la busqueda del registro.

	registro* registroBuscado;

	if (!(registroBuscado = devolverRegistroDeMayorTimestampDeLaMemtable(listaRegistros, memtable,*(argSeparados+0), key))){
		log_info(logger, "El registro no se encuentra en la memtable");
		t_list* registrosConLaKeyEnListaRegistros = list_filter(listaRegistros, encontrarLaKey);
		if (registrosConLaKeyEnListaRegistros->elements_count == 0){
			log_info(logger, "La key buscada no se encuentra");
			printf("No se encuentra la key\n");
				return NULL;
			}
	registroBuscado= list_fold(registrosConLaKeyEnListaRegistros, list_get(registrosConLaKeyEnListaRegistros,0), cualEsElMayorTimestamp);
	log_info(logger, "Registro encontrado en bloques");
	}

	//if((devolverRegistroDeMayorTimestampYAgregarALista(listaRegistros, memtable,*(argSeparados+0), key)) == 0) return NULL;
	config_destroy(part);
	free (ruta);
	list_clean(listaRegistros);
	free(buffer);
	return registroBuscado;
}

void* funcionInsert(char* argumentos) {
	char** argSeparados = string_n_split(argumentos,4," ");

	char* nombreTabla = *(argSeparados + 0);

	if (!verificarExistenciaDirectorioTabla(nombreTabla)) return NULL;
	log_info(logger, "Directorio de tabla valido");
	int key = atoi(*(argSeparados+1));
	char* value = *(argSeparados + 2);
	int timestamp = *(argSeparados + 3);

	if (timestamp == 0) timestamp = (unsigned long)time(NULL);

	obtenerMetadata(nombreTabla);

	registro* registroDePrueba = malloc(sizeof(registro));
				registroDePrueba -> key = key;
				registroDePrueba -> value= string_duplicate(value);
				registroDePrueba -> timestamp = timestamp;

  guardarRegistro(memtable, registroDePrueba, nombreTabla);
  log_info(logger, "Se guardo el registro");
}


