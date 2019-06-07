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
#include <math.h>
#include <unistd.h>


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

typedef struct {
	time_t timestamp;
	u_int16_t key;
	char* value;
} registro;
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
	t_list* listaRegistros;
} tablaMem;

typedef struct {
	char* nombre;
	t_list* temporales;
} tablaTmp;


//Funciones


int verificarExistenciaDirectorioTabla(char* nombreTabla);
metadata obtenerMetadata(char* nombreTabla); //habria que ver de pasarle la ruta de la tabla y de ahi buscar el metadata
int calcularParticion(int key,int cantidadParticiones);// Punto_Montaje/Tables/Nombre_tabla/Metadata
void agregarALista(char* timestamp,char* key,char* value,t_list* head); //este es para la lista del select
char* infoEnBloque(int key,char* numeroBloque,int sizeTabla,t_list* listaRegistros);
int largoBloque(char* numeroDeBloque); //este quizas no vaya
bool estaLaKey(int key,void* elemento);
bool esIgualAlNombre(char* nombreTabla,void * elemento);
void* devolverMayor(registro* registro1, registro* registro2);
bool agregarRegistro(char* nombreTabla, registro* unRegistro, void * elemento); //este es para memtable
registro* devolverRegistroDeMayorTimestampDeLaMemtable(t_list* listaRegistros, t_list* memtable, char* nombreTabla, int key);
void liberarDoblePuntero(char** doblePuntero);
registro* funcionSelect(char* argumentos);
void funcionInsert(char* argumentos);
void guardarInfoEnArchivo(char* ruta, char* info);
char* devolverBloqueLibre(); //devuelve el numero del bloque libre
void crearMetadata(char* ruta, char* consistenciaTabla, char* numeroParticiones, char* tiempoCompactacion);
void crearParticiones(char* ruta, int numeroParticiones); //se puede usar para los temporales.
void funcionCreate(char* argumentos);
int tamanioRegistros(char* nombreTabla);
void liberarMemtable(); //no elimina toda la memtable sino las tablas y registros de ella
int obtenerCantTemporales(char* nombreTabla);
int existeArchivo(char * filename);
void funcionDescribe(char* argumentos); //despues quizas haya que cambiar el tipo

void agregarALista(char* timestamp,char* key,char* value,t_list* head){
	registro* guardarRegistro = malloc (sizeof(registro)) ;
	guardarRegistro->timestamp = atoi(timestamp);
	guardarRegistro->key = atoi(key);
	guardarRegistro->value = value;
	list_add(head,guardarRegistro);
}

int verificarExistenciaDirectorioTabla(char* nombreTabla){
	int validacion;
	string_to_upper(nombreTabla);
	char* rutaDirectorio= string_new();
	string_append(&rutaDirectorio,puntoMontaje); //OJO ACA HAY QUE VER QUE EN EL CONFIG NO TE VENGA CON "" EL PUNTO DE MONTAJE
	string_append(&rutaDirectorio,"Tables/");
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
			list_add(tabla->listaRegistros, unRegistro);
			log_info(logger, "Se añadio registro");
			return true;
		}else{
			return false;
		}

}

//Guarda un registro en la memtable
void guardarRegistro(registro* unRegistro, char* nombreTabla) {

	bool buscarPorNombre(void *elemento){
		return agregarRegistro(nombreTabla, unRegistro, elemento);
	}

	if(!list_find(memtable, buscarPorNombre)){

		tablaMem* nuevaTabla = malloc(sizeof(tablaMem));
						nuevaTabla->nombre = string_duplicate(nombreTabla);
						nuevaTabla->listaRegistros = list_create();
						list_add(nuevaTabla->listaRegistros, unRegistro);
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

	void* liberarRegistro(registro* registro) {
		free(registro);
	}


	tablaMem* encuentraLista =  list_find(memtable, tieneElNombre);

	t_list* registrosConLaKeyEnMemtable = list_filter(encuentraLista->listaRegistros, encontrarLaKey);

	if (registrosConLaKeyEnMemtable->elements_count == 0){
		log_info(logger, "La key buscada no se encuentra la key en la memtable");
		printf("No se encuentra la key en la memtable\n");
		return NULL;
	}

	registro* registroDeMayorTimestamp= list_fold(registrosConLaKeyEnMemtable, list_get(registrosConLaKeyEnMemtable,0), cualEsElMayorTimestamp);

	log_info(logger, "Registro encontrado en la memtable");


//	list_destroy_and_destroy_elements(registrosConLaKeyEnMemtable, liberarRegistro);

return registroDeMayorTimestamp;

}

char* infoEnBloque(int key,char* numeroBloque,int sizeTabla,t_list* listaRegistros){ //pasarle el tamanio de la particion, o ver que onda (rutaTabla)
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
	string_to_upper(nombreTabla);
	t_config* configMetadata;
	int cantParticiones;
	int tiempoCompactacion;
	consistencia tipoConsistencia;
	metadata unaMetadata ;


	char* ruta = string_new();
	string_append(&ruta, puntoMontaje);
	string_append(&ruta,"Tables/");
	string_append(&ruta,nombreTabla);
	string_append(&ruta,"/Metadata");

	configMetadata = config_create(ruta);

	cantParticiones = config_get_int_value(configMetadata, "PARTITIONS");
	//como hago con esto te lo tome siendo que es un enum? con esto toma siempre 0
	tipoConsistencia = config_get_int_value(configMetadata, "CONSISTENCY"); //delegar a funcion con strcmp
	tiempoCompactacion = config_get_int_value(configMetadata, "COMPACTATION_TIME");

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

void liberarDoblePuntero(char** doblePuntero){
	int i;
	for (i=0; *(doblePuntero+i)!= NULL; i++){
			free(*(doblePuntero+i));
		}
	free(*(doblePuntero+i));

}

char* cargarInfoDeTmp(char* buffer, char* nombreTabla, int key){
		char* ruta = string_new();
		t_config* part;
		t_list* listaRegistros = list_create();
		char* bufferAux = string_new();
		bufferAux = buffer;
		void cargarTemporal(int numeroTmp){
			string_append(&ruta,puntoMontaje);
			string_append(&ruta,"Tables/");
			string_append(&ruta,nombreTabla);
			string_append(&ruta,"/"); //vamos a usar la convension PartN.bin
			string_append(&ruta,string_itoa(numeroTmp));
			string_append(&ruta,".tmp");
			part = config_create(ruta);
			char** arrayDeBloques = config_get_array_value(part,"BLOCKS");
			int sizeParticion=config_get_int_value(part,"SIZE");
			int i = 0;

			while(*(arrayDeBloques+i)!= NULL){
					char* informacion = infoEnBloque(key,*(arrayDeBloques+i),sizeParticion,listaRegistros);
					string_append(&bufferAux, informacion);
					i++;
				}
			free(ruta);
			liberarDoblePuntero(arrayDeBloques);
			config_destroy(part);

		}

		return bufferAux;
}


registro* funcionSelect(char* argumentos){ //en la pos 0 esta el nombre y en la segunda la key
	char** argSeparados = string_n_split(argumentos,2," ");
	char* particion;
	t_config* part;
	char* ruta = string_new();
	t_list* listaRegistros = list_create();
	int key = atoi(*(argSeparados+1));
	registro* registroBuscado;
	bool encontrarLaKey(void *elemento){
			return estaLaKey(key, elemento);
		}


	void* liberarRegistro(registro* registro) {
			free(registro);
		}



	void* cualEsElMayorTimestamp(void *elemento1, void *elemento2){
		registro* primerElemento = elemento1;
		registro* segundoElemento = elemento2;

		return devolverMayor(primerElemento, segundoElemento);

	}
	if(verificarExistenciaDirectorioTabla(*(argSeparados+0)) ==0)
		{
			log_error(logger,"No se pudo completar el select");
			return NULL;
		}
	else{
		log_info(logger, "Directorio de tabla valido");

		metadata metadataTabla = obtenerMetadata(*(argSeparados+0));

		log_info(logger, "Metadata cargado");

		particion = string_itoa(calcularParticion(key,metadataTabla.cantParticiones)); //cant de particiones de la tabla
		char* buffer = string_new();
		string_append(&ruta,puntoMontaje);
		string_append(&ruta,"Tables/");
		string_append(&ruta,*(argSeparados+0));
		string_append(&ruta,"/part"); //vamos a usar la convension PartN.bin
		string_append(&ruta,particion);
		string_append(&ruta,".bin");
		part = config_create(ruta);


		char** arrayDeBloques = config_get_array_value(part,"BLOCKS");
		int sizeParticion=config_get_int_value(part,"SIZE");

		//ver si esta vacio que no haga esto desde infoenbloque
	/*
		while(*(arrayDeBloques+i)!= NULL){
			char* informacion = infoEnBloque(key,*(arrayDeBloques+i),sizeParticion,listaRegistros);
			string_append(&buffer, informacion);
			i++;
		}
	*/
		char* bufferFinal = cargarInfoDeTmp(buffer, *(argSeparados+0), key);


		log_info(logger, "Informacion de bloques cargada");

		char** separarRegistro = string_split(bufferFinal,"\n");
		int j =0;
		for(j=0;*(separarRegistro+j)!=NULL;j++){
			char **aCargar =string_split(*(separarRegistro+j),";");
			agregarALista(*(aCargar+0),*(aCargar+1),*(aCargar+2),listaRegistros);
			free(*(aCargar+0));
			free(*(aCargar+1));
			free(*(aCargar+2));
		}

		//habria que hacer el mismo while si hay temporales if(hayTemporales) habria que ver el tema de cuantos temporales hay, quizas convendria agregarlo en el metadata tipo array
		//puts(buffer);
		//y aca afuera haria la busqueda del registro.

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

		liberarDoblePuntero(arrayDeBloques);
		liberarDoblePuntero(separarRegistro);
		liberarDoblePuntero(argSeparados);

		config_destroy(part);
		free (ruta);
		list_destroy_and_destroy_elements(listaRegistros, liberarRegistro);
		free(buffer);

		//ver si la funcion tiene que devolver el registro
		printf("Registro seleccionado: %s \n",registroBuscado->value);
		}
		return registroBuscado;

}


void funcionInsert(char* argumentos) {

	//verificar que no se exceda el tamaño del value, tamanioValue (var. global
	char** argSeparados = string_n_split(argumentos,3,"\"");
	char* nombreYKey = *(argSeparados + 0);
	char** separarNombreYKey = string_n_split(nombreYKey, 2, " ");
	char* nombreTabla = *(separarNombreYKey + 0);
	int key = atoi(*(separarNombreYKey + 1));
	char* value = *(argSeparados + 1);

	char* valorTimestamp = *(argSeparados + 2);
	int timestamp;

	if (!verificarExistenciaDirectorioTabla(nombreTabla)) return;
	log_info(logger, "Directorio de tabla valido");

	if (valorTimestamp == NULL) {
		timestamp = (unsigned long)time(NULL);
	}else{
		timestamp = atoi(valorTimestamp);
	}

//	obtenerMetadata(nombreTabla);

	registro* registroDePrueba = malloc(sizeof(registro));
				registroDePrueba -> key = key;
				registroDePrueba -> value= string_duplicate(value);
				registroDePrueba -> timestamp = timestamp;

 guardarRegistro(registroDePrueba, nombreTabla);
  log_info(logger, "Se guardo el registro");

  liberarDoblePuntero(argSeparados);

}



void printearBitmap(){

	int j;
	for(j=0; j<cantDeBloques; j++){
		bool bit = bitarray_test_bit(bitarray, j);
		printf("%i \n", bit);
	}

}

void guardarInfoEnArchivo(char* ruta, char* info){
	FILE *fp = fopen(ruta, "w");
	if (fp != NULL){
		fputs(info, fp);
		fclose(fp);
	}
}

char* devolverBloqueLibre(){
	int i;

	int bloqueEncontrado = 0;
	int encontroBloque = 0;
	char* numero;

	for(i=0; i < 64; i++){
		bool bit = bitarray_test_bit(bitarray, i);

		if(bit == 0){
			encontroBloque = 1;
			bloqueEncontrado = i;
			break;
		}

	}

	if (encontroBloque == 1){
		bitarray_set_bit(bitarray, bloqueEncontrado);
		numero = string_itoa(bloqueEncontrado);
	}
	return numero;
}

void crearMetadata(char* ruta, char* consistenciaTabla, char* numeroParticiones, char* tiempoCompactacion) {

	char* infoDelMetadata = string_new();
	char* rutaMetadata = string_new();

	string_append(&infoDelMetadata, "CONSISTENCY=");
	string_append(&infoDelMetadata, consistenciaTabla);
	string_append(&infoDelMetadata, "\n");
	string_append(&infoDelMetadata, "PARTITIONS=");
	string_append(&infoDelMetadata, numeroParticiones);
	string_append(&infoDelMetadata, "\n");
	string_append(&infoDelMetadata, "COMPACTATION_TIME=");
	string_append(&infoDelMetadata, tiempoCompactacion);

	FILE *archivoMetadata;
	string_append(&rutaMetadata, ruta);
	string_append(&rutaMetadata, "/metadata.bin");


	guardarInfoEnArchivo(rutaMetadata, infoDelMetadata);


	free(infoDelMetadata);
	free(rutaMetadata);
}

void crearParticiones(char* ruta, int numeroParticiones) {

	char* bloqueLibre;

	for (int i= 0; i < numeroParticiones; i++){

		FILE* particion;
		char * rutaDeLaParticion = string_new();
		char* infoAGuardar = string_new();
		char* numeroParticion = string_itoa(i);

		string_append(&rutaDeLaParticion, ruta);
		string_append(&rutaDeLaParticion, "/part");
		string_append(&rutaDeLaParticion, numeroParticion);
		string_append(&rutaDeLaParticion, ".bin");

		string_append(&infoAGuardar, "SIZE=");
		//aca no se bien que va al principio, por ahora le dejo 0 pero creo que cuando empezas con 1 bloque es el block size pero en string
		string_append(&infoAGuardar, "0");
		string_append(&infoAGuardar, "\n");
		string_append(&infoAGuardar, "BLOCKS=[");
		bloqueLibre = devolverBloqueLibre();
		string_append(&infoAGuardar, bloqueLibre);
		string_append(&infoAGuardar, "]");

//		particion = fopen(rutaDeLaParticion, "w");

		guardarInfoEnArchivo(rutaDeLaParticion, infoAGuardar);
		//fputs(infoAGuardar, particion);

		free(infoAGuardar);
		free(rutaDeLaParticion);

	}

}

void funcionCreate(char* argumentos) {


	char** argSeparados = string_n_split(argumentos,4," ");

	//es todo char porque cuando lo guardes en el metadata se guarda como caracteres
	char* nombreTabla = *(argSeparados + 0);
	string_to_upper(nombreTabla); //para que nos quede en el file system todo en mayuscula
    char* consistenciaTabla = *(argSeparados + 1);
	char* numeroParticiones = *(argSeparados + 2);
	char* tiempoCompactacion = *(argSeparados + 3);

	//hay que liberarlo
	char* directorioTabla = string_new();
	//falta ver que no exista una tabla del mismo nombre
	if (!verificarExistenciaDirectorioTabla(nombreTabla)){

		string_append(&directorioTabla, puntoMontaje);
		string_append(&directorioTabla, "Tables/");
		string_append(&directorioTabla, nombreTabla);

		mkdir(directorioTabla, 0777);

	}else{
		puts("ya existe");
	}


	crearMetadata(directorioTabla, consistenciaTabla, numeroParticiones, tiempoCompactacion);
	int cantidadParticiones = atoi(numeroParticiones);
	crearParticiones(directorioTabla, cantidadParticiones);
	free(directorioTabla);

}

//hacerlo por tabla!! y que reciba el nombre de la tabla
int tamanioRegistros(char* nombreTabla){

	int tamanioTotal = 0;
	int i = 0;

	bool tieneElNombre(void *elemento){
		return esIgualAlNombre(nombreTabla, elemento);
	}

	void* sumarRegistros(int valor , registro* registro ){
		tamanioTotal = tamanioTotal + sizeof(registro->key)  + sizeof(registro->timestamp) + (strlen(registro->value) + 1);
	}


	registro* registroDePrueba = malloc(sizeof(registro));
				registroDePrueba -> key = 13;
				registroDePrueba -> value= string_duplicate("eloooooooooooooo");
				registroDePrueba -> timestamp = 8000;
    tamanioTotal = tamanioTotal + sizeof(registroDePrueba->key) + sizeof(registroDePrueba->timestamp) + (strlen(registroDePrueba->value) + 1);
		registro* registroDePrueba2 = malloc(sizeof(registro));
				  registroDePrueba2 -> key = 56;
				  registroDePrueba2 -> value= string_duplicate("ghj");
				  registroDePrueba2 -> timestamp = 1548421509;
	tamanioTotal = tamanioTotal + sizeof(registroDePrueba2->key) + sizeof(registroDePrueba2->timestamp) + (strlen(registroDePrueba2->value) + 1);
		registro* registroDePrueba3 = malloc(sizeof(registro));
				  registroDePrueba3 -> key = 13;
				  registroDePrueba3 -> value= string_duplicate("aloo");
				  registroDePrueba3 -> timestamp = 9000;

		tablaMem* tablaDePrueba = malloc(sizeof(tablaMem));
				tablaDePrueba-> nombre = string_duplicate("TABLA1");
				tablaDePrueba->listaRegistros = list_create();

				list_add(tablaDePrueba->listaRegistros, registroDePrueba);
				list_add(tablaDePrueba->listaRegistros, registroDePrueba2);

		tablaMem* tablaDePrueba2 = malloc(sizeof(tablaMem));
				  tablaDePrueba2->nombre = string_duplicate("tablaB");
				  tablaDePrueba2->listaRegistros = list_create();

		list_add(tablaDePrueba2->listaRegistros, registroDePrueba3);
		tamanioTotal = tamanioTotal + sizeof(registroDePrueba3->key) + sizeof(registroDePrueba3->timestamp) + (strlen(registroDePrueba3->value) + 1);
		list_add(memtable, tablaDePrueba);
		list_add(memtable, tablaDePrueba2);

		tamanioTotal = 0;

	tablaMem* encuentraTabla =  list_find(memtable, tieneElNombre);

	list_fold(encuentraTabla->listaRegistros, 0, sumarRegistros);

return tamanioTotal;
}

void liberarTabla(tablaMem* tabla) {

	void liberarRegistros(registro* unRegistro) {
//		free(unRegistro->value);
		free(unRegistro);

	}

	free(tabla->nombre);
	list_destroy_and_destroy_elements(tabla->listaRegistros,(void*) liberarRegistros);
	free(tabla);
}

void liberarMemtable() { //no elimina toda la memtable sino las tablas y registros de ella

	list_destroy_and_destroy_elements(memtable,(void*) liberarTabla);
}
int obtenerCantTemporales(char* nombreTabla){ //SIRVE PARA DUMP(TE DEVUELVE EL NUMERO A ESCRIBIR)
											//REUTILIZAR EN COMPACTACION
	//puntoMontaje/Tables/TABLA1/1.tmp, suponemos que los temporales se hacen en orden
	int cantTemporal = 0;
	int existe;
	do{

		char* ruta = string_new();
		string_append(&ruta,puntoMontaje);
		string_append(&ruta,"Tables/");
		string_append(&ruta,nombreTabla);
		string_append(&ruta,"/");
		string_append(&ruta,string_itoa(cantTemporal));
		string_append(&ruta,".tmp");
		existe = existeArchivo(ruta);
		if(existe==0) break;
		free(ruta);
		cantTemporal++;
	}while(existe!=0);
	return cantTemporal;
}

int existeArchivo(char * filename){
    FILE *file;
    if (file = fopen(filename, "r")){
        fclose(file);
        return 1;
    }
    return 0;
}

void funcionDescribe(char* argumentos) {
	metadata metadataBuscado;
	//if(argumentos==NULL){} //seria el describe all argumentos==NULL
	if(1){
		if(verificarExistenciaDirectorioTabla(argumentos)){
		metadataBuscado = obtenerMetadata(argumentos);
		pthread_mutex_lock(&mutexLogger);
		log_info(logger,"Se encontro el archivo metadata de la tabla con los siguientes valores: Consistency= ", metadataBuscado.tipoConsistencia,"Partitions= ",metadataBuscado.cantParticiones,"Compaction_Time= ", metadataBuscado.tiempoCompactacion);
		pthread_mutex_unlock(&mutexLogger);
		}
	}
}

