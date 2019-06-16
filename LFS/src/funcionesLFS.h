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
#include <commonsPropias/conexiones.h>
#include <commonsPropias/serializacion.h>
#include <sys/types.h>
#include <dirent.h>



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

typedef enum {
	INSERT,
	CREATE,
	DESCRIBETABLE,
	DESCRIBEALL,
	DROP,
	JOURNAL,
	SELECT
} operacion;
registroConNombreTabla* registroError; 
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
metadata* obtenerMetadata(char* nombreTabla); //habria que ver de pasarle la ruta de la tabla y de ahi buscar el metadata
int calcularParticion(int key,int cantidadParticiones);// Punto_Montaje/Tables/Nombre_tabla/Metadata
void agregarALista(char* timestamp,char* key,char* value,t_list* head); //este es para la lista del select
char* infoEnBloque(char* numeroBloque,int sizeTabla);
int largoBloque(char* numeroDeBloque); //este quizas no vaya
bool estaLaKey(int key,void* elemento);
bool esIgualAlNombre(char* nombreTabla,void * elemento);
void* devolverMayor(registro* registro1, registro* registro2);
bool agregarRegistro(char* nombreTabla, registro* unRegistro, tablaMem * tabla); //este es para memtable
registro* devolverRegistroDeMayorTimestampDeLaMemtable(t_list* listaRegistros, t_list* memtable, char* nombreTabla, int key);
void liberarDoblePuntero(char** doblePuntero);
void funcionSelect(char* argumentos,int socket);
void funcionInsert(char* argumentos,int socket);
void guardarInfoEnArchivo(char* ruta, char* info);
char* devolverBloqueLibre(); //devuelve el numero del bloque libre
void crearMetadata(char* ruta, char* consistenciaTabla, char* numeroParticiones, char* tiempoCompactacion);
void crearParticiones(char* ruta, int numeroParticiones); //se puede usar para los temporales.
void funcionCreate(char* argumentos,int socket);
int tamanioRegistros(char* nombreTabla);
void liberarMemtable(); //no elimina toda la memtable sino las tablas y registros de ella
int obtenerCantTemporales(char* nombreTabla);
int existeArchivo(char * filename);
void funcionDescribe(char* argumentos,int socket); //despues quizas haya que cambiar el tipo
void enviarYLogearMensajeError(int socket, char* mensaje);
void enviarOMostrarYLogearInfo(int socket, char* mensaje);
void enviarYOLogearAlgo(int socket, char *mensaje, void(*log)(t_log *, char *));
void inicializarRegistroError();

void inicializarRegistroError(){
	registroError = malloc(sizeof(registro));
	registroError->timestamp = 1;
	registroError->key = 1;
	registroError->value = string_duplicate("Error");
	registroError->nombreTabla = string_duplicate("1");
}



void enviarYOLogearAlgo(int socket, char *mensaje, void(*log)(t_log *, char *)){
	if(socket != -1) {
		pthread_mutex_lock(&mutexLogger);
		log(logger, mensaje);
		pthread_mutex_unlock(&mutexLogger);
		enviar(socket, mensaje, strlen(mensaje) + 1);
	} else {
		log(loggerConsola, mensaje);
	}
}
void enviarYLogearMensajeError(int socket, char* mensaje) {
	enviarYOLogearAlgo(socket, mensaje, (void*) log_error);
}

void enviarOMostrarYLogearInfo(int socket, char* mensaje) {
	enviarYOLogearAlgo(socket, mensaje, (void*) log_info);
}
void agregarALista(char* unTimestamp,char* unaKey,char* unValue,t_list* head){
	registro* guardarRegistro = malloc (sizeof(registro));
	guardarRegistro->timestamp = atoi(unTimestamp);
	guardarRegistro->key = atoi(unaKey);
	guardarRegistro->value = string_duplicate(unValue);
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
	pthread_mutex_lock(&mutexLogger);
	log_info(logger,"Determinando existencia de tabla en la ruta: %s",rutaDirectorio);
	pthread_mutex_unlock(&mutexLogger);
	if (stat(rutaDirectorio, &sb) == 0 && S_ISDIR(sb.st_mode))
	    {
		pthread_mutex_lock(&mutexLogger);
	    	log_info(loggerConsola,"La tabla existe en el FS");
	    	pthread_mutex_unlock(&mutexLogger);
	    	validacion=1;
	    }
	    else
	    {
	    	pthread_mutex_lock(&mutexLogger);
	    	log_info(loggerConsola,"Error no existe tabla en ruta indicada %s \n",rutaDirectorio);
	    	pthread_mutex_unlock(&mutexLogger);
	    	validacion=0;
	    }
	free(rutaDirectorio);
	return validacion;
}

int calcularParticion(int key,int cantidadParticiones){
	int particion= key%cantidadParticiones;
	return particion;
}


bool estaLaKey(int key,void* elemento){
	registro* unRegistro = (registro*) elemento;
//guarda basura en el value
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


bool agregarRegistro(char* nombreTabla, registro* unRegistro, tablaMem* tabla){

		if (string_equals_ignore_case(tabla->nombre, nombreTabla)){
			list_add(tabla->listaRegistros, unRegistro);

			pthread_mutex_lock(&mutexLogger);
			log_info(logger, "Se añadio registro");
			pthread_mutex_unlock(&mutexLogger);
			return true;
		}else{
			return false;
		}

}

//Guarda un registro en la memtable
void guardarRegistro(registro* unRegistro, char* nombreTabla) {

	bool buscarPorNombre(tablaMem* elemento){
		return agregarRegistro(nombreTabla, unRegistro, elemento);
	}
	pthread_mutex_lock(&mutexMemtable);
	if(!list_find(memtable, buscarPorNombre)){

		tablaMem* nuevaTabla = malloc(sizeof(tablaMem));
						nuevaTabla->nombre = string_duplicate(nombreTabla);
						nuevaTabla->listaRegistros = list_create();
						list_add(nuevaTabla->listaRegistros, unRegistro);
						list_add(memtable, nuevaTabla);
						//pthread_mutex_lock(&mutexLogger);
						log_info(loggerConsola, "Se añadio la tabla a la memtable");
						//pthread_mutex_unlock(&mutexLogger);
	}
	pthread_mutex_unlock(&mutexMemtable);
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
	if(memtable->elements_count== 0){
		enviarYLogearMensajeError(-1,"Error la memtable esta vacia");
	}

	tablaMem* encuentraLista =  list_find(memtable, tieneElNombre);

//SAQUE SEMAFORO DEADLOCK !!!
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

char* infoEnBloque(char* numeroBloque,int sizeTabla){ //pasarle el tamanio de la particion, o ver que onda (rutaTabla)
	//ver que agarre toda la info de los bloques correspondientes a esa tabla
	struct stat sb;

	char* rutaBloque = string_new();
	string_append(&rutaBloque,puntoMontaje);
	string_append(&rutaBloque,"Bloques/");
	string_append(&rutaBloque,numeroBloque);
	string_append(&rutaBloque,".bin");
	int archivo = open(rutaBloque,O_RDWR);

	fstat(archivo,&sb);
	if (sb.st_size == 0){
				return NULL;
	}

	char* informacion = mmap(NULL,tamanioBloques,PROT_READ,MAP_PRIVATE,archivo,NULL);





	free(rutaBloque);
	return informacion;
}



metadata* obtenerMetadata(char* nombreTabla){
	string_to_upper(nombreTabla);
	t_config* configMetadata;
	int cantParticiones;
	int tiempoCompactacion;
	consistencia tipoConsistencia;
	metadata* unaMetadata;//= malloc(sizeof(metadata)); //ver si es necesario malloc, no me acuerdo porque lo pusimos


	char* ruta = string_new();
	string_append(&ruta, puntoMontaje);
	string_append(&ruta,"Tables/");
	string_append(&ruta,nombreTabla);
	string_append(&ruta,"/Metadata");

	configMetadata = config_create(ruta);

	cantParticiones = config_get_int_value(configMetadata, "PARTITIONS");
	tipoConsistencia = config_get_int_value(configMetadata, "CONSISTENCY"); //delegar a funcion con strcmp
	tiempoCompactacion = config_get_int_value(configMetadata, "COMPACTION_TIME"); //OJO ES COMPACTION TIME Y NO COMPACTATION

	unaMetadata->cantParticiones = cantParticiones;
	unaMetadata->tipoConsistencia = tipoConsistencia;
	unaMetadata->tiempoCompactacion = tiempoCompactacion;
	unaMetadata->nombreTabla = string_duplicate(nombreTabla);

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

void cargarInfoDeBloques(char*** buffer, char**arrayDeBloques, int sizeParticion){
	int i = 0;
		while(*(arrayDeBloques+i)!= NULL){
							char* informacion = infoEnBloque(*(arrayDeBloques+i),sizeParticion);
							string_append(*buffer, informacion);
							i++;
						}
}

void cargarInfoDeTmp(char** buffer, char* nombreTabla){
		char* ruta = string_new();
		t_config* part;
		char* bufferAux = string_new();
		bufferAux = buffer;
		int numeroTmp = obtenerCantTemporales(nombreTabla);

		for (int i = 0; i< numeroTmp; i++){

			string_append(&ruta,puntoMontaje);
			string_append(&ruta,"Tables/");
			string_append(&ruta,nombreTabla);
			string_append(&ruta,"/"); //vamos a usar la convension partN.bin
			string_append(&ruta,string_itoa(i));
			string_append(&ruta,".tmp");
			part = config_create(ruta);
			char** arrayDeBloques = config_get_array_value(part,"BLOCKS");
			int sizeParticion=config_get_int_value(part,"SIZE");


			cargarInfoDeBloques(&buffer, arrayDeBloques, sizeParticion);

			free(ruta);
			liberarDoblePuntero(arrayDeBloques);
			config_destroy(part);

		}

}

void cargarInfoDeBloque(char** arrayDeBloques, int sizeParticion, t_list* listaRegistros, char* buffer){

	int i = 0;

	while(*(arrayDeBloques+i)!= NULL){
			char* informacion = infoEnBloque(*(arrayDeBloques+i),sizeParticion);
			string_append(&buffer, informacion);
			i++;
		}

}

registro* devolverRegistroDeListaDeRegistros(t_list* listaRegistros, int key, int socket ){
	registro* registroBuscado;
	bool encontrarLaKey(void *elemento){
					return estaLaKey(key, elemento);
				}
	void* cualEsElMayorTimestamp(void *elemento1, void *elemento2){
			registro* primerElemento = elemento1;
			registro* segundoElemento = elemento2;

			return devolverMayor(primerElemento, segundoElemento);

		}

		t_list* registrosConLaKeyEnListaRegistros = list_filter(listaRegistros, encontrarLaKey);
					if (registrosConLaKeyEnListaRegistros->elements_count == 0){
						printf("No se encuentra la key\n");
						return NULL;
						}
					else{
				registroBuscado= list_fold(registrosConLaKeyEnListaRegistros, list_get(registrosConLaKeyEnListaRegistros,0), cualEsElMayorTimestamp);
				enviarOMostrarYLogearInfo(socket,"Registro encontrado en bloques");
				}
				return registroBuscado;
}

char** separarRegistrosDeBuffer(char* buffer, t_list* listaRegistros){
	char** separarRegistro = string_split(buffer,"\n");
			int j =0;
			for(j=0;*(separarRegistro+j)!=NULL;j++){
				char **aCargar =string_split(*(separarRegistro+j),";");
				agregarALista(*(aCargar+0),*(aCargar+1),*(aCargar+2),listaRegistros);
				liberarDoblePuntero(aCargar);
			}

		return separarRegistro;
}

void funcionSelect(char* argumentos,int socket){ //en la pos 0 esta el nombre y en la segunda la key
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


	void liberarRegistro(registro* registro) {
			free(registro);
		}



	void* cualEsElMayorTimestamp(void *elemento1, void *elemento2){
		registro* primerElemento = elemento1;
		registro* segundoElemento = elemento2;

		return devolverMayor(primerElemento, segundoElemento);

	}
	if(verificarExistenciaDirectorioTabla(*(argSeparados+0)) ==0)
		{
	//	enviarOMostrarYLogearInfo(socket,"No se encontro el directorio");
		}
	else{
		pthread_mutex_lock(&mutexLogger);
		log_info(logger, "Directorio de tabla valido");
		pthread_mutex_unlock(&mutexLogger);

		metadata* metadataTabla = obtenerMetadata(*(argSeparados+0));

		pthread_mutex_lock(&mutexLogger);
		log_info(logger, "Metadata cargado");
		pthread_mutex_unlock(&mutexLogger);

		particion = string_itoa(calcularParticion(key,metadataTabla->cantParticiones)); //cant de particiones de la tabla
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
		//PASAJE POR REFERENCIA
		cargarInfoDeTmp(&buffer, *(argSeparados+0));

		pthread_mutex_lock(&mutexLogger);
		log_info(logger, "Informacion de bloques cargada");
		pthread_mutex_unlock(&mutexLogger);

		char** separarRegistro = separarRegistrosDeBuffer(buffer, listaRegistros);



		//habria que hacer el mismo while si hay temporales if(hayTemporales) habria que ver el tema de cuantos temporales hay, quizas convendria agregarlo en el metadata tipo array
		//puts(buffer);
		//y aca afuera haria la busqueda del registro.
		pthread_mutex_lock(&mutexMemtable);

		if (memtable->elements_count == 0){
			enviarOMostrarYLogearInfo(-1,"El registro no se encuentra en la memtable");

			registroBuscado = devolverRegistroDeListaDeRegistros(listaRegistros, key, socket);
		} else{
			if (!(registroBuscado = devolverRegistroDeMayorTimestampDeLaMemtable(listaRegistros, memtable,*(argSeparados+0), key))){
				pthread_mutex_lock(&mutexLogger);
				log_info(logger, "El registro no se encuentra en la memtable");
				pthread_mutex_unlock(&mutexLogger);
			registroBuscado = devolverRegistroDeListaDeRegistros(listaRegistros,key, socket);
			}
			}

		pthread_mutex_unlock(&mutexMemtable);

		//if((devolverRegistroDeMayorTimestampYAgregarALista(listaRegistros, memtable,*(argSeparados+0), key)) == 0) return NULL;

		liberarDoblePuntero(arrayDeBloques);
		liberarDoblePuntero(separarRegistro);
		liberarDoblePuntero(argSeparados);

		config_destroy(part);
		free (ruta);
		list_destroy_and_destroy_elements(listaRegistros, (void*) liberarRegistro);
		free(buffer);

		//ver si la funcion tiene que devolver el registro
		//printf("Registro seleccionado: %s \n",registroBuscado->value);
		//enviarOMostrarYLogearInfo(-1,"Se encontro el registro");
		if(socket!=-1){
			if(!registroBuscado) {
				serializarYEnviarRegistro(socket, registroError);
			}
			else {
				serializarYEnviarRegistro(socket,armarRegistroConNombreTabla(registroBuscado,*(argSeparados+0)));
			}
		}
	}
}


void funcionInsert(char* argumentos,int socket) {

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
	pthread_mutex_lock(&mutexLogger);
	log_info(logger, "Directorio de tabla valido");
	pthread_mutex_unlock(&mutexLogger);

	if (valorTimestamp == NULL) {
		timestamp = (unsigned long)time(NULL);
	}else{
		timestamp = atoi(valorTimestamp);
	}

//	obtener(nombreTabla);

	registro* registroDePrueba = malloc(sizeof(registro));
				registroDePrueba -> key = key;
				registroDePrueba -> value= string_duplicate(value);
				registroDePrueba -> timestamp = timestamp;

	guardarRegistro(registroDePrueba, nombreTabla);
	enviarOMostrarYLogearInfo(socket,"Se guardo registro");

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

	for(i=0; i < cantDeBloques; i++){
		pthread_mutex_lock(&mutexBitarray);
		bool bit = bitarray_test_bit(bitarray, i);
		pthread_mutex_unlock(&mutexBitarray);

		if(bit == 0){
			encontroBloque = 1;
			bloqueEncontrado = i;
			break;
		}

	}

	if (encontroBloque == 1){
		pthread_mutex_lock(&mutexBitarray);
		bitarray_set_bit(bitarray, bloqueEncontrado);
		pthread_mutex_unlock(&mutexBitarray);
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
	string_append(&infoDelMetadata, "COMPACTION_TIME=");
	string_append(&infoDelMetadata, tiempoCompactacion);

	FILE *archivoMetadata;
	string_append(&rutaMetadata, ruta);
	string_append(&rutaMetadata, "/Metadata");


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

void funcionCreate(char* argumentos,int socket) {


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
	enviarOMostrarYLogearInfo(socket,"Se creo la tabla");
	//compactar(); cada vez que se crea una tabla habria que crear un hilo compactacion
	free(directorioTabla);

}

//hacerlo por tabla!! y que reciba el nombre de la tabla
int tamanioRegistros(char* nombreTabla){

	int tamanioTotal = 0;

	bool tieneElNombre(void *elemento){
		return esIgualAlNombre(nombreTabla, elemento);
	}

	void* sumarRegistros(int valor , registro* registro ){
		tamanioTotal = tamanioTotal + sizeof(registro->key)  + sizeof(registro->timestamp) + (strlen(registro->value) + 1);
	}

	pthread_mutex_lock(&mutexMemtable);
	tablaMem* encuentraTabla =  list_find(memtable, tieneElNombre);
	pthread_mutex_unlock(&mutexMemtable);

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
	list_clean_and_destroy_elements(memtable,(void*) liberarTabla);
	//list_destroy_and_destroy_elements(memtable,(void*) liberarTabla);



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

//liberarBloquesAsignados(int cant,int flagtmp,char* rutaTabla){
//	int i;
//	for(i=0;i<cant;i++){
//			char* ruta = string_new();
//			string_append(&ruta,rutaTabla);
//			string_append(&ruta,"/");
//			if(flagtmp){
//			string_append(&ruta,string_itoa(i));
//			string_append(&ruta,".tmp");
//			}
//			else{
//				string_append(&ruta,".part"); //es part1.bin
//				string_append(&ruta,string_itoa(i));
//				string_append(&ruta,".bin");
//			}
//			t_config* archivo= config_create(ruta);
//			char **bloques = config_get_array_value(archivo);
//			int pos =0;
//			while(*(bloques+pos)!=NULL){
//				pthread_mutex_lock(&mutexBitarray);
//				bitarray_clean_bit(bitarray,itoa(*(bloques+pos)));
//				pthread_mutex_unlock(&mutexBitarray);
//			}
//
//			liberarDoblePuntero(bloques);
//			config_destroy(archivo);
//			free(ruta);
//		}
//}
void liberarBloquesDeTmpYPart(char* nombreArchivo,char* rutaTabla){
	if(string_equals_ignore_case(nombreArchivo, "Metadata")){
		remove(nombreArchivo);
		return;
	}
	char* ruta = string_new();

	t_config* archivo= config_create(ruta);
	char **bloques = config_get_array_value(archivo,"BLOCKS");
	int pos =0;
	while(*(bloques+pos)!=NULL){
		pthread_mutex_lock(&mutexBitarray);
		bitarray_clean_bit(bitarray,string_itoa(*(bloques+pos)));
		pthread_mutex_unlock(&mutexBitarray);
	}

	liberarDoblePuntero(bloques);
	config_destroy(archivo);
	free(ruta);
	remove(nombreArchivo);

}

void funcionDrop(char* nombreTabla,int socket){
	//int cantTmp = obtenerCantTemporales(nombreTabla);
	if(verificarExistenciaDirectorioTabla(nombreTabla)){
		char* ruta = string_new();
		string_append(&ruta,puntoMontaje);
		string_append(&ruta,"Tables/");
		string_append(&ruta,nombreTabla);
		DIR* dir=opendir(ruta);
		struct dirent *sd;
		while((sd=readdir(dir))!=NULL){
			if (string_equals_ignore_case(sd->d_name, ".") || string_equals_ignore_case(sd->d_name, "..") ){continue;}
			puts(sd->d_name);
			liberarBloquesDeTmpYPart(sd->d_name,ruta);
		}
		//ver tema de un semaforo aca pero de la tabla

		free(ruta);
	}
	enviarOMostrarYLogearInfo(socket,"No se encontro la tabla");
}

void agregarTablaALista(char* nombreTabla){
	metadata* metadataBuscado = obtenerMetadata(nombreTabla);
		bool seEncuentraTabla(void* elemento){
			metadata* unMetadata = elemento;
			return string_equals_ignore_case(unMetadata->nombreTabla,nombreTabla);
		}
	pthread_mutex_lock(&mutexListaTabla);
	if(!list_find(listaDeTablas, seEncuentraTabla)){
		list_add(listaDeTablas,metadataBuscado);
		pthread_mutex_unlock(&mutexListaTabla);
		//aca deberiamos abrir tambien un hilo de compactacion
	}
}

void funcionDescribe(char* argumentos,int socket) {
	void loggearYMostarTabla(metadata* unMetadata){
		log_info(loggerConsola,"La tabla: %s, tiene %d particiones, consistencia= %d "
				"y tiempo de compactacion= %d \n",unMetadata->nombreTabla,unMetadata->cantParticiones,
				unMetadata->tipoConsistencia,unMetadata->tiempoCompactacion);
	}
	metadata* metadataBuscado = NULL;
	DIR* dir;
	struct dirent *sd;
	if(string_equals_ignore_case(argumentos,"ALL")){
		char* rutaDirectorioTablas = string_new();
		string_append(&rutaDirectorioTablas,puntoMontaje);
		string_append(&rutaDirectorioTablas,"Tables");
		if((dir = opendir(rutaDirectorioTablas))==NULL)
				enviarOMostrarYLogearInfo(-1,"No se pudo abrir el directorio");

		while((sd=readdir(dir))!=NULL){
			if (string_equals_ignore_case(sd->d_name, ".") || string_equals_ignore_case(sd->d_name, "..") ){continue;} //no me lo ignoraba sino de otra manera
			else{
				agregarTablaALista(sd->d_name);

			}

		}
		enviarOMostrarYLogearInfo(-1,"Se cargaron todas las tablas del directorio");
		if(socket==-1){
			pthread_mutex_lock(&mutexListaTabla);
			list_iterate(listaDeTablas,(void*)loggearYMostarTabla);
			pthread_mutex_unlock(&mutexListaTabla);
		}
		else{
			//aca deberiamos ver de mandar la lista a memoria
		}
		closedir(dir);
		free(rutaDirectorioTablas );
	}
	else{
		if(verificarExistenciaDirectorioTabla(argumentos)){
			metadataBuscado = obtenerMetadata(argumentos);
			enviarOMostrarYLogearInfo(-1,"Se encontro el metadata buscado");
		if(socket!=-1){
			serializarYEnviarMetadata(socket,metadataBuscado);
		}
		free(metadataBuscado->nombreTabla);
		}
	}
}

