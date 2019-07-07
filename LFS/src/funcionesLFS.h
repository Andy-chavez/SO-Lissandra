#include <commons/config.h>
#include <commons/log.h>
#include <stdio.h>
#include <stdlib.h> //malloc,alloc,realloc
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/io.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include "utils.h"
#include "compactador.h"


// DECLARACIONES Y ESTRUCTURAS //

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
	char* nombre;
	t_list* temporales;
} tablaTmp;


int verificarExistenciaDirectorioTabla(char* nombreTabla,int socket);
metadata* obtenerMetadata(char* nombreTabla); //habria que ver de pasarle la ruta de la tabla y de ahi buscar el metadata
bool estaLaKey(int key,void* elemento);
bool esIgualAlNombre(char* nombreTabla,void * elemento);
bool agregarRegistro(char* nombreTabla, registro* unRegistro, tablaMem * tabla); //este es para memtable
registro* devolverRegistroDeMayorTimestampDeLaMemtable(t_list* listaRegistros, t_list* memtable, char* nombreTabla, int key,int socket);
void liberarDoblePuntero(char** doblePuntero);
void funcionSelect(char* argumentos,int socket);
void funcionInsert(char* argumentos,int socket);
void crearMetadata(char* ruta, char* consistenciaTabla, char* numeroParticiones, char* tiempoCompactacion);
void crearParticiones(char* ruta, int numeroParticiones); //se puede usar para los temporales.
void funcionCreate(char* argumentos,int socket);
void funcionDescribe(char* argumentos,int socket); //despues quizas haya que cambiar el tipo
void inicializarRegistroError();
void funcionDrop(char* nombreTabla,int socket);
void agregarTablaALista(char* nombreTabla);
void cargarInfoDeTmpYParticion(char** buffer, char* nombreTabla,char** arrayDeParticion);

// ------------------------------------------------------------------------ //
// 2) CREACION DE ELEMENTOS //

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

	string_append(&rutaMetadata, ruta);
	string_append(&rutaMetadata, "/Metadata");


	guardarInfoEnArchivo(rutaMetadata, infoDelMetadata);


	free(infoDelMetadata);
	free(rutaMetadata);
}

void crearParticiones(char* ruta, int numeroParticiones) {

	char* bloqueLibre;

	for (int i= 0; i < numeroParticiones; i++){

		char * rutaDeLaParticion = string_new();
		char* infoAGuardar = string_new();
		char* numeroParticion = string_itoa(i);

		string_append(&rutaDeLaParticion, ruta);
		string_append(&rutaDeLaParticion, "/part");
		string_append(&rutaDeLaParticion, numeroParticion);
		string_append(&rutaDeLaParticion, ".bin");

		string_append(&infoAGuardar, "SIZE=0"); //cuando se crea esta vacio el bloque, es 0
		string_append(&infoAGuardar, "\n");
		string_append(&infoAGuardar, "BLOCKS=[");
		bloqueLibre = devolverBloqueLibre();
		string_append(&infoAGuardar, bloqueLibre);
		string_append(&infoAGuardar, "]");

		guardarInfoEnArchivo(rutaDeLaParticion, infoAGuardar);

		free(infoAGuardar);
		free(rutaDeLaParticion);
		free(numeroParticion);
		free(bloqueLibre);
	}
}

metadataConSemaforo* crearMetadataConSemaforo (metadata* unMetadata){
	pthread_mutex_t mutexFS;
	pthread_mutex_t mutexMemtable;
	pthread_t threadCompactacion;
	metadataConSemaforo* nuevoMetadata=malloc (sizeof(metadataConSemaforo));
	nuevoMetadata->hiloDeCompactacion = threadCompactacion;
	nuevoMetadata->cantParticiones = unMetadata->cantParticiones;
	nuevoMetadata->nombreTabla = string_duplicate(unMetadata->nombreTabla);
	nuevoMetadata->tiempoCompactacion = unMetadata->tiempoCompactacion;
	nuevoMetadata->tipoConsistencia = unMetadata->tipoConsistencia;
	nuevoMetadata->semaforoFS = mutexFS;
	nuevoMetadata->semaforoFS = mutexMemtable;
	free(unMetadata->nombreTabla);
	free(unMetadata);
	return nuevoMetadata;
}

// ------------------------------------------------------------------------ //
// 3) FUNCIONES //

int verificarExistenciaDirectorioTabla(char* nombreTabla,int socket){
	bool seEncuentraTabla(metadataConSemaforo* unMetadata){
		return string_equals_ignore_case(unMetadata->nombreTabla,nombreTabla);
	}
	int validacion;
	string_to_upper(nombreTabla);
	char* rutaDirectorio= string_new();
	string_append(&rutaDirectorio,puntoMontaje); //OJO ACA HAY QUE VER QUE EN EL CONFIG NO TE VENGA CON "" EL PUNTO DE MONTAJE
	string_append(&rutaDirectorio,"Tables/");
	string_append(&rutaDirectorio,nombreTabla);
	//struct stat sb;
	soloLoggear(socket,"Determinando existencia de tabla en la ruta: %s",rutaDirectorio);
	pthread_mutex_lock(&mutexListaDeTablas);
	if (list_find(listaDeTablas,seEncuentraTabla))
	    {
			soloLoggear(socket,"Existe en el FS la tabla: %s ", nombreTabla);
	    	validacion=1;
	    }
	    else
	    {
	    	soloLoggear(socket,"No existe tabla en la ruta: %s \n",rutaDirectorio);
	    	validacion=0;
	    }
	pthread_mutex_unlock(&mutexListaDeTablas);
	free(rutaDirectorio);
	return validacion;
}

bool agregarRegistro(char* nombreTabla, registro* unRegistro, tablaMem* tabla){

		if (string_equals_ignore_case(tabla->nombre, nombreTabla)){
			list_add(tabla->listaRegistros, unRegistro);
			enviarOMostrarYLogearInfo(-1,"Se añadio registro");
			return true;
		}else{
			return false;
		}

}

void agregarTablaALista(char* nombreTabla){
	metadataConSemaforo* metadataBuscado = crearMetadataConSemaforo(obtenerMetadata(nombreTabla));
		bool seEncuentraTabla(void* elemento){
			metadata* unMetadata = elemento;
			return string_equals_ignore_case(unMetadata->nombreTabla,nombreTabla);
		}
	pthread_mutex_lock(&mutexListaDeTablas);
	if(!list_find(listaDeTablas, seEncuentraTabla)){
		pthread_mutex_init(&(metadataBuscado->semaforoFS),NULL); //inicias el semaforo de la nueva tabla
		pthread_mutex_init(&(metadataBuscado->semaforoMemtable),NULL);
		pthread_create(&(metadataBuscado->hiloDeCompactacion),NULL,(void*) compactar,metadataBuscado);
		list_add(listaDeTablas,metadataBuscado);
	}
	else{
		free(metadataBuscado->nombreTabla);
		free(metadataBuscado);
	}
	pthread_mutex_unlock(&mutexListaDeTablas);
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

						enviarOMostrarYLogearInfo(-1,"Se añadio la tabla %s a la memtable",nombreTabla);
	}
	pthread_mutex_unlock(&mutexMemtable);
}


registro* devolverRegistroDeMayorTimestampDeLaMemtable(t_list* listaRegistros, t_list* memtable, char* nombreTabla, int key,int socket){


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
	if(memtable->elements_count== 0){
		soloLoggearError(socket,"Error la memtable esta vacia");
	}
	pthread_mutex_lock(&mutexMemtable);
	tablaMem* encuentraLista =  list_find(memtable, tieneElNombre);
	pthread_mutex_unlock(&mutexMemtable);
	if(encuentraLista == NULL){
		return NULL;
	}
	t_list* registrosConLaKeyEnMemtable = list_filter(encuentraLista->listaRegistros, encontrarLaKey);

	if (registrosConLaKeyEnMemtable->elements_count == 0){
		return NULL;
	}

	registro* registroDeMayorTimestamp= list_fold(registrosConLaKeyEnMemtable, list_get(registrosConLaKeyEnMemtable,0), cualEsElMayorTimestamp);

	soloLoggear(socket,"Registro encontrado en la memtable");

	//list_destroy(encuentraLista);
	list_destroy(registrosConLaKeyEnMemtable);
//	list_destroy_and_destroy_elements(registrosConLaKeyEnMemtable, liberarRegistros);

return registroDeMayorTimestamp;

}

metadata* obtenerMetadata(char* nombreTabla){
	string_to_upper(nombreTabla);
	t_config* configMetadata;
	int cantParticiones;
	int tiempoCompactacion;
	consistencia tipoConsistencia;
	metadata* unaMetadata= malloc(sizeof(metadata)); //ver si es necesario malloc, no me acuerdo porque lo pusimos

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
// ------------------------------------------------------------------------ //
// 4) FUNCIONES USADAS POR ORDEN SUPERIOR//

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
						soloLoggearError(socket,"No se encuentra la key en los bloques de tmp y particion\n");
						return NULL;
						}
					else{
				registroBuscado= list_fold(registrosConLaKeyEnListaRegistros, list_get(registrosConLaKeyEnListaRegistros,0), cualEsElMayorTimestamp);
				soloLoggear(socket,"Registro encontrado en bloques");
				}
				return registroBuscado;
}

// ------------------------------------------------------------------------ //
// 5) CARGAR COSAS//


void cargarInfoDeBloques(char*** buffer, char**arrayDeBloques){
	int i = 0;
		while(*(arrayDeBloques+i)!= NULL){
							char* informacion = infoEnBloque(*(arrayDeBloques+i));
							if(informacion!=NULL)
							string_append(*buffer, informacion);
							i++;
						}
}

void cargarInfoDeTmpYParticion(char** buffer, char* nombreTabla,char** arrayDeParticion){
		t_config* part;
		int numeroTmp = obtenerCantTemporales(nombreTabla);

		for (int i = 0; i< numeroTmp; i++){
			char* ruta = string_new();
			char* numeroTmp =string_itoa(i);
			string_append(&ruta,puntoMontaje);
			string_append(&ruta,"Tables/");
			string_append(&ruta,nombreTabla);
			string_append(&ruta,"/");
			string_append(&ruta,numeroTmp);
			string_append(&ruta,".tmp");
			part = config_create(ruta);
			char** arrayDeBloques = config_get_array_value(part,"BLOCKS");


			cargarInfoDeBloques(&buffer, arrayDeBloques);

			free(numeroTmp);
			free(ruta);
			liberarDoblePuntero(arrayDeBloques);
			config_destroy(part);

		}
		cargarInfoDeBloques(&buffer, arrayDeParticion); //aca cargas lo de la particion

}

void cargarInfoDeBloque(char** arrayDeBloques, int sizeParticion, t_list* listaRegistros, char* buffer){

	int i = 0;

	while(*(arrayDeBloques+i)!= NULL){
			char* informacion = infoEnBloque(*(arrayDeBloques+i));
			string_append(&buffer, informacion);
			i++;
		}

}


// ------------------------------------------------------------------------ //
// 6) OPERACIONES LQL //

void funcionSelect(char* argumentos,int socket){ //en la pos 0 esta el nombre y en la segunda la key
	char** argSeparados = string_n_split(argumentos,2," ");
	char* nombreTabla = *(argSeparados+0);
	char* particion;
	t_config* part;
	char* ruta = string_new();
	t_list* listaRegistros = list_create();
	int key = atoi(*(argSeparados+1));
	registro* registroBuscado;

	bool encontrarLaKey(void *elemento){
			return estaLaKey(key, elemento);
		}

	void* cualEsElMayorTimestamp(void *elemento1, void *elemento2){
		registro* primerElemento = elemento1;
		registro* segundoElemento = elemento2;

		return devolverMayor(primerElemento, segundoElemento);

	}
	if(verificarExistenciaDirectorioTabla(nombreTabla,socket) ==0){
		soloLoggearError(socket,"No se encontro el directorio");
		if(socket!=-1) {
			enviarError(socket);
		}
		return;
		}
	else{
		pthread_mutex_t semaforoDeTabla =devolverSemaforoDeTablaFS(nombreTabla);
		pthread_mutex_t semaforoTablaMemtable = devolverSemaforoDeTablaMemtable(nombreTabla);
		soloLoggear(socket,"Buscando registro con key= %d",key);
		pthread_mutex_lock(&semaforoDeTabla);

		metadata* metadataTabla = obtenerMetadata(nombreTabla);


		soloLoggear(socket,"Metadata cargado");

		particion = string_itoa(calcularParticion(key,metadataTabla->cantParticiones));
		liberarMetadata(metadataTabla);
		char* buffer = string_new();
		string_append(&ruta,puntoMontaje);
		string_append(&ruta,"Tables/");
		string_append(&ruta,nombreTabla);
		string_append(&ruta,"/part");
		string_append(&ruta,particion);
		string_append(&ruta,".bin");
		part = config_create(ruta);
		char** arrayDeBloques = config_get_array_value(part,"BLOCKS");

		cargarInfoDeTmpYParticion(&buffer, nombreTabla,arrayDeBloques);
		pthread_mutex_unlock(&semaforoDeTabla);
		separarRegistrosYCargarALista(buffer, listaRegistros);
		soloLoggear(socket,"Informacion de bloques de particion y tmps cargada");
		pthread_mutex_lock(&mutexMemtable);
		int cantElementosMemtable = memtable->elements_count;
		pthread_mutex_unlock(&mutexMemtable);
		if (cantElementosMemtable == 0){
			soloLoggear(socket,"Memtable esta Vacia");
			registroBuscado = devolverRegistroDeListaDeRegistros(listaRegistros, key, socket);
		} else{
			pthread_mutex_lock(&semaforoTablaMemtable);
			if (!(registroBuscado = devolverRegistroDeMayorTimestampDeLaMemtable(listaRegistros, memtable,nombreTabla, key,socket))){
			pthread_mutex_unlock(&semaforoTablaMemtable);
			soloLoggear(socket,"El registro no se encuentra en la memtable");
			registroBuscado = devolverRegistroDeListaDeRegistros(listaRegistros,key, socket);
			}
		}

		liberarDoblePuntero(arrayDeBloques);

		config_destroy(part);
		free (ruta);
		free(buffer);
		free(particion);

			if(!registroBuscado) {
				if(socket!=-1) enviarError(socket);
				soloLoggearError(socket,"No se encontro el registro");
				list_destroy_and_destroy_elements(listaRegistros, (void*) liberarRegistros);
				liberarDoblePuntero(argSeparados);
				return;
			}
			else {
				soloLoggear(socket,"El value del registro buscado es: %s ",registroBuscado->value);
				if(socket!=-1){
					registroConNombreTabla* registroAMandar = armarRegistroConNombreTabla(registroBuscado,nombreTabla);
					serializarYEnviarRegistro(socket,registroAMandar);
					liberarRegistroConNombreTabla(registroAMandar);
					list_destroy_and_destroy_elements(listaRegistros, (void*) liberarRegistros);
					liberarDoblePuntero(argSeparados);
				}
					return;
			}
	}
}


void funcionInsert(char* argumentos,int socket) {
	//la memtable, y si es que existe la tabla

	//al liberar memtable mutex memtable

	char** argSeparados = string_n_split(argumentos,3,"\"");
	char* nombreYKey = *(argSeparados + 0);
	char** separarNombreYKey = string_split(nombreYKey, " ");
	char* nombreTabla = *(separarNombreYKey + 0);
	int key = atoi(*(separarNombreYKey + 1));
	char* value = *(argSeparados + 1);
	char* valorTimestamp = *(argSeparados + 2);
	int timestamp;

	if(strlen(value)> tamanioValue){
		soloLoggearError(socket,"El tamanio del value es mayor al maximo");
		liberarDoblePuntero(separarNombreYKey);
		liberarDoblePuntero(argSeparados);
		return;
	}
	if (!verificarExistenciaDirectorioTabla(nombreTabla,socket)){
		liberarDoblePuntero(separarNombreYKey);
		liberarDoblePuntero(argSeparados);
		if(socket!=-1) enviarError(socket);
		return;
	}
	soloLoggear(socket,"Directorio de tabla valido");

	if (valorTimestamp == NULL) {
		timestamp = (unsigned long)time(NULL);
	}else{
		timestamp = atoi(valorTimestamp);
	}

	pthread_mutex_t semaforoDeTablaMemtable = devolverSemaforoDeTablaMemtable(nombreTabla);
	registro* registroAGuardar = malloc(sizeof(registro));
	registroAGuardar -> key = key;
	registroAGuardar -> value= string_duplicate(value);
	registroAGuardar -> timestamp = timestamp;


	pthread_mutex_lock(&semaforoDeTablaMemtable);
	guardarRegistro(registroAGuardar, nombreTabla);
	pthread_mutex_unlock(&semaforoDeTablaMemtable);
	soloLoggear(socket,"Se guardo el registro con value: %s y key igual a: %d",registroAGuardar->value,registroAGuardar->key);

	liberarDoblePuntero(separarNombreYKey);
	liberarDoblePuntero(argSeparados);


}

void funcionCreate(char* argumentos,int socket) {


	char** argSeparados = string_n_split(argumentos,4," ");

	//es todo char porque cuando lo guardes en el metadata se guarda como caracteres
	char* nombreTabla = *(argSeparados + 0);
	string_to_upper(nombreTabla); //para que nos quede en el file system todo en mayuscula
    char* consistenciaTabla = *(argSeparados + 1);
	char* numeroParticiones = *(argSeparados + 2);
	char* tiempoCompactacion = *(argSeparados + 3);

	char* directorioTabla = string_new();
	if (!verificarExistenciaDirectorioTabla(nombreTabla,socket)){

		string_append(&directorioTabla, puntoMontaje);
		string_append(&directorioTabla, "Tables/");
		string_append(&directorioTabla, nombreTabla);

		mkdir(directorioTabla, 0777);

		crearMetadata(directorioTabla, consistenciaTabla, numeroParticiones, tiempoCompactacion);
		int cantidadParticiones = atoi(numeroParticiones);
		crearParticiones(directorioTabla, cantidadParticiones);
		enviarOMostrarYLogearInfo(socket,"Se creo la tabla: %s",nombreTabla);
		agregarTablaALista(nombreTabla);

	}else{
		soloLoggearError(socket,"Error ya existe la tabla en FS");
		if (socket!=-1) enviarError(socket);
		liberarDoblePuntero(argSeparados);
		free(directorioTabla);
		return;
	}
	liberarDoblePuntero(argSeparados);

	free(directorioTabla);

}


void funcionDrop(char* nombreTabla,int socket){
	bool liberarTablaConEsteNombre(metadataConSemaforo* unMetadata){
		return string_equals_ignore_case(unMetadata->nombreTabla,nombreTabla);
}

	if(verificarExistenciaDirectorioTabla(nombreTabla,socket)){
		pthread_mutex_t semaforoDeTabla = devolverSemaforoDeTablaFS(nombreTabla);
		char* ruta = string_new();
		string_append(&ruta,puntoMontaje);
		string_append(&ruta,"Tables/");
		string_append(&ruta,nombreTabla);
		DIR* dir=opendir(ruta);
		soloLoggear(socket,"Liberando Bloques de los tmp y las particiones");
		struct dirent *sd;
		pthread_mutex_lock(&semaforoDeTabla);
		while((sd=readdir(dir))!=NULL){
			if (string_equals_ignore_case(sd->d_name, ".") || string_equals_ignore_case(sd->d_name, "..") ){continue;}
			liberarBloquesDeTmpYPart(sd->d_name,ruta);
		}
		rmdir(ruta);
		closedir(dir);
		pthread_mutex_unlock(&semaforoDeTabla);
		pthread_mutex_lock(&mutexListaDeTablas);
		list_remove_and_destroy_by_condition(listaDeTablas,liberarTablaConEsteNombre,(void*) liberarMetadataConSemaforo);
		pthread_mutex_unlock(&mutexListaDeTablas);
		enviarOMostrarYLogearInfo(socket,"Se elimino la tabla: %s",nombreTabla);
		free(ruta);
		return;
	}
	soloLoggearError(socket,"No se encontro la tabla");
	if(socket!=-1) enviarError(socket);
}



void* tranformarMetadataSinSemaforo(metadataConSemaforo* metadataATransformar){
	metadata* metadataSinSemaforo = malloc (sizeof(metadata));
	metadataSinSemaforo->cantParticiones = metadataATransformar->cantParticiones;
	metadataSinSemaforo->nombreTabla = string_duplicate(metadataATransformar->nombreTabla);
	metadataSinSemaforo->tiempoCompactacion = metadataATransformar->tiempoCompactacion;
	metadataSinSemaforo->tipoConsistencia = metadataATransformar->tipoConsistencia;
	return metadataSinSemaforo;
}

void serializarMetadataConSemaforo(int socket){
	operacionProtocolo protocolo = PAQUETEMETADATAS;
	enviar(socket,(void*) &protocolo,sizeof(operacionProtocolo));
	pthread_mutex_lock(&mutexListaDeTablas);
	t_list* listaAMandar = list_map(listaDeTablas,tranformarMetadataSinSemaforo);
	pthread_mutex_unlock(&mutexListaDeTablas);
	serializarYEnviarPaqueteMetadatas(socket,listaAMandar);
	list_destroy_and_destroy_elements(listaAMandar,(void*) liberarMetadata);
}

void funcionDescribe(char* argumentos,int socket) {
	void loggearYMostrarTabla(metadataConSemaforo* unMetadata){
		soloLoggear(-1,"La tabla: %s, tiene %d particion/es, consistencia= %d "
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
		if((dir = opendir(rutaDirectorioTablas))==NULL){
				soloLoggearError(socket,"No se pudo abrir el directorio"); //ver en realidad de mandar error
				if(socket!=-1) enviarError(socket);
				free(rutaDirectorioTablas);
				return;
		}
		while((sd=readdir(dir))!=NULL){
			if (string_equals_ignore_case(sd->d_name, ".") || string_equals_ignore_case(sd->d_name, "..") ){continue;} //no me lo ignoraba sino de otra manera
			else{
				agregarTablaALista(sd->d_name);

			}

		}
		if(list_size(listaDeTablas)==0){
			soloLoggearError(socket,"No hay tablas en el FS"); //caso a revisar, que no haya tablas en LFS
			if(socket!=-1) enviarError(socket);
			closedir(dir);
			free(rutaDirectorioTablas);
			return;
		}
		soloLoggear(socket,"Se cargaron todas las tablas del directorio");
		if(socket==-1){
			pthread_mutex_lock(&mutexListaDeTablas);
			list_iterate(listaDeTablas,(void*)loggearYMostrarTabla);
			pthread_mutex_unlock(&mutexListaDeTablas);
		}
		else{
			serializarMetadataConSemaforo(socket);
		}
		closedir(dir);
		free(rutaDirectorioTablas );
	}
	else{ //este es el decribe de una tabla sola
		if(verificarExistenciaDirectorioTabla(argumentos,socket)){
			metadataBuscado = obtenerMetadata(argumentos);
			soloLoggear(socket,"Se encontro el metadata buscado");
					if(socket!=-1){
						serializarYEnviarMetadata(socket,metadataBuscado);
						liberarMetadata(metadataBuscado);
						return;
					}
					soloLoggear(socket,"La tabla: %s, tiene %d particion/es, consistencia= %d "
									"y tiempo de compactacion= %d \n",metadataBuscado->nombreTabla,metadataBuscado->cantParticiones,
									metadataBuscado->tipoConsistencia,metadataBuscado->tiempoCompactacion);
					liberarMetadata(metadataBuscado);
		}
		else {
			soloLoggearError(socket,"No se encontro el metadata buscado");
			if(socket!=-1) enviarError(socket);
			return;
		}
	}
}
