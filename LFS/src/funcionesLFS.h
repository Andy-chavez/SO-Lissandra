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
registro* devolverRegistroDeMayorTimestampDeLaMemtable(char* nombreTabla, int key,int socket);
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
	metadataConSemaforo* nuevoMetadata=malloc(sizeof(metadataConSemaforo));
	nuevoMetadata->cantParticiones = unMetadata->cantParticiones;
	nuevoMetadata->nombreTabla = string_duplicate(unMetadata->nombreTabla);
	nuevoMetadata->tiempoCompactacion = unMetadata->tiempoCompactacion;
	nuevoMetadata->tipoConsistencia = unMetadata->tipoConsistencia;
	nuevoMetadata->semaforoFS = malloc(sizeof(sem_t));
	nuevoMetadata->semaforoMemtable = malloc(sizeof(sem_t));
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
	sem_wait(&mutexListaDeTablas);
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
	sem_post(&mutexListaDeTablas);
	free(rutaDirectorio);
	return validacion;
}

bool agregarRegistro(char* nombreTabla, registro* unRegistro, tablaMem* tabla){

		if (string_equals_ignore_case(tabla->nombre, nombreTabla)){
			list_add(tabla->listaRegistros, unRegistro);
			enviarOMostrarYLogearInfo(-1,"Se añadio en la tabla %s el registro key %d value \"%s\" timestamp %d", tabla->nombre, unRegistro->key, unRegistro->value, unRegistro->timestamp);
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
	sem_wait(&mutexListaDeTablas);
	if(!list_find(listaDeTablas, seEncuentraTabla)){
		sem_init(metadataBuscado->semaforoFS, 0, 1); //inicias el semaforo de la nueva tabla
		sem_init(metadataBuscado->semaforoMemtable, 0, 1);
		pthread_create(&(metadataBuscado->hiloDeCompactacion),NULL,(void*) compactar,metadataBuscado);
		list_add(listaDeTablas,metadataBuscado);
	}
	else{
		free(metadataBuscado->nombreTabla);
		free(metadataBuscado->semaforoFS);
		free(metadataBuscado->semaforoMemtable);
		free(metadataBuscado);
	}
	sem_post(&mutexListaDeTablas);
}

//Guarda un registro en la memtable
void guardarRegistro(registro* unRegistro, char* nombreTabla) {

	bool buscarPorNombre(tablaMem* elemento){
		return agregarRegistro(nombreTabla, unRegistro, elemento);
	}
	if(!list_find(memtable, buscarPorNombre)){

		tablaMem* nuevaTabla = malloc(sizeof(tablaMem));
						nuevaTabla->nombre = string_duplicate(nombreTabla);
						nuevaTabla->listaRegistros = list_create();
						list_add(nuevaTabla->listaRegistros, unRegistro);
						list_add(memtable, nuevaTabla);

						enviarOMostrarYLogearInfo(-1,"Se añadio la tabla %s a la memtable",nombreTabla);
	}
}


registro* devolverRegistroDeMayorTimestampDeLaMemtable(char* nombreTabla, int key,int socket){


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
	tablaMem* encuentraLista =  list_find(memtable, tieneElNombre);
	if(encuentraLista == NULL){
		return NULL;
	}
	t_list* registrosConLaKeyEnMemtable = list_filter(encuentraLista->listaRegistros, encontrarLaKey);

	if (registrosConLaKeyEnMemtable->elements_count == 0){
		list_destroy(registrosConLaKeyEnMemtable);
		return NULL;
	}

	registro* registroDeMayorTimestamp= list_fold(registrosConLaKeyEnMemtable, list_get(registrosConLaKeyEnMemtable,0), cualEsElMayorTimestamp);

	soloLoggear(socket,"Registro encontrado en la memtable");

	//list_destroy(encuentraLista);
	list_destroy(registrosConLaKeyEnMemtable);
	//list_destroy_and_destroy_elements(registrosConLaKeyEnMemtable, liberarRegistros);

return registroDeMayorTimestamp;

}
int devolverConsistencia(char* consistencia){
	if(string_equals_ignore_case(consistencia,"SC")) return 0;
	if(string_equals_ignore_case(consistencia,"SHC")) return 1;
	if(string_equals_ignore_case(consistencia,"EC")) return 2;
	return -1;
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

	while(configMetadata == NULL || !config_has_property(configMetadata, "PARTITIONS") || !config_has_property(configMetadata, "CONSISTENCY") || !config_has_property(configMetadata, "COMPACTION_TIME")) {
		soloLoggearError(-1,"No se abrio bien la metadata de la tabla %s", nombreTabla);
		configMetadata = config_create(ruta);
	}

	cantParticiones = config_get_int_value(configMetadata, "PARTITIONS");
	char* consistencia = config_get_string_value(configMetadata, "CONSISTENCY");
	tipoConsistencia = devolverConsistencia(consistencia);
	tiempoCompactacion = config_get_int_value(configMetadata, "COMPACTION_TIME"); //OJO ES COMPACTION TIME Y NO COMPACTATION

	unaMetadata->cantParticiones = cantParticiones;
	unaMetadata->tipoConsistencia = tipoConsistencia;
	unaMetadata->tiempoCompactacion = tiempoCompactacion;
	unaMetadata->nombreTabla = string_duplicate(nombreTabla);

	//free(consistencia);
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
						list_destroy(registrosConLaKeyEnListaRegistros);
						return NULL;
						}
					else{
				registroBuscado= list_fold(registrosConLaKeyEnListaRegistros, list_get(registrosConLaKeyEnListaRegistros,0), cualEsElMayorTimestamp);
				soloLoggear(socket,"Registro encontrado en bloques");
				list_destroy(registrosConLaKeyEnListaRegistros);
				}
				return registroBuscado;
}

// ------------------------------------------------------------------------ //
// 5) CARGAR COSAS//


void cargarInfoDeBloques(char** buffer, char**arrayDeBloques){
	int i = 0;
		while(*(arrayDeBloques+i)!= NULL){
			char* informacion = infoEnBloque(*(arrayDeBloques+i));
			if(informacion!=NULL) {
				string_append(buffer, informacion);
				munmap((void*) informacion, tamanioBloques);
			}
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


			cargarInfoDeBloques(buffer, arrayDeBloques);

			free(numeroTmp);
			free(ruta);
			liberarDoblePuntero(arrayDeBloques);
			config_destroy(part);

		}
		cargarInfoDeBloques(buffer, arrayDeParticion); //aca cargas lo de la particion

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
		if(socket!=-1) enviarError(socket);
		free(ruta);
		liberarDoblePuntero(argSeparados);
		return;
		}
	else{
		sem_t *semaforoDeTabla =devolverSemaforoDeTablaFS(nombreTabla);
		sem_t *semaforoTablaMemtable = devolverSemaforoDeTablaMemtable(nombreTabla);
		soloLoggear(socket,"Buscando registro con key= %d",key);
		//pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);

		sem_wait(semaforoTablaMemtable);
		sem_wait(&mutexMemtable);
		int cantElementosMemtable = memtable->elements_count;
		registroBuscado = devolverRegistroDeMayorTimestampDeLaMemtable(nombreTabla, key,socket);
		sem_post(&mutexMemtable);
		sem_post(semaforoTablaMemtable);
		if (cantElementosMemtable == 0 || !registroBuscado){
			soloLoggear(socket,"Memtable esta Vacia o no se encuentra en la memtable el registro buscado");
			sem_wait(semaforoDeTabla);
			metadata* metadataTabla = obtenerMetadata(nombreTabla);
			particion = string_itoa(calcularParticion(key,metadataTabla->cantParticiones));
			liberarMetadata(metadataTabla);
			char* buffer = string_new();
			string_append(&ruta,puntoMontaje);
			string_append(&ruta,"Tables/");
			string_append(&ruta,nombreTabla);
			string_append(&ruta,"/part");
			string_append(&ruta,particion);
			string_append(&ruta,".bin");
			soloLoggear(-1,"Voy a crear el config en base a la ruta %s", ruta);
			part = config_create(ruta);
			char** arrayDeBloques = config_get_array_value(part,"BLOCKS");
			config_destroy(part);
			cargarInfoDeTmpYParticion(&buffer, nombreTabla,arrayDeBloques);
			sem_post(semaforoDeTabla);

			separarRegistrosYCargarALista(buffer, listaRegistros);
			soloLoggear(socket,"Informacion de bloques de particion y tmps cargada");
			registroBuscado = devolverRegistroDeListaDeRegistros(listaRegistros, key, socket);
			liberarDoblePuntero(arrayDeBloques);

			free(buffer);
			free(particion);
		}
			free(ruta);

			if(!registroBuscado) {
				if(socket!=-1) enviarError(socket);
				soloLoggearError(socket,"SELECT %s %d: No se encontro el registro",nombreTabla,key);
				soloLoggearResultados(socket,1,"RESULTADO SELECT %s %d es: ERROR SELECT",nombreTabla,key);
				list_destroy_and_destroy_elements(listaRegistros, (void*) liberarRegistros);
				liberarDoblePuntero(argSeparados);
				return;
			}
			else {
				soloLoggear(socket,"RESULTADO SELECT %s %d; value: %s ",nombreTabla,registroBuscado->key,registroBuscado->value);
				soloLoggearResultados(socket,0,"RESULTADO SELECT %s %d es: %s ",nombreTabla,registroBuscado->key,registroBuscado->value);
				if(socket!=-1){
					registroConNombreTabla* registroAMandar = armarRegistroConNombreTabla(registroBuscado,nombreTabla);
					serializarYEnviarRegistro(socket,registroAMandar);
					liberarRegistroConNombreTabla(registroAMandar);
				}
				list_destroy_and_destroy_elements(listaRegistros, (void*) liberarRegistros);
				liberarDoblePuntero(argSeparados);
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
		soloLoggearResultados(socket,1,"RESULTADO INSERT %s %d %s :ERROR INSERT",nombreTabla,key,value);
		liberarDoblePuntero(separarNombreYKey);
		liberarDoblePuntero(argSeparados);
		return;
	}
	if (!verificarExistenciaDirectorioTabla(nombreTabla,socket)){
		soloLoggearResultados(socket,1,"RESULTADO INSERT %s %d %s :ERROR INSERT",nombreTabla,key,value);
		liberarDoblePuntero(separarNombreYKey);
		liberarDoblePuntero(argSeparados);
		if(socket!=-1) enviarError(socket);
		return;
	}

	if (valorTimestamp == NULL) {
		timestamp = (unsigned long)time(NULL);
	}else{
		timestamp = atoi(valorTimestamp);
	}

	sem_t *semaforoDeTablaMemtable = devolverSemaforoDeTablaMemtable(nombreTabla);
	registro* registroAGuardar = malloc(sizeof(registro));
	registroAGuardar -> key = key;
	registroAGuardar -> value= string_duplicate(value);
	registroAGuardar -> timestamp = timestamp;


	sem_wait(semaforoDeTablaMemtable);
	sem_wait(&mutexMemtable);
	guardarRegistro(registroAGuardar, nombreTabla);
	sem_post(&mutexMemtable);
	sem_post(semaforoDeTablaMemtable);
	soloLoggear(socket,"Se guardo el registro con value: %s y key igual a: %d",registroAGuardar->value,registroAGuardar->key);
	soloLoggearResultados(socket,0,"RESULTADO INSERT %s %d %s :EXITOSA",nombreTabla,key,value);

	liberarDoblePuntero(separarNombreYKey);
	liberarDoblePuntero(argSeparados);
}

void funcionCreate(char* argumentos,int socket) {


	char** argSeparados = string_n_split(argumentos,4," ");

	char* nombreTabla = *(argSeparados + 0);
	string_to_upper(nombreTabla);
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
		sem_t *semaforoDeTabla = devolverSemaforoDeTablaFS(nombreTabla);
		char* ruta = string_new();
		string_append(&ruta,puntoMontaje);
		string_append(&ruta,"Tables/");
		string_append(&ruta,nombreTabla);
		DIR* dir=opendir(ruta);
		soloLoggear(socket,"Liberando Bloques de los tmp y las particiones");
		struct dirent *sd;
		sem_wait(semaforoDeTabla);
		while((sd=readdir(dir))!=NULL){
			if (string_equals_ignore_case(sd->d_name, ".") || string_equals_ignore_case(sd->d_name, "..") ){continue;}
			liberarBloquesDeTmpYPart(sd->d_name,ruta);
		}
		rmdir(ruta);
		closedir(dir);
		sem_post(semaforoDeTabla);
		sem_wait(&mutexListaDeTablas);
		list_remove_and_destroy_by_condition(listaDeTablas,liberarTablaConEsteNombre,(void*) liberarMetadataConSemaforo);
		sem_post(&mutexListaDeTablas);
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
	sem_wait(&mutexListaDeTablas);
	t_list* listaAMandar = list_map(listaDeTablas,tranformarMetadataSinSemaforo);
	sem_post(&mutexListaDeTablas);
	serializarYEnviarPaqueteMetadatas(socket,listaAMandar);
	list_destroy_and_destroy_elements(listaAMandar,(void*) liberarMetadata);
}

void funcionDescribe(char* argumentos,int socket) {
	void loggearYMostrarTabla(metadataConSemaforo* unMetadata){
		soloLoggearResultados(socket,0,"RESULTADO DESCRIBE: La tabla: %s, tiene %d particion/es, consistencia= %d "
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
		sem_wait(&mutexListaDeTablas);
		int cantElementos = list_size(listaDeTablas);
		sem_post(&mutexListaDeTablas);
		if(cantElementos ==0){
			soloLoggearError(socket,"No hay tablas en el FS"); //caso a revisar, que no haya tablas en LFS
			if(socket!=-1) enviarError(socket);
			closedir(dir);
			free(rutaDirectorioTablas);
			return;
		}
		soloLoggear(socket,"Se cargaron todas las tablas del directorio");
		if(socket==-1){
			sem_wait(&mutexListaDeTablas);
			list_iterate(listaDeTablas,(void*)loggearYMostrarTabla);
			sem_post(&mutexListaDeTablas);
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
						operacionProtocolo protocolo = METADATA;
						enviar(socket, (void *) &protocolo, sizeof(operacionProtocolo));
						serializarYEnviarMetadata(socket,metadataBuscado);
						soloLoggearResultados(socket,0,"Resultado DESCRIBE La tabla: %s, tiene %d particion/es, consistencia= %d y tiempo de compactacion= %d \n",metadataBuscado->nombreTabla,metadataBuscado->cantParticiones,metadataBuscado->tipoConsistencia,metadataBuscado->tiempoCompactacion);
						liberarMetadata(metadataBuscado);
						return;
					}
					soloLoggearResultados(socket,0,"Resultado DESCRIBE La tabla: %s, tiene %d particion/es, consistencia= %d y tiempo de compactacion= %d \n",metadataBuscado->nombreTabla,metadataBuscado->cantParticiones,metadataBuscado->tipoConsistencia,metadataBuscado->tiempoCompactacion);
					soloLoggear(socket,"La tabla: %s, tiene %d particion/es, consistencia= %d y tiempo de compactacion= %d \n",metadataBuscado->nombreTabla,metadataBuscado->cantParticiones,metadataBuscado->tipoConsistencia,metadataBuscado->tiempoCompactacion);
					liberarMetadata(metadataBuscado);
		}
		else {
			soloLoggearError(socket,"No se encontro el metadata buscado");
			if(socket!=-1) enviarError(socket);
			return;
		}
	}
}
