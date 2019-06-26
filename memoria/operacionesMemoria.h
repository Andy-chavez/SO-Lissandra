/*
 * operacionesMemoria.c
 *
 *  Created on: 4 may. 2019
 *      Author: utnso
 */


#include "structsYVariablesGlobales.h"
#include <unistd.h>
#include <stdarg.h>

// DECLARACIONES //
void inicializarProcesoMemoria();
void inicializarArchivos();
void inicializarSemaforos();
void inicializarRetardos();
void inicializarTablaMarcos();
void inicializarTablaGossip();
memoria* inicializarMemoria(datosInicializacion* datosParaInicializar);
void liberarSegmento(void* unSegmento);
void liberarConfigYLogs();
void liberarMemoria();
void liberarTablaGossip();
void vaciarMemoria();
void liberarTablaMarcos();
int calcularEspacioParaUnRegistro(int tamanioMaximo);
marco* encontrarMarcoEscrito(int numeroMarco);
marco* algoritmoLRU();
marco* encontrarEspacio(int socketKernel);
bufferDePagina *armarBufferDePagina(registroConNombreTabla* unRegistro, int tamanioValueMaximo);
void liberarbufferDePagina(bufferDePagina* buffer);
void guardarEnMarco(marco* unMarco, bufferDePagina* bufferAGuardar);
marco* guardar(registroConNombreTabla* unRegistro,int socketKernel);
int guardarEnMemoria(registroConNombreTabla* unRegistro,int socketKernel);
registro* leerDatosEnMemoria(paginaEnTabla* unaPagina);
void cambiarDatosEnMemoria(paginaEnTabla* registroACambiar, registro* registroNuevo);
void* pedirALFS(operacionLQL *operacion);
registroConNombreTabla* pedirRegistroLFS(operacionLQL *operacion);
paginaEnTabla* crearPaginaParaSegmento(int numeroPagina, registro* unRegistro, int deDondeVengo, int socketKernel);
int agregarSegmento(registro* primerRegistro,char* tabla, int deDondeVengo, int socketKernel);
int agregarSegmentoConNombreDeLFS(registroConNombreTabla* registroLFS, int deDondeVengo, int socketKernel);
bool agregarPaginaEnSegmento(segmento* unSegmento, registro* unRegistro, int socketKernel, int deDondeVengo);
void liberarParametrosSpliteados(char** parametrosSpliteados);
void* obtenerValorDe(char** parametros, int lugarDelParametroBuscado);
registro* crearRegistroNuevo(char** parametros, int tamanioMaximoValue);
void dropearSegmento(segmento* unSegmento);
void liberarRegistro(registro* unRegistro);
bool tienenIgualNombre(char* unNombre,char* otroNombre);
segmento* encontrarSegmentoPorNombre(char* tablaNombre);
bool igualKeyRegistro(paginaEnTabla* unRegistro,int keyDada);
paginaEnTabla* encontrarRegistroPorKey(segmento* unSegmento, int keyDada);
char* valueRegistro(paginaEnTabla* paginaEncontrada);
void enviarYOLogearAlgo(int socket, char *mensaje, void(*log)(t_log *, char *), va_list parametrosAdicionales);
void enviarYLogearMensajeError(int socket, char* mensaje, ...);
void enviarOMostrarYLogearInfo(int socket, char* mensaje, ...);
void liberarRecursosSelectLQL(char* nombreTabla, char *key);
void selectLQL(operacionLQL *operacionSelect, int socketKernel);
void liberarRecursosInsertLQL(char* nombreTabla, registro* unRegistro);
void insertLQL(operacionLQL* operacionInsert, int socketKernel);
operacionLQL* armarInsertLQLParaPaquete(char* nombreTablaPerteneciente, paginaEnTabla* unaPagina);
void modificarValorJournalRealizandose(int valor);
hiloEnTabla* obtenerHiloEnTabla(pthread_t hilo);
void marcarHiloRealizandoOperacionEnMemoria(pthread_t hilo);
void marcarHiloComoOperacionRealizada(pthread_t hilo);
void journalLQL(int socketKernel);
void createLQL(operacionLQL* operacionCreate, int socketKernel);
void describeLQL(operacionLQL* operacionDescribe, int socketKernel);
void dropLQL(operacionLQL* operacionDrop, int socketKernel);
void cargarSeeds();
bool sonSeedsIguales(seed* unaSeed, seed* otraSeed);
void pedirTablaGossip(int socketMemoria);
void recibirYGuardarEnTablaGossip(int socketMemoria);
void intentarConexiones();
void* timedGossip();
void* timedJournal();
void agregarHiloAListaDeHilos();
void eliminarHiloDeListaDeHilos();
void esperarAHilosEjecutandose();
void dejarEjecutarOperacionesDeNuevo();

// ------------------------------------------------------------------------ //
// 1) INICIALIZACIONES Y FINALIZACIONES //

void inicializarProcesoMemoria() {
	inicializarArchivos();
	inicializarSemaforos();
	inicializarRetardos();
}

void inicializarRetardos() {
	RETARDO_GOSSIP = config_get_int_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "RETARDO_GOSSIPING");
	RETARDO_JOURNAL = config_get_int_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "RETARDO_JOURNAL");
	RETARDO_MEMORIA = config_get_int_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "RETARDO_MEM");
}

void inicializarTablaMarcos() {
	TABLA_MARCOS = list_create();
	int cantidadMaximaDeMarcos = config_get_int_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "TAM_MEM")/TAMANIO_UN_REGISTRO_EN_MEMORIA;
	int i;

	for(i = 0; i < cantidadMaximaDeMarcos; i++) {
		marco* nuevoMarco = malloc(sizeof(marco));
		nuevoMarco->estaEnUso = 0;
		nuevoMarco->marco = i;
		nuevoMarco->lugarEnMemoria = (MEMORIA_PRINCIPAL->base) + ((i)*TAMANIO_UN_REGISTRO_EN_MEMORIA);
		list_add(TABLA_MARCOS, nuevoMarco);
	}
}

void inicializarTablaGossip() {
	seed* seedPropia = malloc(sizeof(seed));
	seedPropia->ip = config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "IP_MEMORIA");
	seedPropia->puerto = config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "PUERTO");
	seedPropia->numero = config_get_int_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "MEMORY_NUMBER");

	TABLA_GOSSIP = list_create();
	list_add(TABLA_GOSSIP, seedPropia);
}

memoria* inicializarMemoria(datosInicializacion* datosParaInicializar) {
	memoria* nuevaMemoria = malloc(sizeof(memoria));
	int tamanioMemoria = config_get_int_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "TAM_MEM");

	nuevaMemoria->base = malloc(tamanioMemoria);
	memset(nuevaMemoria->base, 0, tamanioMemoria); // para que se vea linda jajajajaja
	nuevaMemoria->limite = nuevaMemoria->base + tamanioMemoria;
	nuevaMemoria->tamanioMaximoValue = datosParaInicializar->tamanio;
	nuevaMemoria->tablaSegmentos = list_create();

	TAMANIO_UN_REGISTRO_EN_MEMORIA = calcularEspacioParaUnRegistro(nuevaMemoria->tamanioMaximoValue);

	if(nuevaMemoria == NULL || nuevaMemoria->base == NULL) {
		log_error(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "hubo un error al inicializar la memoria");
		return NULL;
	}

	inicializarTablaGossip();
	TABLA_THREADS = list_create();

	log_info(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "Memoria inicializada.");
	return nuevaMemoria;
}

void inicializarArchivos() {
	ARCHIVOS_DE_CONFIG_Y_LOG = malloc(sizeof(configYLogs));
	ARCHIVOS_DE_CONFIG_Y_LOG->logger = log_create("memoria.log", "MEMORIA", 0, LOG_LEVEL_INFO);
	ARCHIVOS_DE_CONFIG_Y_LOG->config = config_create("memoria.config");
	if(!ARCHIVOS_DE_CONFIG_Y_LOG->config) {
		log_error(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "Hubo un error al abrir el archivo de config");
	}
	LOGGER_CONSOLA = log_create("memoria_consola.log", "MEMORIA_CONSOLE", 0, LOG_LEVEL_INFO);
}

void inicializarSemaforos() {
	sem_init(&BINARIO_SOCKET_KERNEL, 0, 1);
	sem_init(&MUTEX_LOG, 0, 1);
	sem_init(&MUTEX_SOCKET_LFS, 0, 1);
	sem_init(&MUTEX_RETARDO_MEMORIA, 0, 1);
	sem_init(&MUTEX_RETARDO_GOSSIP, 0, 1);
	sem_init(&MUTEX_RETARDO_JOURNAL, 0, 1);
	sem_init(&MUTEX_LOG_CONSOLA, 0, 1);
	sem_init(&MUTEX_TABLA_GOSSIP, 0, 1);
	sem_init(&BINARIO_FINALIZACION_PROCESO, 0, 0);
	sem_init(&MUTEX_TABLA_THREADS, 0, 1);
	sem_init(&MUTEX_JOURNAL_REALIZANDOSE, 0, 1);
	sem_init(&MUTEX_TABLA_MARCOS, 0, 1);
	sem_init(&MUTEX_TABLA_SEGMENTOS, 0, 1);

}

void liberarSegmento(void* segmentoEnMemoria) {

	segmento* unSegmento = (segmento*) segmentoEnMemoria;

	void liberarPaginas(void* paginaEnLaTabla) {
		free((paginaEnTabla*) paginaEnLaTabla);
	}

	sem_wait(&unSegmento->mutexSegmento);
		free(unSegmento->nombreTabla);
		list_destroy_and_destroy_elements(unSegmento->tablaPaginas, liberarPaginas);
	sem_post(&unSegmento->mutexSegmento);
		free(unSegmento);

}

void liberarConfigYLogs() {
	log_destroy(ARCHIVOS_DE_CONFIG_Y_LOG->logger);
	log_destroy(LOGGER_CONSOLA);
	config_destroy(ARCHIVOS_DE_CONFIG_Y_LOG->config);
	free(ARCHIVOS_DE_CONFIG_Y_LOG);

}

void liberarMemoria() {
	free(MEMORIA_PRINCIPAL->base);
	list_destroy_and_destroy_elements(MEMORIA_PRINCIPAL->tablaSegmentos, liberarSegmento);
	free(MEMORIA_PRINCIPAL);
}

void liberarTablaGossip() {
	list_destroy_and_destroy_elements(TABLA_GOSSIP, liberarSeed);
}

void vaciarMemoria() {
	void marcarMarcoComoDisponible(void* marcoAMarcar) {
		marco* unMarco = (marco*) marcoAMarcar;
		unMarco->estaEnUso = 0;
	}
	sem_wait(&MUTEX_TABLA_MARCOS);
	list_iterate(TABLA_MARCOS, marcarMarcoComoDisponible);
	sem_post(&MUTEX_TABLA_MARCOS);

	size_t tamanioMemoria = MEMORIA_PRINCIPAL->limite - MEMORIA_PRINCIPAL->base;

	memset(MEMORIA_PRINCIPAL->base, 0, tamanioMemoria); // para que se vea lindo despues de hacer el journal tambien

	sem_wait(&MUTEX_TABLA_SEGMENTOS);
	list_destroy_and_destroy_elements(MEMORIA_PRINCIPAL->tablaSegmentos, liberarSegmento);
	MEMORIA_PRINCIPAL->tablaSegmentos = list_create();
	sem_post(&MUTEX_TABLA_SEGMENTOS);
}

void liberarTablaMarcos() {
	void destructorMarcos(void* unMarco) {
		free((marco*) unMarco);
	}
	list_destroy_and_destroy_elements(TABLA_MARCOS, destructorMarcos);
}
// ------------------------------------------------------------------------ //
// 2) OPERACIONES SOBRE MEMORIA PRINCIPAL //

int calcularEspacioParaUnRegistro(int tamanioMaximo) {
	int timeStamp = sizeof(time_t);
	int key = sizeof(uint16_t);
	int value = tamanioMaximo;
	return timeStamp + key + value;
}

marco* encontrarMarcoEscrito(int numeroMarco) {
	marco* marco = list_get(TABLA_MARCOS, numeroMarco);
	return marco;
}

marco* algoritmoLRU() {
	paginaEnTabla* paginaACambiar = NULL;

	void compararTimestamp(void* paginaAComparar) {
		paginaEnTabla* pagina = (paginaEnTabla*) paginaAComparar;

		if(pagina->flag == NO) {
			if(paginaACambiar == NULL) {
				paginaACambiar = pagina;
			} else if(pagina->timestamp < paginaACambiar->timestamp){
				paginaACambiar = pagina;
			}
		}
	}

	void iterarSegmento(void* unSegmento){
		list_iterate(((segmento*) unSegmento)->tablaPaginas, compararTimestamp);
	}

	list_iterate(MEMORIA_PRINCIPAL->tablaSegmentos, iterarSegmento);


		if(paginaACambiar == NULL){
			return NULL;
		}

	return encontrarMarcoEscrito(paginaACambiar->marco);

}

marco* encontrarEspacio(int socketKernel) {
	bool encontrarLibreEnTablaMarcos(void* unMarcoEnTabla) {
		return !(((marco*) unMarcoEnTabla)->estaEnUso);
	}
	marco* marcoLibre;

	if(!(marcoLibre = list_find(TABLA_MARCOS, encontrarLibreEnTablaMarcos))) {
		enviarOMostrarYLogearInfo(-1, "No se encontro espacio libre en la tabla de marcos. Empezando algoritmo LRU");
		if(!(marcoLibre = algoritmoLRU())) {
			if(socketKernel == -1) {
				enviarOMostrarYLogearInfo(-1, "el algoritmo LRU no pudo liberar memoria. Por favor, ingrese JOURNAL para liberar la memoria.");
				return NULL;
			}
			enviarOMostrarYLogearInfo(-1, "el algoritmo LRU no pudo liberar memoria. empezando proceso de journal");
			char* lleno = "FULL";

			enviar(socketKernel, (void*) lleno , strlen(lleno) + 1);
			return NULL;
		}
	}

	return marcoLibre;
}

bufferDePagina *armarBufferDePagina(registroConNombreTabla* unRegistro, int tamanioValueMaximo) {
	bufferDePagina* buffer = malloc(sizeof(bufferDePagina));
	int tamanioValueRegistro = strlen(unRegistro->value) + 1;
	buffer->tamanio = sizeof(time_t) + sizeof(uint16_t) + strlen(unRegistro->value) + 1;
	buffer->buffer = malloc(buffer->tamanio);

	memcpy(buffer->buffer, &(unRegistro->timestamp), sizeof(time_t));
	int desplazamiento = sizeof(time_t);

	memcpy(buffer->buffer + desplazamiento, &(unRegistro->key), sizeof(uint16_t));
	desplazamiento += sizeof(uint16_t);

	memcpy(buffer->buffer + desplazamiento, unRegistro->value, tamanioValueRegistro);
	desplazamiento += tamanioValueRegistro;

	return buffer;
}

void liberarbufferDePagina(bufferDePagina* buffer) {
	free(buffer->buffer);
	free(buffer);
}

void guardarEnMarco(marco* unMarco, bufferDePagina* bufferAGuardar) {
	memcpy(unMarco->lugarEnMemoria, bufferAGuardar->buffer, bufferAGuardar->tamanio);
	unMarco->tamanioValue = bufferAGuardar->tamanio - sizeof(time_t) - sizeof(uint16_t); // restando estos tipos obtenemos el tamanio del value.
}

marco* guardar(registroConNombreTabla* unRegistro,int socketKernel) {
	bufferDePagina *bufferAGuardar = armarBufferDePagina(unRegistro, MEMORIA_PRINCIPAL->tamanioMaximoValue);

	marco *guardarEn = encontrarEspacio(socketKernel);

	if(!guardarEn) {
		return NULL;
	}

	guardarEnMarco(guardarEn, bufferAGuardar);
	guardarEn->estaEnUso = 1;

	liberarbufferDePagina(bufferAGuardar);
	return guardarEn;
}

int guardarEnMemoria(registroConNombreTabla* unRegistro,int socketKernel) {
	marco* marcoGuardado = guardar(unRegistro,socketKernel);
		if(marcoGuardado == NULL){
			return -1;
		}
	return marcoGuardado->marco;
}

registro* leerDatosEnMemoria(paginaEnTabla* unaPagina) {
	registro* registroARetornar = malloc(sizeof(registro));

	sem_wait(&MUTEX_TABLA_MARCOS);
	marco* marcoEnMemoria = encontrarMarcoEscrito(unaPagina->marco);

	memcpy(&(registroARetornar->timestamp),(time_t*) marcoEnMemoria->lugarEnMemoria, sizeof(time_t));
	int desplazamiento = sizeof(time_t);

	memcpy(&(registroARetornar->key), (uint16_t*) (marcoEnMemoria->lugarEnMemoria + desplazamiento), sizeof(uint16_t));
	desplazamiento += sizeof(uint16_t);

	registroARetornar->value = malloc(marcoEnMemoria->tamanioValue);
	memcpy(registroARetornar->value, (marcoEnMemoria->lugarEnMemoria + desplazamiento), marcoEnMemoria->tamanioValue);

	sem_post(&MUTEX_TABLA_MARCOS);
	return registroARetornar;
}

void cambiarDatosEnMemoria(paginaEnTabla* registroACambiar, registro* registroNuevo) {
	bufferDePagina* bufferParaCambio = armarBufferDePagina((registroConNombreTabla*) registroNuevo, MEMORIA_PRINCIPAL->tamanioMaximoValue);

	sem_wait(&MUTEX_TABLA_MARCOS);
	marco* marcoACambiarValue = list_get(TABLA_MARCOS, registroACambiar->marco);
	guardarEnMarco(marcoACambiarValue, bufferParaCambio);
	sem_post(&MUTEX_TABLA_MARCOS);

	liberarbufferDePagina(bufferParaCambio);
}

// ------------------------------------------------------------------------ //
// 3) OPERACIONES CON LISSANDRA FILE SYSTEM //

void* pedirALFS(operacionLQL *operacion) {
	sem_wait(&MUTEX_SOCKET_LFS);
	serializarYEnviarOperacionLQL(SOCKET_LFS, operacion);
	void* buffer = recibir(SOCKET_LFS);
	if(buffer == NULL) {
		enviarOMostrarYLogearInfo(-1, "Lissandra File System se ha desconectado");
	}
	sem_post(&MUTEX_SOCKET_LFS);
	return buffer;
}

registroConNombreTabla* pedirRegistroLFS(operacionLQL *operacion) {
	void* bufferRegistroConTabla = pedirALFS(operacion);

	if(bufferRegistroConTabla == NULL) {
		return NULL;
	}

	registroConNombreTabla* paginaEncontradaEnLFS = deserializarRegistro(bufferRegistroConTabla);

	if(atoi(paginaEncontradaEnLFS->nombreTabla)) {
		return NULL;
	}

	return paginaEncontradaEnLFS;
}

// ------------------------------------------------------------------------ //
// 4) OPERACIONES SOBRE LISTAS, SEGMENTOS Y PAGINAS //

paginaEnTabla* crearPaginaParaSegmento(int numeroPagina, registro* unRegistro, int deDondeVengo, int socketKernel) { // deDondevengo insert= 1 ,select=0
	paginaEnTabla* pagina = malloc(sizeof(paginaEnTabla));

	sem_wait(&MUTEX_TABLA_MARCOS); //TODO Este semaforo es muy gordo xdxd
	int marco = guardarEnMemoria((registroConNombreTabla*) unRegistro,socketKernel);
	sem_post(&MUTEX_TABLA_MARCOS);

	if(marco == -1) {
		// TODO Avisar que no se pudo guardar en memoria.
		return NULL;
	};

	pagina->marco = marco;
	pagina->numeroPagina = numeroPagina;
	pagina->timestamp = time(NULL);

	if(deDondeVengo == 0){ // es un select
		pagina->flag = NO;
	} else if (deDondeVengo == 1) { // es un insert
		pagina->flag = SI;
	}

	return pagina;
}

int agregarSegmento(registro* primerRegistro,char* tabla, int deDondeVengo, int socketKernel){

	paginaEnTabla* primeraPagina = crearPaginaParaSegmento(0, primerRegistro,deDondeVengo, socketKernel);

	if(!primeraPagina) {
		return 0;
	}

	segmento* segmentoNuevo = malloc(sizeof(segmento));
	segmentoNuevo->nombreTabla = string_duplicate(tabla);
	segmentoNuevo->tablaPaginas = list_create();
	sem_init(&segmentoNuevo->mutexSegmento, 0, 1);

	sem_wait(&MUTEX_TABLA_SEGMENTOS);
	list_add(MEMORIA_PRINCIPAL->tablaSegmentos, segmentoNuevo);
	list_add(segmentoNuevo->tablaPaginas, primeraPagina); // Por si hacen un select justo de esa primer pagina, que se cargue el segmento nuevo con la primer pagina antes de liberar la tablaSegmentos
	sem_post(&MUTEX_TABLA_SEGMENTOS);

	return 1;
}


int agregarSegmentoConNombreDeLFS(registroConNombreTabla* registroLFS, int deDondeVengo, int socketKernel) {
	return agregarSegmento((registro*) registroLFS, registroLFS->nombreTabla,deDondeVengo, socketKernel);
}

bool agregarPaginaEnSegmento(segmento* unSegmento, registro* unRegistro, int socketKernel, int deDondeVengo) {
	sem_wait(&unSegmento->mutexSegmento);
	paginaEnTabla* paginaParaAgregar = crearPaginaParaSegmento(list_size(unSegmento->tablaPaginas), unRegistro, deDondeVengo, socketKernel);
	sem_post(&unSegmento->mutexSegmento);

	if(!paginaParaAgregar) {
		enviarYLogearMensajeError(-1, "ERROR: No se pudo guardar el registro en la memoria");
		return false;
	}

	sem_wait(&unSegmento->mutexSegmento);
	list_add(unSegmento->tablaPaginas, paginaParaAgregar);
	sem_post(&unSegmento->mutexSegmento);

	enviarOMostrarYLogearInfo(socketKernel, "Se inserto exitosamente.");
	return true;
}

void liberarParametrosSpliteados(char** parametrosSpliteados) {
	int i = 0;
	while(*(parametrosSpliteados + i)) {
		free(*(parametrosSpliteados + i));
		i++;
	}
	free(parametrosSpliteados);
}

void* obtenerValorDe(char** parametros, int lugarDelParametroBuscado) {
	char* parametroBuscado = string_duplicate(*(parametros + lugarDelParametroBuscado));
	return (void*) parametroBuscado;
}

registro* crearRegistroNuevo(char** parametros, int tamanioMaximoValue) {
	registro* nuevoRegistro = malloc(sizeof(registro));

	char* aux = (char*) obtenerValorDe(parametros, 2);
	nuevoRegistro->value = string_trim_quotation(aux);
	if(strlen(nuevoRegistro->value + 1) > tamanioMaximoValue){
		liberarRegistro(nuevoRegistro);
		return NULL;
	};

	nuevoRegistro->timestamp = time(NULL);
	char* key = (char*) obtenerValorDe(parametros, 1);
	nuevoRegistro->key = atoi(key);

	free(key);
	return nuevoRegistro;
}

void dropearSegmento(segmento* unSegmento) {
	void liberarMarcoYPagina(void* unaPagina) {
		marco* marcoDePagina = list_get(TABLA_MARCOS,((paginaEnTabla*) unaPagina)->marco);
		marcoDePagina->estaEnUso = 0;

		free(unaPagina);
	}

	bool igualNombreSegmento(void* otroSegmento){
		return unSegmento->nombreTabla == ((segmento*) otroSegmento)->nombreTabla;
	}

	sem_wait(&MUTEX_TABLA_MARCOS);
	sem_wait(&unSegmento->mutexSegmento);
	list_destroy_and_destroy_elements(unSegmento->tablaPaginas, liberarMarcoYPagina);
	sem_post(&unSegmento->mutexSegmento);
	sem_post(&MUTEX_TABLA_MARCOS);

	sem_wait(&MUTEX_TABLA_SEGMENTOS);
	list_remove_by_condition(MEMORIA_PRINCIPAL->tablaSegmentos,igualNombreSegmento);
	sem_post(&MUTEX_TABLA_SEGMENTOS);

	free(unSegmento->nombreTabla);
	free(unSegmento);
}

void liberarRegistro(registro* unRegistro) {
	free(unRegistro->value);
	free(unRegistro);
}

bool tienenIgualNombre(char* unNombre,char* otroNombre){
	return string_equals_ignore_case(unNombre, otroNombre);
}

segmento* encontrarSegmentoPorNombre(char* tablaNombre){
	bool segmentoDeIgualNombre(segmento* unSegmento) {
		return tienenIgualNombre(unSegmento->nombreTabla, tablaNombre);
	}

	sem_wait(&MUTEX_TABLA_SEGMENTOS);
	segmento* segmentoARetornar = (segmento*) list_find(MEMORIA_PRINCIPAL->tablaSegmentos, (void*)segmentoDeIgualNombre);
	sem_post(&MUTEX_TABLA_SEGMENTOS);

	return segmentoARetornar;
}

bool igualKeyRegistro(paginaEnTabla* unRegistro,int keyDada){
	registro* registroReal = leerDatosEnMemoria(unRegistro);
	bool respuesta = registroReal->key == keyDada;

	liberarRegistro(registroReal);
	return respuesta;
}

paginaEnTabla* encontrarRegistroPorKey(segmento* unSegmento, int keyDada){
	bool tieneIgualKeyQueDada(paginaEnTabla* unRegistro) {
			return igualKeyRegistro(unRegistro, keyDada);
	}

	sem_wait(&unSegmento->mutexSegmento);
	paginaEnTabla* paginaEncontrada = (paginaEnTabla*) list_find(unSegmento->tablaPaginas,(void*)tieneIgualKeyQueDada);
	sem_post(&unSegmento->mutexSegmento);

	if(!paginaEncontrada) {
		return NULL;
	}
	paginaEncontrada->timestamp = time(NULL);

	return paginaEncontrada;
}

char* valueRegistro(paginaEnTabla* paginaEncontrada){

	registro* registroReal = leerDatosEnMemoria(paginaEncontrada);
	char* value = malloc(strlen(registroReal->value) + 1);
	memcpy(value, registroReal->value, strlen(registroReal->value) + 1);

	liberarRegistro(registroReal);
	return value;
}

void enviarYOLogearAlgo(int socket, char *mensaje, void(*log)(t_log *, char *), va_list parametrosAdicionales) {
	char* mensajeTotal = string_from_vformat(mensaje, parametrosAdicionales);
	if(socket != -1) {
		sem_wait(&MUTEX_LOG);
		log(ARCHIVOS_DE_CONFIG_Y_LOG->logger, mensajeTotal);
		sem_post(&MUTEX_LOG);
		enviar(socket, mensajeTotal, strlen(mensajeTotal) + 1);
	} else {
		sem_wait(&MUTEX_LOG_CONSOLA);
		log(LOGGER_CONSOLA, mensajeTotal);
		sem_post(&MUTEX_LOG_CONSOLA);
	}
	free(mensajeTotal);
}

void enviarYLogearMensajeError(int socket, char* mensaje, ...) {
	va_list parametrosAdicionales;
	va_start(parametrosAdicionales, mensaje);
	enviarYOLogearAlgo(socket, mensaje, (void*) log_error, parametrosAdicionales);
	va_end(parametrosAdicionales);
}

void enviarOMostrarYLogearInfo(int socket, char* mensaje, ...) {
	va_list parametrosAdicionales;
	va_start(parametrosAdicionales, mensaje);
	enviarYOLogearAlgo(socket, mensaje, (void*) log_info, parametrosAdicionales);
	va_end(parametrosAdicionales);
}

// ------------------------------------------------------------------------ //
// 6) OPERACIONESLQL //

operacionLQL* armarInsertLQLParaPaquete(char* nombreTablaPerteneciente, paginaEnTabla* unaPagina) {
	operacionLQL* operacionARetornar = malloc(sizeof(operacionLQL));
	registro* unRegistro = leerDatosEnMemoria(unaPagina);

	operacionARetornar->operacion = string_duplicate("INSERT");
	operacionARetornar->parametros = string_from_format("%s %d \"%s\" %d", nombreTablaPerteneciente, unRegistro->key, unRegistro->value, unRegistro->timestamp);

	liberarRegistro(unRegistro);
	return operacionARetornar;
}

void modificarValorJournalRealizandose(int valor) {
	sem_wait(&MUTEX_JOURNAL_REALIZANDOSE);
	JOURNAL_REALIZANDOSE = valor;
	sem_post(&MUTEX_JOURNAL_REALIZANDOSE);
}

hiloEnTabla* obtenerHiloEnTabla(pthread_t hilo) {
	bool esHiloPropio(void* unHilo) {
		return hilo == ((hiloEnTabla*) unHilo)->thread;
	}

	sem_wait(&MUTEX_TABLA_THREADS);
	hiloEnTabla* unHilo = (hiloEnTabla*) list_find(TABLA_THREADS, esHiloPropio);
	sem_post(&MUTEX_TABLA_THREADS);

	return unHilo;
}

void marcarHiloRealizandoOperacionEnMemoria(pthread_t hilo) {
	hiloEnTabla* hiloPropio = obtenerHiloEnTabla(hilo);

	sem_wait(&hiloPropio->semaforoThread); // este wait es para que el journal espere a este hilo que esta ejecutando.

	// En el caso en que el journal ya se esta ejecutando, tendra que esperar a que el journal termine de ejecutar. Por lo tanto espera de nuevo a su propio semaforo
	// (El journal lo liberara)

	sem_wait(&MUTEX_JOURNAL_REALIZANDOSE);
	if(JOURNAL_REALIZANDOSE) {
		sem_post(&MUTEX_JOURNAL_REALIZANDOSE);
		sem_wait(&hiloPropio->semaforoThread);
	} else {
		sem_post(&MUTEX_JOURNAL_REALIZANDOSE);
	}
}

void marcarHiloComoOperacionRealizada(pthread_t hilo) {
	hiloEnTabla* hiloPropio = obtenerHiloEnTabla(hilo);
	sem_post(&hiloPropio->semaforoThread);
}

void journalLQL(int socketKernel) {
	modificarValorJournalRealizandose(1);

	esperarAHilosEjecutandose();

	t_list* insertsAEnviar = list_create();

	void armarPaqueteParaEnviarALFS(void* segmentoParaArmar) {
		segmento* unSegmento = (segmento*) segmentoParaArmar;

		void agregarAPaqueteSiModificado(void* paginaParaArmar) {
			paginaEnTabla* unaPagina = (paginaEnTabla*) paginaParaArmar;
				if(unaPagina->flag) {
					operacionLQL* unaOperacion = armarInsertLQLParaPaquete(unSegmento->nombreTabla, unaPagina);
					list_add(insertsAEnviar, unaOperacion);
				}
			}

		list_iterate(unSegmento->tablaPaginas, agregarAPaqueteSiModificado);
	}

	list_iterate(MEMORIA_PRINCIPAL->tablaSegmentos, armarPaqueteParaEnviarALFS);

	operacionProtocolo protocoloJournal = PAQUETEOPERACIONES;

	sem_wait(&MUTEX_SOCKET_LFS);
	enviar(SOCKET_LFS, (void*)&protocoloJournal, sizeof(operacionProtocolo));
	serializarYEnviarPaqueteOperacionesLQL(SOCKET_LFS, insertsAEnviar);
	sem_post(&MUTEX_SOCKET_LFS);

	vaciarMemoria();

	list_destroy_and_destroy_elements(insertsAEnviar, liberarOperacionLQL);
	enviarOMostrarYLogearInfo(socketKernel, "Se realizo el Journal exitosamente.");

	modificarValorJournalRealizandose(0);
	dejarEjecutarOperacionesDeNuevo();
}

void liberarRecursosSelectLQL(char* nombreTabla, char *key) {
	free(nombreTabla);
	free(key);
}

void selectLQL(operacionLQL *operacionSelect, int socketKernel) {
	marcarHiloRealizandoOperacionEnMemoria(pthread_self());

	char** parametrosSpliteados = string_split(operacionSelect->parametros, " ");
	char* nombreTabla = (char*) obtenerValorDe(parametrosSpliteados, 0);
	char *keyString = (char*) obtenerValorDe(parametrosSpliteados, 1);
	uint16_t key = atoi(keyString);

	segmento* unSegmento = encontrarSegmentoPorNombre(nombreTabla);
	if(unSegmento){

	paginaEnTabla* paginaEncontrada = encontrarRegistroPorKey(unSegmento,key);

		if(paginaEncontrada){

			char* value = valueRegistro(paginaEncontrada);
			char *mensaje = string_new();
			string_append_with_format(&mensaje, "SELECT exitoso. Su valor es: %s", value);

			enviarOMostrarYLogearInfo(socketKernel, mensaje);
			free(value);
			free(mensaje);
		}
		else {
			// Pedir a LFS un registro para guardar el registro en segmento encontrado.

			registroConNombreTabla* registroLFS;
			if(!(registroLFS = pedirRegistroLFS(operacionSelect))) {
				enviarYLogearMensajeError(socketKernel, "ERROR: Por la operacion %s %s, No se encontro el registro en LFS, o hubo un error al buscarlo.", operacionSelect->operacion, operacionSelect->parametros);
			}
			else if(agregarPaginaEnSegmento(unSegmento,(registro*) registroLFS,socketKernel,0)) {
				enviar(socketKernel, (void*) registroLFS->value, strlen(registroLFS->value) + 1);
			}
			else {
				enviarYLogearMensajeError(socketKernel, "ERROR: Por la operacion %s %s, Hubo un error al guardar el registro LFS en la memoria.", operacionSelect->operacion, operacionSelect->parametros);
			}
		}
	}

	else {
		// pedir a LFS un registro para guardar registro con el nombre de la tabla.
		registroConNombreTabla* registroLFS = pedirRegistroLFS(operacionSelect);
		if(!(registroLFS = pedirRegistroLFS(operacionSelect))) {
			enviarYLogearMensajeError(socketKernel, "ERROR: Por la operacion %s %s, No se encontro el registro en LFS, o hubo un error al buscarlo.", operacionSelect->operacion, operacionSelect->parametros);
		}
		else if(agregarSegmentoConNombreDeLFS(registroLFS,0,socketKernel)){
			enviar(socketKernel, (void*) registroLFS->value, strlen(registroLFS->value) + 1);
			free(registroLFS->value);
			free(registroLFS->nombreTabla);
			free(registroLFS);
		}
		else {
			enviarYLogearMensajeError(socketKernel, "ERROR: Por la operacion %s %s, Hubo un error al agregar el segmento en la memoria.", operacionSelect->operacion, operacionSelect->parametros);
		}
	}
	// TODO else journal();


	liberarRecursosSelectLQL(nombreTabla, keyString);
	liberarParametrosSpliteados(parametrosSpliteados);
	marcarHiloComoOperacionRealizada(pthread_self());
}

void liberarRecursosInsertLQL(char* nombreTabla, registro* unRegistro) {
	free(nombreTabla);
	liberarRegistro(unRegistro);
}

void insertLQL(operacionLQL* operacionInsert, int socketKernel){
	marcarHiloRealizandoOperacionEnMemoria(pthread_self());

	char** parametrosSpliteados = string_n_split(operacionInsert->parametros, 3, " ");
	char* nombreTabla = (char*) obtenerValorDe(parametrosSpliteados, 0);
	registro* registroNuevo = crearRegistroNuevo(parametrosSpliteados, MEMORIA_PRINCIPAL->tamanioMaximoValue);

	if(!registroNuevo) {
		enviarYLogearMensajeError(socketKernel, "ERROR: El value %s es mayor al tamanio maximo del value posible. (Tamanio maximo posible: %d)", *(parametrosSpliteados+2), MEMORIA_PRINCIPAL->tamanioMaximoValue);
		free(nombreTabla);
		liberarParametrosSpliteados(parametrosSpliteados);
		return;
	}

	segmento* unSegmento = encontrarSegmentoPorNombre(nombreTabla);
	if(unSegmento){

		paginaEnTabla* paginaEncontrada = encontrarRegistroPorKey(unSegmento,registroNuevo->key);

		if(paginaEncontrada){
			cambiarDatosEnMemoria(paginaEncontrada, registroNuevo);
			paginaEncontrada->flag = SI;

			enviarOMostrarYLogearInfo(socketKernel, "Por la operacion %s %s, Se inserto exitosamente.", operacionInsert->operacion, operacionInsert->parametros);
		} else {
			if(agregarPaginaEnSegmento(unSegmento, registroNuevo, socketKernel,1)){
				enviarOMostrarYLogearInfo(socketKernel, "Por la operacion %s %s, Se inserto exitosamente.", operacionInsert->operacion, operacionInsert->parametros);
			} else {
				enviarYLogearMensajeError(socketKernel, "ERROR: Por la operacion %s %s, Hubo un error al agregar el segmento en la memoria.", operacionInsert->operacion, operacionInsert->parametros);
			}

		}
	}

	else {
		if(agregarSegmento(registroNuevo,nombreTabla,1, socketKernel)) {
			enviarOMostrarYLogearInfo(socketKernel, "Por la operacion %s %s, Se inserto exitosamente.", operacionInsert->operacion, operacionInsert->parametros);
		} else {
			enviarYLogearMensajeError(socketKernel, "ERROR: Por la operacion %s %s, Hubo un error al agregar el segmento en la memoria.", operacionInsert->operacion, operacionInsert->parametros);
		};
	}
	// TODO else journal();
	liberarParametrosSpliteados(parametrosSpliteados);
	liberarRecursosInsertLQL(nombreTabla, registroNuevo);
	marcarHiloComoOperacionRealizada(pthread_self());
}

void createLQL(operacionLQL* operacionCreate, int socketKernel) {
	char* mensaje = (char*) pedirALFS(operacionCreate);

	if(!mensaje) {
		enviarYLogearMensajeError(socketKernel, "ERROR: Hubo un error al pedir al LFS que realizara CREATE %s", operacionCreate->parametros);
	}

	else {
	enviarOMostrarYLogearInfo(socketKernel, mensaje);

	free(mensaje);
	}
}

void describeLQL(operacionLQL* operacionDescribe, int socketKernel) {
	void* bufferMetadata = pedirALFS(operacionDescribe);

	if(!bufferMetadata) {
		enviarYLogearMensajeError(socketKernel, "ERROR: Hubo un error al pedir al LFS que realizara DESCRIBE %s", operacionDescribe->parametros);
		return;
	}

	metadata* unaMetadata = deserializarMetadata(bufferMetadata);

	serializarYEnviarMetadata(socketKernel, unaMetadata);

	free(unaMetadata->nombreTabla);
	free(unaMetadata);
}

void dropLQL(operacionLQL* operacionDrop, int socketKernel) {
	marcarHiloRealizandoOperacionEnMemoria(pthread_self());

	segmento* unSegmento = encontrarSegmentoPorNombre(operacionDrop->parametros);

	if(unSegmento) {
		dropearSegmento(unSegmento);
		sem_wait(&MUTEX_LOG_CONSOLA);
		log_info(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "Se ha dropeado el segmento %s de la memoria. enviando al LFS para que dropee la tabla...", operacionDrop->parametros);
		sem_post(&MUTEX_LOG_CONSOLA);
	} else {
		sem_wait(&MUTEX_LOG_CONSOLA);
		log_info(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "El segmento %s no existe en la memoria. enviando al LFS para que dropee la tabla...", operacionDrop->parametros);
		sem_post(&MUTEX_LOG_CONSOLA);
	}

	char* mensaje = (char*) pedirALFS(operacionDrop);
	if(!mensaje) {
		enviarYLogearMensajeError(socketKernel, "ERROR: Hubo un error al pedir al LFS que realizara DROP");
	}
	else {
		enviarOMostrarYLogearInfo(socketKernel, mensaje);

		free(mensaje);
	}

	marcarHiloComoOperacionRealizada(pthread_self());
}
// ------------------------------------------------------------------------ //
// 7) TIMED OPERATIONS //

void cargarSeeds() {
	char** IPs = config_get_array_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "IP_SEEDS");
	char** puertos = config_get_array_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "PUERTO_SEEDS");

	int i = 0;
	while(*(IPs + i) != NULL && *(puertos + i) != NULL) {
		seed *unaSeed = malloc(sizeof(seed));
		unaSeed->ip = string_duplicate(*(IPs + i));
		unaSeed->puerto = string_duplicate(*(puertos + i));
		unaSeed->numero = -1;

		sem_wait(&MUTEX_TABLA_GOSSIP);
		list_add(TABLA_GOSSIP, unaSeed);
		sem_post(&MUTEX_TABLA_GOSSIP);

		free(*(IPs + i));
		free(*(puertos + i));
		i++;
	}

	free(IPs);
	free(puertos);
}

bool sonSeedsIguales(seed* unaSeed, seed* otraSeed) {
	return string_equals_ignore_case(unaSeed->ip, otraSeed->ip) && string_equals_ignore_case(unaSeed->puerto, otraSeed->puerto);
}

void pedirTablaGossip(int socketMemoria) {
	operacionProtocolo protocolo = TABLAGOSSIP;
	enviar(socketMemoria, (void*) &protocolo, sizeof(operacionProtocolo));
}

void recibirYGuardarEnTablaGossip(int socketMemoria) {
	void guardarEnTablaGossip(seed* unaSeed) {
		// Duplicamos strings de IP y Puerto ya que la funcion de recibir libera la seed

		bool esIgualA(void* otraSeed) {
			return sonSeedsIguales(unaSeed,(seed*) otraSeed);
		}

		seed* seedEnTablaGossip = list_find(TABLA_GOSSIP, esIgualA);
		if(seedEnTablaGossip && seedEnTablaGossip->numero == -1) {

			seedEnTablaGossip->numero = unaSeed->numero;
			return;
		}
		else if (seedEnTablaGossip){
			return;
		}
		else {
			seed* seedAGuardar = malloc(sizeof(seed));
			seedAGuardar->ip = string_duplicate(unaSeed->ip);
			seedAGuardar->puerto = string_duplicate(unaSeed->puerto);
			seedAGuardar->numero = unaSeed->numero;

			list_add(TABLA_GOSSIP, seedAGuardar);
		}

	}

	pedirTablaGossip(socketMemoria);
	recibirYDeserializarTablaDeGossipRealizando(socketMemoria, guardarEnTablaGossip);
}

void intentarConexiones() {
	seed* seedPropia = malloc(sizeof(seed));
	seedPropia->ip = config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "IP_MEMORIA");
	seedPropia->puerto = config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "PUERTO");

	void intentarConexion(void* seedEnTablaGossip) {
		seed* unaSeed = (seed*) seedEnTablaGossip;

		bool esIgualA(void* otraSeed) {
			return sonSeedsIguales(unaSeed,(seed*) otraSeed);
		}

		if(sonSeedsIguales(seedPropia, unaSeed)) {
			return; // Para no hacer una conexion al pedo con la propia memoria.
		}

		int socketMemoria = crearSocketCliente(unaSeed->ip, unaSeed->puerto);

		sem_wait(&MUTEX_LOG_CONSOLA);
		log_info(LOGGER_CONSOLA, "Intentando conexion con la memoria de IP \"%s\" y puerto \"%s\"", unaSeed->ip, unaSeed->puerto);
		sem_post(&MUTEX_LOG_CONSOLA);

		if(socketMemoria == -1) {
			sem_wait(&MUTEX_LOG_CONSOLA);
			log_info(LOGGER_CONSOLA, "se cerro la conexion con esta IP y este puerto. Eliminando de la tabla gossip...");
			sem_post(&MUTEX_LOG_CONSOLA);

			list_remove_by_condition(TABLA_GOSSIP, esIgualA);

			return;
		}

		log_info(LOGGER_CONSOLA, "recibiendo tabla Gossip de la memoria de IP \"%s\" y puerto \"%s\"...", unaSeed->ip, unaSeed->puerto);
		recibirYGuardarEnTablaGossip(socketMemoria);
		cerrarConexion(socketMemoria);
	}

	sem_wait(&MUTEX_TABLA_GOSSIP);
	list_iterate(TABLA_GOSSIP, intentarConexion);
	sem_post(&MUTEX_TABLA_GOSSIP);
}

void* timedGossip() {
	cargarSeeds();

	while(1) {
		intentarConexiones();

		sem_wait(&MUTEX_RETARDO_GOSSIP);
		int retardoGossip = RETARDO_GOSSIP * 1000;
		sem_post(&MUTEX_RETARDO_GOSSIP);

		usleep(retardoGossip);
	}
}

void* timedJournal(){

	while(1){
		sem_wait(&MUTEX_RETARDO_JOURNAL);
		int retardoJournal = RETARDO_JOURNAL * 1000;
		sem_post(&MUTEX_RETARDO_JOURNAL);

		usleep(retardoJournal);

		journalLQL(-1);
	}
}

// ------------------------------------------------------------------------ //
// 9) LISTA DE HILOS //

void agregarHiloAListaDeHilos() {
	hiloEnTabla* hiloPropio = malloc(sizeof(hiloEnTabla));
	hiloPropio->thread = pthread_self();
	sem_init(&hiloPropio->semaforoThread, 0 , 1);

	sem_wait(&MUTEX_TABLA_THREADS);
	list_add(TABLA_THREADS, hiloPropio);
	sem_post(&MUTEX_TABLA_THREADS);
}

void eliminarHiloDeListaDeHilos() {
	bool esElPropioThread(void* hiloEnLaTabla) {
		hiloEnTabla* unHilo = (hiloEnTabla*) hiloEnLaTabla;
		if(unHilo->thread == pthread_self()) {
			free(unHilo);
			return true;
		}
		return false;
	}

	sem_wait(&MUTEX_TABLA_THREADS);
	list_remove_by_condition(TABLA_THREADS, esElPropioThread);
	sem_post(&MUTEX_TABLA_THREADS);
}

void esperarAHilosEjecutandose() {
	t_list* listaHilosEsperandoSemaforos = list_create();

	void esperarHiloEsperando(void* hiloEsperando) {
		if(pthread_join(*(pthread_t*) hiloEsperando, NULL)) {
			printf("hubo un error al joinear un hilo esperando un semaforo\n");
		}
	}

	void* esperarSemaforoDeHilo(void* buffer) {
		hiloEnTabla* hiloAEsperar = (hiloEnTabla*) buffer;

		sem_wait(&hiloAEsperar->semaforoThread);

		pthread_exit(0);
	}

	void crearHiloParaEsperar(void* unHilo) {
		pthread_t *hiloQueEspera = malloc(sizeof(pthread_t));
		pthread_create(hiloQueEspera, NULL, esperarSemaforoDeHilo, unHilo);
		list_add(listaHilosEsperandoSemaforos, (void*) hiloQueEspera);
	}

	sem_wait(&MUTEX_TABLA_THREADS);
	list_iterate(TABLA_THREADS, crearHiloParaEsperar);
	sem_post(&MUTEX_TABLA_THREADS);

	list_iterate(listaHilosEsperandoSemaforos, esperarHiloEsperando);
	list_destroy(listaHilosEsperandoSemaforos);
}

void dejarEjecutarOperacionesDeNuevo() {
	void postSemaforoDelHilo(void* hilo) {
		sem_post(&((hiloEnTabla*) hilo)->semaforoThread);
	}

	sem_wait(&MUTEX_TABLA_THREADS);
	list_iterate(TABLA_THREADS, postSemaforoDelHilo);
	sem_post(&MUTEX_TABLA_THREADS);
}
