/*
 * operacionesMemoria.c
 *
 *  Created on: 4 may. 2019
 *      Author: utnso
 */


#include "structsYVariablesGlobales.h"
#include <unistd.h>
#include <stdarg.h>
#include <fcntl.h>

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
marco* encontrarEspacio(int socketKernel, bool* seEjecutaraJournal);
bufferDePagina *armarBufferDePagina(registroConNombreTabla* unRegistro, int tamanioValueMaximo);
void liberarBufferDePagina(bufferDePagina* buffer);
void guardarEnMarco(marco* unMarco, bufferDePagina* bufferAGuardar);
marco* guardar(registroConNombreTabla* unRegistro,int socketKernel, bool* seEjecutaraJournal);
int guardarEnMemoria(registroConNombreTabla* unRegistro,int socketKernel, bool* seEjecutaraJournal);
registro* leerDatosEnMemoria(paginaEnTabla* unaPagina);
void cambiarDatosEnMemoria(paginaEnTabla* registroACambiar, registro* registroNuevo);
void* pedirALFS(operacionLQL *operacion);
registroConNombreTabla* pedirRegistroLFS(operacionLQL *operacion);
paginaEnTabla* crearPaginaParaSegmento(registro* unRegistro, int deDondeVengo, int socketKernel, bool* seEjecutaraJournal);
int agregarSegmento(registro* primerRegistro,char* tabla, int deDondeVengo, int socketKernel, bool* seEjecutaraJournal);
int agregarSegmentoConNombreDeLFS(registroConNombreTabla* registroLFS, int deDondeVengo, int socketKernel, bool* seEjecutaraJournal);
bool agregarPaginaEnSegmento(segmento* unSegmento, registro* unRegistro, int socketKernel, int deDondeVengo, bool* seEjecutaraJournal);
void liberarParametrosSpliteados(char** parametrosSpliteados);
void* obtenerValorDe(char** parametros, int lugarDelParametroBuscado);
registro* crearRegistroNuevo(char* value, char* timestamp, char* key, int tamanioMaximoValue);
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
void marcarHiloRealizandoSemaforo(sem_t *semaforoElegido);
void marcarHiloComoSemaforoRealizado(sem_t *semaforoElegido);
void journalLQL(int socketKernel);
void createLQL(operacionLQL* operacionCreate, int socketKernel);
void describeLQL(operacionLQL* operacionDescribe, int socketKernel);
void dropLQL(operacionLQL* operacionDrop, int socketKernel);
void cargarSeeds();
bool sonSeedsIguales(seed* unaSeed, seed* otraSeed);
void pedirTablaGossip(int socketMemoria);
void recibirYGuardarEnTablaGossip(int socketMemoria, int estoyPidiendo);
void intentarConexiones();
void* timedGossip();
void* timedJournal();
void agregarHiloAListaDeHilos();
void eliminarHiloDeListaDeHilos();
void* esperarSemaforoDeHilo(void* buffer);
void* esperarSemaforoDeCancelar(void* buffer);
void esperarAHilosEjecutandose(void* (*esperarSemaforoParticular)(void*));
void dejarEjecutarOperacionesDeNuevo();
void protocoloCancelar();
void cancelarListaHilos();
void cancelarJournal();
void cancelarGossiping();
void cancelarConfig();

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
	RETARDO_FS = config_get_int_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "RETARDO_FS");
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
		sem_init(&nuevoMarco->mutexMarco, 0, 1);
		list_add(TABLA_MARCOS, nuevoMarco);
	}
}

void inicializarTablaGossip() {
	seed* seedPropia = malloc(sizeof(seed));
	seedPropia->ip = string_duplicate(config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "IP_MEMORIA"));
	seedPropia->puerto = string_duplicate(config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "PUERTO"));
	seedPropia->numero = config_get_int_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "MEMORY_NUMBER");

	TABLA_GOSSIP = list_create();
	list_add(TABLA_GOSSIP, seedPropia);
}

void inicializarTablaSeedsConfig(){
	TABLA_SEEDS_CONFIG = list_create();
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
	inicializarTablaSeedsConfig();
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
	sem_init(&MUTEX_JOURNAL, 0, 1);
	sem_init(&BINARIO_SOCKET_KERNEL, 0, 1);
	sem_init(&MUTEX_RETARDO_FS, 0, 1);
	sem_init(&MUTEX_LOG, 0, 1);
	sem_init(&MUTEX_SOCKET_LFS, 0, 1);
	sem_init(&MUTEX_RETARDO_MEMORIA, 0, 1);
	sem_init(&MUTEX_RETARDO_GOSSIP, 0, 1);
	sem_init(&MUTEX_RETARDO_JOURNAL, 0, 1);
	sem_init(&MUTEX_LOG_CONSOLA, 0, 1);
	sem_init(&MUTEX_TABLA_GOSSIP, 0, 1);
	sem_init(&MUTEX_TABLA_SEEDS_CONFIG, 0, 1);
	sem_init(&BINARIO_FINALIZACION_PROCESO, 0, 0);
	sem_init(&MUTEX_TABLA_THREADS, 0, 1);
	sem_init(&MUTEX_JOURNAL_REALIZANDOSE, 0, 1);
	sem_init(&MUTEX_TABLA_MARCOS, 0, 1);
	sem_init(&MUTEX_TABLA_SEGMENTOS, 0, 1);
	sem_init(&MUTEX_CERRANDO_MEMORIA, 0, 1);
	sem_init(&BINARIO_ALGORITMO_LRU, 0, 1);
	sem_init(&BINARIO_HILO_EN_TABLA, 0, 1);
}

void liberarSegmento(void* segmentoEnMemoria) {

	segmento* unSegmento = (segmento*) segmentoEnMemoria;

	void liberarPaginas(void* paginaEnLaTabla) {
		paginaEnTabla* unaPagina = (paginaEnTabla*) paginaEnLaTabla;
		free(unaPagina);
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
		sem_wait(&unMarco->mutexMarco);
		unMarco->estaEnUso = 0;
		sem_post(&unMarco->mutexMarco);
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
		segmento* segmentoAIterar = (segmento*) unSegmento;

		sem_wait(&segmentoAIterar->mutexSegmento);
		list_iterate(segmentoAIterar->tablaPaginas, compararTimestamp);
		sem_post(&segmentoAIterar->mutexSegmento);
	}

	sem_wait(&MUTEX_TABLA_SEGMENTOS);
	list_iterate(MEMORIA_PRINCIPAL->tablaSegmentos, iterarSegmento);
	sem_post(&MUTEX_TABLA_SEGMENTOS);

		if(paginaACambiar == NULL){
			sem_post(&BINARIO_ALGORITMO_LRU);
			return NULL;
		}

	registro* registroAEliminar = leerDatosEnMemoria(paginaACambiar);
	enviarOMostrarYLogearInfo(-1, "RegistroAEliminar: %d, \"%s\", %d", registroAEliminar->key, registroAEliminar->value, registroAEliminar->timestamp);

	free(registroAEliminar->value);
	free(registroAEliminar);

	bool eliminarPaginaSeleccionadaPorLRU(void* unaPagina) {
		paginaEnTabla* paginaAEliminar = (paginaEnTabla*) unaPagina;
		return paginaACambiar->marco == paginaAEliminar->marco;
	}

	void encontrarYEliminarPagina(void* unSegmento) {
		segmento* segmentoConPaginaParaEliminar = (segmento*) unSegmento;
		sem_wait(&segmentoConPaginaParaEliminar->mutexSegmento);
		list_remove_by_condition(segmentoConPaginaParaEliminar->tablaPaginas, eliminarPaginaSeleccionadaPorLRU);
		sem_post(&segmentoConPaginaParaEliminar->mutexSegmento);
	}

	sem_wait(&MUTEX_TABLA_SEGMENTOS);
	list_iterate(MEMORIA_PRINCIPAL->tablaSegmentos, encontrarYEliminarPagina);
	sem_post(&MUTEX_TABLA_SEGMENTOS);

	int numeroMarcoDePaginaACambiar = paginaACambiar->marco;

	free(paginaACambiar);

	sem_post(&BINARIO_ALGORITMO_LRU);

	return encontrarMarcoEscrito(numeroMarcoDePaginaACambiar);
}

marco* encontrarEspacio(int socketKernel, bool* seEjecutaraJournal) {
	bool encontrarLibreEnTablaMarcos(void* unMarcoEnTabla) {
		marco* marcoEnTabla = (marco*) unMarcoEnTabla;
		sem_wait(&marcoEnTabla->mutexMarco);
		int estaEnUso = marcoEnTabla->estaEnUso;
		sem_post(&marcoEnTabla->mutexMarco);
		return !estaEnUso;
	}
	marco* marcoLibre;

	sem_wait(&BINARIO_ALGORITMO_LRU);
	sem_wait(&MUTEX_TABLA_MARCOS);

	if(!(marcoLibre = list_find(TABLA_MARCOS, encontrarLibreEnTablaMarcos))) {
		sem_post(&MUTEX_TABLA_MARCOS);
		enviarOMostrarYLogearInfo(-1, "No se encontro espacio libre en la tabla de marcos. Empezando algoritmo LRU");
		if(!(marcoLibre = algoritmoLRU())) {
			if(socketKernel == -1) {
				enviarOMostrarYLogearInfo(-1, "el algoritmo LRU no pudo liberar memoria. Por favor, ingrese JOURNAL para liberar la memoria.");
				return NULL;
			}
			enviarOMostrarYLogearInfo(-1, "el algoritmo LRU no pudo liberar memoria. empezando proceso de journal");
			char* lleno = "FULL";

			*seEjecutaraJournal = true;
			enviar(socketKernel, (void*) lleno , strlen(lleno) + 1);
			return NULL;
		}
	} else {
		sem_post(&BINARIO_ALGORITMO_LRU); // funca como mutex tambien pero es necesario
		sem_post(&MUTEX_TABLA_MARCOS);
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

void liberarBufferDePagina(bufferDePagina* buffer) {
	free(buffer->buffer);
	free(buffer);
}

void guardarEnMarco(marco* unMarco, bufferDePagina* bufferAGuardar) {
	memcpy(unMarco->lugarEnMemoria, bufferAGuardar->buffer, bufferAGuardar->tamanio);
	unMarco->tamanioValue = bufferAGuardar->tamanio - sizeof(time_t) - sizeof(uint16_t); // restando estos tipos obtenemos el tamanio del value.
}

marco* guardar(registroConNombreTabla* unRegistro,int socketKernel, bool* seEjecutaraJournal) {
	bufferDePagina *bufferAGuardar = armarBufferDePagina(unRegistro, MEMORIA_PRINCIPAL->tamanioMaximoValue);

	marco *guardarEn = encontrarEspacio(socketKernel, seEjecutaraJournal);

	if(!guardarEn) {
		liberarBufferDePagina(bufferAGuardar);
		return NULL;
	}

	sem_wait(&guardarEn->mutexMarco);
	guardarEnMarco(guardarEn, bufferAGuardar);
	guardarEn->estaEnUso = 1;
	sem_post(&guardarEn->mutexMarco);

	liberarBufferDePagina(bufferAGuardar);
	return guardarEn;
}

int guardarEnMemoria(registroConNombreTabla* unRegistro,int socketKernel, bool* seEjecutaraJournal) {
	marco* marcoGuardado = guardar(unRegistro,socketKernel, seEjecutaraJournal);
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

	liberarBufferDePagina(bufferParaCambio);
}

// ------------------------------------------------------------------------ //
// 3) OPERACIONES CON LISSANDRA FILE SYSTEM //

void* pedirALFS(operacionLQL *operacion) {
	sem_wait(&MUTEX_RETARDO_FS);
	int retardo = RETARDO_FS*1000;
	sem_post(&MUTEX_RETARDO_FS);
	usleep(retardo);

	sem_wait(&MUTEX_SOCKET_LFS);
	if(SOCKET_LFS == -1) {
		enviarOMostrarYLogearInfo(-1, "Se encuentra desconectado el LFS.");
		sem_post(&MUTEX_SOCKET_LFS);
		return NULL;
	}
	serializarYEnviarOperacionLQL(SOCKET_LFS, operacion);
	void* buffer = recibir(SOCKET_LFS);
	if(buffer == NULL) {
		enviarOMostrarYLogearInfo(-1, "Lissandra File System se ha desconectado");
		SOCKET_LFS = -1;
		sem_post(&MUTEX_SOCKET_LFS);
		return NULL;
	} else if(empezarDeserializacion(&buffer) == ERROR) {
		sem_post(&MUTEX_SOCKET_LFS);
		free(buffer);
		return NULL;
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

	return paginaEncontradaEnLFS;
}

// ------------------------------------------------------------------------ //
// 4) OPERACIONES SOBRE LISTAS, SEGMENTOS Y PAGINAS //

paginaEnTabla* crearPaginaParaSegmento(registro* unRegistro, int deDondeVengo, int socketKernel, bool* seEjecutaraJournal) { // deDondevengo insert= 1 ,select=0
	paginaEnTabla* pagina = malloc(sizeof(paginaEnTabla));

	int marco = guardarEnMemoria((registroConNombreTabla*) unRegistro, socketKernel, seEjecutaraJournal);

	if(marco == -1) {
		free(pagina);
		return NULL;
	};

	pagina->marco = marco;
	pagina->timestamp = time(NULL);

	if(deDondeVengo == 0){ // es un select
		pagina->flag = NO;
	} else if (deDondeVengo == 1) { // es un insert
		pagina->flag = SI;
	}

	return pagina;
}

int agregarSegmento(registro* primerRegistro,char* tabla, int deDondeVengo, int socketKernel, bool* seEjecutaraJournal){

	paginaEnTabla* primeraPagina = crearPaginaParaSegmento(primerRegistro,deDondeVengo, socketKernel, seEjecutaraJournal);

	if(!primeraPagina) {
		return 0;
	}

	segmento* segmentoNuevo = malloc(sizeof(segmento));
	segmentoNuevo->nombreTabla = string_duplicate(tabla);
	segmentoNuevo->tablaPaginas = list_create();
	sem_init(&segmentoNuevo->mutexSegmento, 0, 1);

	primeraPagina->numeroPagina = 0;

	sem_wait(&MUTEX_TABLA_SEGMENTOS);
	list_add(MEMORIA_PRINCIPAL->tablaSegmentos, segmentoNuevo);
	list_add(segmentoNuevo->tablaPaginas, primeraPagina); // Por si hacen un select justo de esa primer pagina, que se cargue el segmento nuevo con la primer pagina antes de liberar la tablaSegmentos
	sem_post(&MUTEX_TABLA_SEGMENTOS);

	return 1;
}


int agregarSegmentoConNombreDeLFS(registroConNombreTabla* registroLFS, int deDondeVengo, int socketKernel, bool* seEjecutaraJournal) {
	return agregarSegmento((registro*) registroLFS, registroLFS->nombreTabla,deDondeVengo, socketKernel, seEjecutaraJournal);
}

bool agregarPaginaEnSegmento(segmento* unSegmento, registro* unRegistro, int socketKernel, int deDondeVengo, bool* seEjecutaraJournal) {
	paginaEnTabla* paginaParaAgregar = crearPaginaParaSegmento(unRegistro, deDondeVengo, socketKernel, seEjecutaraJournal);

	if(!paginaParaAgregar) {
		enviarYLogearMensajeError(-1, "ERROR: No se pudo guardar el registro en la memoria");
		return false;
	}

	sem_wait(&unSegmento->mutexSegmento);
	paginaParaAgregar->numeroPagina = list_size(unSegmento->tablaPaginas);
	list_add(unSegmento->tablaPaginas, paginaParaAgregar);
	sem_post(&unSegmento->mutexSegmento);

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

registro* crearRegistroNuevo(char* value, char* timestamp, char* key, int tamanioMaximoValue) {
	registro* nuevoRegistro = malloc(sizeof(registro));

	nuevoRegistro->value = string_duplicate(value);
	if(strlen(nuevoRegistro->value) >= tamanioMaximoValue){
		liberarRegistro(nuevoRegistro);
		return NULL;
	}

	if(timestamp != NULL) {
		nuevoRegistro->timestamp = atoi(timestamp);
	} else {
		nuevoRegistro->timestamp = time(NULL);
	}
	nuevoRegistro->key = atoi(key);

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

	sem_wait(&unSegmento->mutexSegmento);
	sem_wait(&MUTEX_TABLA_MARCOS);
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

	paginaEnTabla* paginaEncontrada = (paginaEnTabla*) list_find(unSegmento->tablaPaginas,(void*)tieneIgualKeyQueDada);

	if(!paginaEncontrada) {
		return NULL;
	}

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
		return pthread_equal(hilo, ((hiloEnTabla*) unHilo)->thread);
	}

	sem_wait(&MUTEX_TABLA_THREADS);
	hiloEnTabla* unHilo = (hiloEnTabla*) list_find(TABLA_THREADS, esHiloPropio);
	sem_post(&MUTEX_TABLA_THREADS);

	return unHilo;
}

void marcarHiloRealizandoSemaforo(sem_t *semaforo) {
	// En el caso en que el journal ya se esta ejecutando, tendra que esperar a que el journal termine de ejecutar. Por lo tanto espera de nuevo a su propio semaforo
	// (El journal lo liberara)
	sem_wait(semaforo); // yeah idk why this is here
}

void verSiHayJournalEjecutandose(sem_t *semaforo) {
	sem_wait(&MUTEX_JOURNAL_REALIZANDOSE);
	if(JOURNAL_REALIZANDOSE) {
		sem_post(&MUTEX_JOURNAL_REALIZANDOSE);
		sem_wait(semaforo);
	} else {
		sem_post(&MUTEX_JOURNAL_REALIZANDOSE);
	}
}

void marcarHiloComoSemaforoRealizado(sem_t *semaforo) {
	sem_post(semaforo);
}

void journalLQL(int socketKernel) {
	// TODO nuevo semaforo para mutexear journals
	sem_wait(&MUTEX_JOURNAL); // Para que no se ejecute el timeado con otro que se ejecuto por kernel o consola
	modificarValorJournalRealizandose(1);

	esperarAHilosEjecutandose(esperarSemaforoDeHilo);
	t_log* logJournal = log_create("journal_inserts_enviados.log", "JOURNAL", 0, LOG_LEVEL_INFO);
	t_list* insertsAEnviar = list_create();
	log_info(logJournal, "Comenzando Journal, se enviaran los siguientes inserts:");

	void armarPaqueteParaEnviarALFS(void* segmentoParaArmar) {
		segmento* unSegmento = (segmento*) segmentoParaArmar;

		void agregarAPaqueteSiModificado(void* paginaParaArmar) {
			paginaEnTabla* unaPagina = (paginaEnTabla*) paginaParaArmar;
				if(unaPagina->flag == SI) {
					operacionLQL* unaOperacion = armarInsertLQLParaPaquete(unSegmento->nombreTabla, unaPagina);
					log_info(logJournal, "%s %s", unaOperacion->operacion, unaOperacion->parametros);
					list_add(insertsAEnviar, unaOperacion);
				}
			}

		list_iterate(unSegmento->tablaPaginas, agregarAPaqueteSiModificado);
	}

	list_iterate(MEMORIA_PRINCIPAL->tablaSegmentos, armarPaqueteParaEnviarALFS);

	operacionProtocolo protocoloJournal = PAQUETEOPERACIONES;

	enviarOMostrarYLogearInfo(-1, "Tomo semaforo mutex en Journal");
	sem_wait(&MUTEX_SOCKET_LFS);
	if(SOCKET_LFS == -1) {
		enviarOMostrarYLogearInfo(-1, "ERROR: No se puede enviar la informacion del Journal, ya que el LFS no se encuentra disponible.");
		enviarOMostrarYLogearInfo(-1, "Se pasara a borrar los datos en la memoria de todos modos. Se perderan los datos.");
	}else {
		enviar(SOCKET_LFS, (void*)&protocoloJournal, sizeof(operacionProtocolo)); // TODO VERIFICAR SI SOCKETLFS NO EXISTE YA
		serializarYEnviarPaqueteOperacionesLQL(SOCKET_LFS, insertsAEnviar);
	}
	sem_post(&MUTEX_SOCKET_LFS);
	enviarOMostrarYLogearInfo(-1, "libero semaforo mutex en journal.");

	vaciarMemoria();

	list_destroy_and_destroy_elements(insertsAEnviar, liberarOperacionLQL);
	enviarOMostrarYLogearInfo(socketKernel, "Se realizo el Journal exitosamente.");

	log_info(logJournal, "Journal Realizado.");
	log_destroy(logJournal);

	modificarValorJournalRealizandose(0);
	dejarEjecutarOperacionesDeNuevo();
	sem_post(&MUTEX_JOURNAL);
}

void liberarRecursosSelectLQL(char* nombreTabla, char *key) {
	free(nombreTabla);
	free(key);
}

void selectLQL(operacionLQL *operacionSelect, int socketKernel) {
	sem_t* semaforoDeOperacion = obtenerHiloEnTabla(pthread_self())->semaforoOperacion;
	marcarHiloRealizandoSemaforo(semaforoDeOperacion);
	verSiHayJournalEjecutandose(semaforoDeOperacion);

	bool seEjecutaraJournal = false;

	char** parametrosSpliteados = string_split(operacionSelect->parametros, " ");
	char* nombreTabla = (char*) obtenerValorDe(parametrosSpliteados, 0);
	char *keyString = (char*) obtenerValorDe(parametrosSpliteados, 1);
	uint16_t key = atoi(keyString);

	segmento* unSegmento = encontrarSegmentoPorNombre(nombreTabla);
	if(unSegmento){

	sem_wait(&unSegmento->mutexSegmento);
	paginaEnTabla* paginaEncontrada = encontrarRegistroPorKey(unSegmento,key);

		if(paginaEncontrada){

			char* value = valueRegistro(paginaEncontrada);
			char *mensaje = string_new();
			string_append_with_format(&mensaje, "SELECT exitoso. Su valor es: %s", value);

			paginaEncontrada->timestamp = time(NULL);

			sem_post(&unSegmento->mutexSegmento);

			enviarOMostrarYLogearInfo(socketKernel, mensaje);

			free(value);
			free(mensaje);
		}
		else {
			// Pedir a LFS un registro para guardar el registro en segmento encontrado.
			sem_post(&unSegmento->mutexSegmento);
			registroConNombreTabla* registroLFS;
			if(!(registroLFS = pedirRegistroLFS(operacionSelect))) {
				enviarYLogearMensajeError(socketKernel, "Por la operacion %s %s, No se encontro el registro en LFS, o hubo un problema al buscarlo.", operacionSelect->operacion, operacionSelect->parametros);
			} else {
				if(agregarPaginaEnSegmento(unSegmento,(registro*) registroLFS,socketKernel,0, &seEjecutaraJournal)) {
					char *mensaje = string_new();
					string_append_with_format(&mensaje, "SELECT exitoso. Su valor es: %s", registroLFS->value);
					enviarOMostrarYLogearInfo(socketKernel, mensaje);
					free(mensaje);
				}
				else if(!seEjecutaraJournal){
					enviarYLogearMensajeError(socketKernel, "Por la operacion %s %s, Hubo un error al guardar el registro LFS en la memoria.", operacionSelect->operacion, operacionSelect->parametros);
				}
				liberarRegistroConNombreTabla(registroLFS);
			}

		}
	}

	else {
		// pedir a LFS un registro para guardar registro con el nombre de la tabla.
		registroConNombreTabla* registroLFS = pedirRegistroLFS(operacionSelect);
		if(!registroLFS) {
			enviarYLogearMensajeError(socketKernel, "Por la operacion %s %s, No se encontro el registro en LFS, o hubo un problema al buscarlo.", operacionSelect->operacion, operacionSelect->parametros);
		}else{
			if(agregarSegmentoConNombreDeLFS(registroLFS,0,socketKernel, &seEjecutaraJournal)){
				char *mensaje = string_new();
				string_append_with_format(&mensaje, "SELECT exitoso. Su valor es: %s", registroLFS->value);
				enviarOMostrarYLogearInfo(socketKernel, mensaje);
				free(mensaje);
		}
			else if(!seEjecutaraJournal){
				enviarYLogearMensajeError(socketKernel, "Por la operacion %s %s, Hubo un error al agregar el segmento en la memoria.", operacionSelect->operacion, operacionSelect->parametros);
			}
			liberarRegistroConNombreTabla(registroLFS);
		}
	}


	liberarRecursosSelectLQL(nombreTabla, keyString);
	liberarParametrosSpliteados(parametrosSpliteados);
	marcarHiloComoSemaforoRealizado(semaforoDeOperacion);
}

void liberarRecursosInsertLQL(char* nombreTabla, registro* unRegistro) {
	free(nombreTabla);
	liberarRegistro(unRegistro);
}

void insertLQL(operacionLQL* operacionInsert, int socketKernel){
	sem_t* semaforoDeOperacion = obtenerHiloEnTabla(pthread_self())->semaforoOperacion;
	marcarHiloRealizandoSemaforo(semaforoDeOperacion);
	verSiHayJournalEjecutandose(semaforoDeOperacion);
	char* timestamp = NULL;
	bool seEjecutaraJournal = false;

	char** parametrosSpliteadosPorComillas = string_split(operacionInsert->parametros, "\"");
	char* value = string_duplicate(*(parametrosSpliteadosPorComillas + 1));
	if(*(parametrosSpliteadosPorComillas + 2) != NULL) {
		timestamp = string_duplicate(*(parametrosSpliteadosPorComillas + 2));
	}
	char** tablaYKey = string_split(*(parametrosSpliteadosPorComillas + 0), " ");
	char* tabla = string_duplicate(*tablaYKey);
	char* key = string_duplicate(*(tablaYKey + 1));

	registro* registroNuevo = crearRegistroNuevo(value, timestamp, key, MEMORIA_PRINCIPAL->tamanioMaximoValue);

	if(!registroNuevo) {
		enviarYLogearMensajeError(socketKernel, "ERROR: El value %s es mayor al tamanio maximo del value posible. (Tamanio maximo posible: %d)", value, MEMORIA_PRINCIPAL->tamanioMaximoValue);
		free(tabla);
		free(value);
		free(timestamp);
		free(key);
		liberarParametrosSpliteados(tablaYKey);
		liberarParametrosSpliteados(parametrosSpliteadosPorComillas);
		return;
	}

	segmento* unSegmento = encontrarSegmentoPorNombre(tabla);
	if(unSegmento){

		sem_wait(&unSegmento->mutexSegmento);
		paginaEnTabla* paginaEncontrada = encontrarRegistroPorKey(unSegmento,registroNuevo->key);

		if(paginaEncontrada){
			cambiarDatosEnMemoria(paginaEncontrada, registroNuevo);
			paginaEncontrada->flag = SI;

			sem_post(&unSegmento->mutexSegmento);

			enviarOMostrarYLogearInfo(socketKernel, "Por la operacion %s %s, Se inserto exitosamente.", operacionInsert->operacion, operacionInsert->parametros);
		} else {
			sem_post(&unSegmento->mutexSegmento);
			if(agregarPaginaEnSegmento(unSegmento, registroNuevo, socketKernel, 1, &seEjecutaraJournal)){
				enviarOMostrarYLogearInfo(socketKernel, "Por la operacion %s %s, Se inserto exitosamente.", operacionInsert->operacion, operacionInsert->parametros);
			} else if(!seEjecutaraJournal){
				enviarYLogearMensajeError(socketKernel, "ERROR: Por la operacion %s %s, Hubo un error al agregar el segmento en la memoria.", operacionInsert->operacion, operacionInsert->parametros);
			}

		}
	}

	else {
		if(agregarSegmento(registroNuevo,tabla,1, socketKernel, &seEjecutaraJournal)) {
			enviarOMostrarYLogearInfo(socketKernel, "Por la operacion %s %s, Se inserto exitosamente.", operacionInsert->operacion, operacionInsert->parametros);
		} else if(!seEjecutaraJournal){
			enviarYLogearMensajeError(socketKernel, "ERROR: Por la operacion %s %s, Hubo un error al agregar el segmento en la memoria.", operacionInsert->operacion, operacionInsert->parametros);
		};
	}
	free(value);
	if(timestamp) free(timestamp);
	free(key);
	liberarParametrosSpliteados(parametrosSpliteadosPorComillas);
	liberarParametrosSpliteados(tablaYKey);
	liberarRecursosInsertLQL(tabla, registroNuevo);
	marcarHiloComoSemaforoRealizado(semaforoDeOperacion);
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
		enviarYLogearMensajeError(-1, "ERROR: Hubo un error al pedir al LFS que realizara DESCRIBE %s", operacionDescribe->parametros);
		enviarError(socketKernel);
		return;
	}

	operacionProtocolo tipoDeMetadata = empezarDeserializacion(&bufferMetadata);

	if(tipoDeMetadata == METADATA){
		sem_wait(&MUTEX_SOCKET_LFS);
		void* bufferMetadataReal = recibir(SOCKET_LFS); // Pls dont question about this on the coloquio thanks
		sem_post(&MUTEX_SOCKET_LFS);
		metadata* unaMetadata = deserializarMetadata(bufferMetadataReal);
		serializarYEnviarMetadata(socketKernel, unaMetadata);
		free(unaMetadata->nombreTabla);
		free(unaMetadata);
	} else if(tipoDeMetadata == PAQUETEMETADATAS) {
		t_list* metadatas = list_create();

		void guardarMetadata(void* unaMetadata) {
			metadata* auxMetadata = (metadata*) unaMetadata;
			metadata* metadataAEnviar = malloc(sizeof(metadata));

			metadataAEnviar->cantParticiones = auxMetadata->cantParticiones;
			metadataAEnviar->nombreTabla = string_duplicate(auxMetadata->nombreTabla);
			metadataAEnviar->tiempoCompactacion = auxMetadata->tiempoCompactacion;
			metadataAEnviar->tipoConsistencia = auxMetadata->tipoConsistencia;

			list_add(metadatas, metadataAEnviar);
		}

		sem_wait(&MUTEX_SOCKET_LFS);
		recibirYDeserializarPaqueteDeMetadatasRealizando(SOCKET_LFS, guardarMetadata);
		sem_post(&MUTEX_SOCKET_LFS);

		enviar(socketKernel, (void*) &tipoDeMetadata, sizeof(operacionProtocolo));
		serializarYEnviarPaqueteMetadatas(socketKernel, metadatas);
		list_destroy_and_destroy_elements(metadatas, liberarMetadata);
	}
	free(bufferMetadata);
}

void dropLQL(operacionLQL* operacionDrop, int socketKernel) {
	sem_t* semaforoDeOperacion = obtenerHiloEnTabla(pthread_self())->semaforoOperacion;
	marcarHiloRealizandoSemaforo(semaforoDeOperacion);
	verSiHayJournalEjecutandose(semaforoDeOperacion);

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

	marcarHiloComoSemaforoRealizado(semaforoDeOperacion);
}
// ------------------------------------------------------------------------ //
// 7) TIMED OPERATIONS //

void cargarSeeds() {
	char** IPs = config_get_array_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "IP_SEEDS");
	char** puertos = config_get_array_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "PUERTO_SEEDS");

	int i = 0;
	while(*(IPs + i) != NULL && *(puertos + i) != NULL) {
		seed *unaSeedParaTablaGossip = malloc(sizeof(seed));
		unaSeedParaTablaGossip->ip = string_duplicate(*(IPs + i));
		unaSeedParaTablaGossip->puerto = string_duplicate(*(puertos + i));
		unaSeedParaTablaGossip->numero = -1;

		seed* unaSeedParaTablaDeSeeds = malloc(sizeof(seed)); // YEAH THIS FUCKING SUCKS
		unaSeedParaTablaDeSeeds->ip = string_duplicate(*(IPs + i));
		unaSeedParaTablaDeSeeds->puerto = string_duplicate(*(puertos + i));
		unaSeedParaTablaDeSeeds->numero = -1;

		sem_wait(&MUTEX_TABLA_GOSSIP);
		list_add(TABLA_GOSSIP, unaSeedParaTablaGossip);
		sem_post(&MUTEX_TABLA_GOSSIP);

		sem_wait(&MUTEX_TABLA_SEEDS_CONFIG);
		list_add(TABLA_SEEDS_CONFIG, unaSeedParaTablaDeSeeds);
		sem_post(&MUTEX_TABLA_SEEDS_CONFIG);


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
	serializarYEnviarTablaGossip(socketMemoria,TABLA_GOSSIP);
}

void recibirYGuardarEnTablaGossip(int socketMemoria, int estoyPidiendo) {
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

	if(estoyPidiendo) {
		pedirTablaGossip(socketMemoria);
	}
	recibirYDeserializarTablaDeGossipRealizando(socketMemoria, guardarEnTablaGossip);
}

bool seedEnTablaGossip(void* seedAComprobar){
	seed* unaSeed = (seed*) seedAComprobar;
	bool esIgualA(void* otraSeed) {
		return sonSeedsIguales(unaSeed,(seed*) otraSeed);
	}
	return list_any_satisfy(TABLA_GOSSIP,esIgualA);
}

void intentarConexiones(t_log* logGossip) {
	seed* seedPropia = malloc(sizeof(seed));
	seedPropia->ip = string_duplicate(config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "IP_MEMORIA"));
	seedPropia->puerto = string_duplicate(config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "PUERTO"));

	void intentarConexion(void* seedEnTabla, int tabla) {
		seed* unaSeed = (seed*) seedEnTabla;

		bool esIgualA(void* otraSeed) {
			return sonSeedsIguales(unaSeed,(seed*) otraSeed);
		}

		if(sonSeedsIguales(seedPropia, unaSeed)) {
			return; // Para no hacer una conexion al pedo con la propia memoria.
		}

		int socketMemoria = crearSocketCliente(unaSeed->ip, unaSeed->puerto);

		log_info(logGossip, "Intentando conexion con la memoria de IP \"%s\" y puerto \"%s\"", unaSeed->ip, unaSeed->puerto);

		if(socketMemoria == -1) {
			if(tabla==1){

				log_info(logGossip, "Se cerro la conexion con esta IP y este puerto. Eliminando de la tabla gossip...");

				seed* seedRemovida = (seed*) list_remove_by_condition(TABLA_GOSSIP, esIgualA);

				liberarSeed(seedRemovida);

				return;
			}
			log_info(logGossip, "No se pudo conectar con esta seed proveniente del archivo de config, no se agregara a la tabla de Gossip.");
			return;
		}


		log_info(logGossip, "Recibiendo tabla Gossip de la memoria de IP \"%s\" y puerto \"%s\"...", unaSeed->ip, unaSeed->puerto);
		recibirYGuardarEnTablaGossip(socketMemoria, 1);

		cerrarConexion(socketMemoria);
	}

	void intentarConexionTablaGossip(void* seedEnTablaGossip){
		intentarConexion(seedEnTablaGossip,1);
	}

	void intentarConexionTablaConfig(void* seedEnTablaConfig){
		if(!seedEnTablaGossip(seedEnTablaConfig)){
			intentarConexion(seedEnTablaConfig,0);
		}
	}

	sem_wait(&MUTEX_TABLA_GOSSIP);
	list_iterate(TABLA_GOSSIP, intentarConexionTablaGossip);
	sem_post(&MUTEX_TABLA_GOSSIP);

	sem_wait(&MUTEX_TABLA_SEEDS_CONFIG);
	list_iterate(TABLA_SEEDS_CONFIG, intentarConexionTablaConfig);
	sem_post(&MUTEX_TABLA_SEEDS_CONFIG);
	liberarSeed(seedPropia);
}

void* timedGossip() {
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
	cargarSeeds();

	t_log* logGossip = log_create("memoria_gossip.log", "GOSSIP", 0, LOG_LEVEL_INFO);


	while(1) {
		log_info(logGossip, "Gossip Realizandose...");
		intentarConexiones(logGossip);

		sem_wait(&MUTEX_RETARDO_GOSSIP);
		int retardoGossip = RETARDO_GOSSIP * 1000;
		sem_post(&MUTEX_RETARDO_GOSSIP);

		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
		usleep(retardoGossip);
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);


		log_info(logGossip, "Gossip Realizado");

	}
}

void* timedJournal(){
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

	while(1){

		sem_wait(&MUTEX_RETARDO_JOURNAL);
		int retardoJournal = RETARDO_JOURNAL * 1000;
		sem_post(&MUTEX_RETARDO_JOURNAL);

		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
		usleep(retardoJournal);
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);

		journalLQL(-1);
	}
}

// ------------------------------------------------------------------------ //
// 8) LISTA DE HILOS //

void agregarHiloAListaDeHilos() {
	hiloEnTabla* hiloPropio = malloc(sizeof(hiloEnTabla));
	hiloPropio->thread = pthread_self();

	hiloPropio->semaforoOperacion = malloc(sizeof(sem_t));
	sem_init(hiloPropio->semaforoOperacion, 0 , 1);

	sem_wait(&MUTEX_TABLA_THREADS);
	list_add(TABLA_THREADS, hiloPropio);
	sem_post(&MUTEX_TABLA_THREADS);
}

void eliminarHiloDeListaDeHilos() {
	void destruirThreadEnTabla(void* hiloEnLaTabla) {
		hiloEnTabla* unHilo = (hiloEnTabla*) hiloEnLaTabla;
		free(unHilo->semaforoOperacion);
		free(unHilo);
	}

	bool esElPropioThread(void* hiloEnLaTabla) {
		hiloEnTabla* unHilo = (hiloEnTabla*) hiloEnLaTabla;
		return pthread_equal(unHilo->thread, pthread_self());
	}

	sem_wait(&MUTEX_TABLA_THREADS);
	hiloEnTabla* hiloADestruir = (hiloEnTabla*) list_remove_by_condition(TABLA_THREADS, esElPropioThread);
	sem_post(&MUTEX_TABLA_THREADS);

	if(!hiloADestruir) {
		printf("Hubo un problema eliminando el hilo de la lista de hilos\n");
		return;
	}
	destruirThreadEnTabla((void*) hiloADestruir);
	return;
}

void* esperarSemaforoDeHilo(void* buffer) {
		hiloEnTabla* hiloAEsperar = (hiloEnTabla*) buffer;

		sem_post(&BINARIO_HILO_EN_TABLA);
		sem_wait(hiloAEsperar->semaforoOperacion);
		pthread_exit(0);
}

void esperarAHilosEjecutandose(void* (*esperarSemaforoParticular)(void*)){
	t_list* listaHilosEsperandoSemaforos = list_create();

	void esperarHiloEsperando(void* hiloEsperando) {
		hiloQueEspera* unHiloEsperando = (hiloQueEspera*) hiloEsperando;
		pthread_join(unHiloEsperando->thread, NULL);
		free(unHiloEsperando);
	}

	void crearHiloParaEsperar(void* unHilo) {
		sem_wait(&BINARIO_HILO_EN_TABLA);
		hiloQueEspera* unHiloQueEspera = malloc(sizeof(hiloQueEspera));
		int error = pthread_create(&unHiloQueEspera->thread, NULL, esperarSemaforoParticular, unHilo);
		if(error) {
			sem_post(&BINARIO_HILO_EN_TABLA);
			printf("Hubo un error al crear un hilo para esperar\n");
		}
		list_add(listaHilosEsperandoSemaforos, unHiloQueEspera);
	}

	sem_wait(&MUTEX_TABLA_THREADS);
	list_iterate(TABLA_THREADS, crearHiloParaEsperar);
	list_iterate(listaHilosEsperandoSemaforos, esperarHiloEsperando);
	sem_post(&MUTEX_TABLA_THREADS);

	list_destroy(listaHilosEsperandoSemaforos);
}

void dejarEjecutarOperacionesDeNuevo() {
	void postSemaforoDelHilo(void* hilo) {
		sem_post(((hiloEnTabla*) hilo)->semaforoOperacion);
	}

	int valorMutexTablaThreads;
	sem_getvalue(&MUTEX_TABLA_THREADS, &valorMutexTablaThreads);

	sem_wait(&MUTEX_TABLA_THREADS);
	list_iterate(TABLA_THREADS, postSemaforoDelHilo);
	sem_post(&MUTEX_TABLA_THREADS);
}

//----------------Cancelaciones---------------------------


void cancelarListaHilos(){
		void cancelarHilo(void* hilo){
			pthread_cancel(((hiloEnTabla*) hilo)->thread);
			pthread_join(((hiloEnTabla*) hilo)->thread, NULL);
		}

	list_iterate(TABLA_THREADS,cancelarHilo);
}

void cleanupTrabajarConConexion() {
	cancelarListaHilos();
}
