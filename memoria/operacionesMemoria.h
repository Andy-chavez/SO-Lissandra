/*
 * operacionesMemoria.c
 *
 *  Created on: 4 may. 2019
 *      Author: utnso
 */


#include "structsYVariablesGlobales.h"
#include <unistd.h>

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
	};
}

void inicializarTablaGossip() {
	seed* seedPropia = malloc(sizeof(seed));
	seedPropia->ip = config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "IP_MEMORIA");
	seedPropia->puerto = config_get_string_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "PUERTO");

	TABLA_GOSSIP = list_create();
	list_add(TABLA_GOSSIP, seedPropia);
}

memoria* inicializarMemoria(datosInicializacion* datosParaInicializar, configYLogs* ARCHIVOS_DE_CONFIG_Y_LOG) {
	memoria* nuevaMemoria = malloc(sizeof(memoria));
	int tamanioMemoria = config_get_int_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "TAM_MEM");

	nuevaMemoria->base = malloc(tamanioMemoria);
	memset(nuevaMemoria->base, 0, tamanioMemoria);
	nuevaMemoria->limite = nuevaMemoria->base + tamanioMemoria;
	nuevaMemoria->tamanioMaximoValue = datosParaInicializar->tamanio;
	nuevaMemoria->tablaSegmentos = list_create();


	TAMANIO_UN_REGISTRO_EN_MEMORIA = calcularEspacioParaUnRegistro(nuevaMemoria->tamanioMaximoValue);

	if(nuevaMemoria == NULL || nuevaMemoria->base == NULL) {
		log_error(ARCHIVOS_DE_CONFIG_Y_LOG->logger, "hubo un error al inicializar la memoria");
		return NULL;
	}

	inicializarTablaGossip();

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
	sem_init(&MUTEX_OPERACION, 0, 1);
	sem_init(&BINARIO_SOCKET_KERNEL, 0, 1);
	sem_init(&MUTEX_LOG, 0, 1);
	sem_init(&MUTEX_SOCKET_LFS, 0, 1);
	sem_init(&MUTEX_RETARDO_MEMORIA, 0, 1);
	sem_init(&MUTEX_RETARDO_GOSSIP, 0, 1);
	sem_init(&MUTEX_RETARDO_JOURNAL, 0, 1);
	sem_init(&MUTEX_LOG_CONSOLA, 0, 1);
	sem_init(&MUTEX_TABLA_GOSSIP, 0, 1);
}

void* liberarSegmentos(segmento* unSegmento) {
	void* liberarPaginas(paginaEnTabla* unRegistro) {
		free(unRegistro);
	}

		free(unSegmento->nombreTabla);
		list_destroy_and_destroy_elements(unSegmento->tablaPaginas, liberarPaginas);
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
	list_destroy_and_destroy_elements(MEMORIA_PRINCIPAL->tablaSegmentos, liberarSegmentos);
	free(MEMORIA_PRINCIPAL);
}

void liberarTablaMarcos() {
	void* destructorMarcos(marco* unMarco) {
		free(unMarco);
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

void *algoritmoLRU() {
	// TODO algoritmo Last Recently Used.
	// Debe retornar un espacio en memoria (marco) donde pueda guardar la pagina
	// y NULL si no pudo hacerlo.
}

marco* encontrarEspacio() {
	void* encontrarLibreEnTablaMarcos(marco* unMarcoEnTabla) {
		return !(unMarcoEnTabla->estaEnUso);
	}
	marco* marcoLibre;

	if(!(marcoLibre = list_find(TABLA_MARCOS, encontrarLibreEnTablaMarcos))) {
		enviarOMostrarYLogearInfo(-1, "No se encontro espacio libre en la tabla de marcos. Empezando algoritmo LRU");
		if(!(marcoLibre = algoritmoLRU())) {
			enviarOMostrarYLogearInfo(-1, "el algoritmo LRU no pudo liberar memoria. empezando proceso de journal");
			//journal();
		};
	}

	return marcoLibre;
}

marco* encontrarMarcoEscrito(int numeroMarco) {
	marco* marco = list_get(TABLA_MARCOS, numeroMarco);
	return marco;
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

marco* guardar(registroConNombreTabla* unRegistro) {
	bufferDePagina *bufferAGuardar = armarBufferDePagina(unRegistro, MEMORIA_PRINCIPAL->tamanioMaximoValue);

	marco *guardarEn = encontrarEspacio();

	if(!guardarEn) {
		return NULL;
	}

	memcpy(guardarEn->lugarEnMemoria, bufferAGuardar->buffer, bufferAGuardar->tamanio);

	liberarbufferDePagina(bufferAGuardar);

	guardarEn->estaEnUso = 1;

	return guardarEn;
}

int guardarEnMemoria(registroConNombreTabla* unRegistro) {
	marco* marcoGuardado = guardar(unRegistro);
	return marcoGuardado->marco;
}

paginaEnTabla* crearPaginaParaSegmento(int numeroPagina, registro* unRegistro, int deDondeVengo) { // deDondevengo insert= 1 ,select=0
	paginaEnTabla* pagina = malloc(sizeof(paginaEnTabla));
	int marco = guardarEnMemoria(unRegistro);
	if(marco == -1) {
		// TODO Avisar que no se pudo guardar en memoria.
		return NULL;
	};

	pagina->marco = marco;
	pagina->numeroPagina = numeroPagina;

	if(deDondeVengo == 0){ // es un select
		pagina->flag = NO;
	} else if (deDondeVengo == 1) { // es un insert
		pagina->flag = SI;
	}

	return pagina;
}

int agregarSegmento(registro* primerRegistro,char* tabla, int deDondeVengo){

	paginaEnTabla* primeraPagina = crearPaginaParaSegmento(0, primerRegistro,deDondeVengo);

	if(!primeraPagina) {
		return 0;
	}

	segmento* segmentoNuevo = malloc(sizeof(segmento));
	segmentoNuevo->nombreTabla = string_duplicate(tabla);
	segmentoNuevo->tablaPaginas = list_create();

	list_add(MEMORIA_PRINCIPAL->tablaSegmentos, segmentoNuevo);

	list_add(segmentoNuevo->tablaPaginas, primeraPagina);

	return 1;
}


int agregarSegmentoConNombreDeLFS(registroConNombreTabla* registroLFS, int deDondeVengo) {
	return agregarSegmento(registroLFS, registroLFS->nombreTabla,deDondeVengo);
}

void* agregarPaginaEnSegmento(segmento* unSegmento, registro* unRegistro, int socketKernel, int deDondeVengo) {
	paginaEnTabla* paginaParaAgregar = crearPaginaParaSegmento(list_size(unSegmento->tablaPaginas), unRegistro, deDondeVengo);
	if(!paginaParaAgregar) {
		enviarYLogearMensajeError(socketKernel, "ERROR: No se pudo guardar el registro en la memoria");
		return NULL;
	}

	list_add(unSegmento->tablaPaginas, paginaParaAgregar);
	enviarOMostrarYLogearInfo(socketKernel, "Se inserto exitosamente.");
}

registro* leerDatosEnMemoria(paginaEnTabla* unaPagina) {
	registro* registroARetornar = malloc(sizeof(registro));
	marco* marcoEnMemoria = encontrarMarcoEscrito(unaPagina->marco);

	memcpy(&(registroARetornar->timestamp),(time_t*) marcoEnMemoria->lugarEnMemoria, sizeof(time_t));
	int desplazamiento = sizeof(time_t);

	memcpy(&(registroARetornar->key), (uint16_t*) (marcoEnMemoria->lugarEnMemoria + desplazamiento), sizeof(uint16_t));
	desplazamiento += sizeof(uint16_t);

	int tamanioValue = obtenerTamanioValue((marcoEnMemoria->lugarEnMemoria + desplazamiento)) + 1;
	registroARetornar->value = malloc(tamanioValue);
	memcpy(registroARetornar->value, (marcoEnMemoria->lugarEnMemoria + desplazamiento), tamanioValue);

	return registroARetornar;
}

void cambiarDatosEnMemoria(paginaEnTabla* registroACambiar, registro* registroNuevo) {
	bufferDePagina* bufferParaCambio = armarBufferDePagina(registroNuevo, MEMORIA_PRINCIPAL->tamanioMaximoValue);
	//memcpy(registroACambiar->unRegistro, bufferParaCambio->buffer, bufferParaCambio->tamanio);
	liberarbufferDePagina(bufferParaCambio);
}

int obtenerTamanioValue(void* valueBuffer) {
	int tamanio = 0;
	char prueba = *((char*) valueBuffer);
	while(prueba != '\0') {
		tamanio++;
		valueBuffer++;
		prueba = *((char*) valueBuffer);
	}
	return tamanio;
}

// ------------------------------------------------------------------------ //
// 3) OPERACIONES CON LISSANDRA FILE SYSTEM //

void* pedirALFS(operacionLQL *operacion) {
	sem_wait(&MUTEX_SOCKET_LFS);
	serializarYEnviarOperacionLQL(SOCKET_LFS, operacion);
	void* buffer = recibir(SOCKET_LFS);
	sem_post(&MUTEX_SOCKET_LFS);
	return buffer;
}

registroConNombreTabla* pedirRegistroLFS(operacionLQL *operacion) {
	void* bufferRegistroConTabla = pedirALFS(operacion);
	registroConNombreTabla* paginaEncontradaEnLFS = deserializarRegistro(bufferRegistroConTabla);

	if(atoi(paginaEncontradaEnLFS->nombreTabla)) {
		return NULL;
	}

	return paginaEncontradaEnLFS;
}

// ------------------------------------------------------------------------ //
// 4) OPERACIONES SOBRE LISTAS, TABLAS Y PAGINAS //

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

	return (segmento*) list_find(MEMORIA_PRINCIPAL->tablaSegmentos, (void*)segmentoDeIgualNombre);
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

	return (paginaEnTabla*) list_find(unSegmento->tablaPaginas,(void*)tieneIgualKeyQueDada);
}

char* valueRegistro(segmento* unSegmento, int key){
	paginaEnTabla* paginaEncontrada = encontrarRegistroPorKey(unSegmento,key);

	registro* registroReal = leerDatosEnMemoria(paginaEncontrada);
	char* value = malloc(strlen(registroReal->value) + 1);
	memcpy(value, registroReal->value, strlen(registroReal->value) + 1);

	liberarRegistro(registroReal);
	return value;
}

void enviarYOLogearAlgo(int socket, char *mensaje, void(*log)(t_log *, char *)) { // ME ILUMINE AH BUENO
	if(socket != -1) {
		sem_wait(&MUTEX_LOG);
		log(ARCHIVOS_DE_CONFIG_Y_LOG->logger, mensaje);
		sem_post(&MUTEX_LOG);
		enviar(socket, mensaje, strlen(mensaje) + 1);
	} else {
		sem_wait(&MUTEX_LOG_CONSOLA);
		log(LOGGER_CONSOLA, mensaje);
		sem_post(&MUTEX_LOG_CONSOLA);
	}
}

void enviarYLogearMensajeError(int socket, char* mensaje) {
	enviarYOLogearAlgo(socket, mensaje, (void*) log_error);
}

void enviarOMostrarYLogearInfo(int socket, char* mensaje) {
	enviarYOLogearAlgo(socket, mensaje, (void*) log_info);
}

// ------------------------------------------------------------------------ //
// 5) CHECKS A OPERACIONESLQL //

int esInsertOSelectEjecutable(char* parametros) {
	char** parametrosSpliteados = string_split(parametros, " ");
	if(!atoi(*(parametrosSpliteados + 1)) && *(parametrosSpliteados + 1) != "0"){
		return 0;
	}
	return 1;
}

// ------------------------------------------------------------------------ //
// 6) OPERACIONESLQL //

void liberarRecursosSelectLQL(char* nombreTabla, int *key) {
	free(nombreTabla);
	free(key);
}

void selectLQL(operacionLQL *operacionSelect, int socketKernel){
	char** parametrosSpliteados = string_split(operacionSelect->parametros, " ");
	char* nombreTabla = (char*) obtenerValorDe(parametrosSpliteados, 0);
	char *keyString = (char*) obtenerValorDe(parametrosSpliteados, 1);
	uint16_t key = atoi(keyString);

	segmento* unSegmento;
	if(unSegmento = encontrarSegmentoPorNombre(nombreTabla)){
	paginaEnTabla* paginaEncontrada;

		if(paginaEncontrada = encontrarRegistroPorKey(unSegmento,key)){

			char* value = valueRegistro(unSegmento,key);
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
				enviarYLogearMensajeError(socketKernel, "ERROR: No se encontro el registro en LFS, o hubo un error al buscarlo.");
			}
			else if(agregarPaginaEnSegmento(unSegmento,registroLFS,socketKernel,0)) {
				enviar(socketKernel, (void*) registroLFS->value, strlen(registroLFS->value) + 1);
			}
			else {
				enviarYLogearMensajeError(socketKernel, "ERROR: Hubo un error al guardar el registro LFS en la memoria.");
			};
		}
	}

	else {
		// pedir a LFS un registro para guardar registro con el nombre de la tabla.
		registroConNombreTabla* registroLFS = pedirRegistroLFS(operacionSelect);
		if(agregarSegmentoConNombreDeLFS(registroLFS,0)){
		enviar(socketKernel, (void*) registroLFS->value, strlen(registroLFS->value) + 1);
		}
		else {
			enviarYLogearMensajeError(socketKernel, "ERROR: Hubo un error al agregar el segmento en la memoria.");
		}
	}
	// TODO else journal();

	liberarRecursosSelectLQL(nombreTabla, keyString);
	liberarParametrosSpliteados(parametrosSpliteados); // Por alguna magica razon no me deja liberarlos dentro de la funcion de liberar Recursos
	/*
	size_t length = config_get_int_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "TAMANIOMEM");
	mem_hexdump(MEMORIA_PRINCIPAL->base, length);
	*/
}

void liberarRecursosInsertLQL(char* nombreTabla, registro* unRegistro) {
	free(nombreTabla);
	liberarRegistro(unRegistro);
}

void insertLQL(operacionLQL* operacionInsert, int socketKernel){
	char** parametrosSpliteados = string_n_split(operacionInsert->parametros, 3, " ");
	char* nombreTabla = (char*) obtenerValorDe(parametrosSpliteados, 0);
	registro* registroNuevo = crearRegistroNuevo(parametrosSpliteados, MEMORIA_PRINCIPAL->tamanioMaximoValue);

	if(!registroNuevo) {
		enviarYLogearMensajeError(socketKernel, "ERROR: El value era mayor al tamanio maximo del value posible.");
		free(nombreTabla);
		liberarParametrosSpliteados(parametrosSpliteados);
		return;
	}

	segmento* unSegmento;
	if(unSegmento = encontrarSegmentoPorNombre(nombreTabla)){
		paginaEnTabla* paginaEncontrada;
		if(paginaEncontrada = encontrarRegistroPorKey(unSegmento,registroNuevo->key)){
			cambiarDatosEnMemoria(paginaEncontrada, registroNuevo);
			paginaEncontrada->flag = SI;

			enviarOMostrarYLogearInfo(socketKernel, "Se inserto exitosamente.");
		} else {
			agregarPaginaEnSegmento(unSegmento, registroNuevo, socketKernel,1);
		}
	}

	else {
		if(agregarSegmento(registroNuevo,nombreTabla,1)) {
			enviarOMostrarYLogearInfo(socketKernel, "Se inserto exitosamente.");
		} else {
			enviarYLogearMensajeError(socketKernel, "ERROR: Hubo un error al agregar el segmento en la memoria.");
		};
	}
	// TODO else journal();
	liberarParametrosSpliteados(parametrosSpliteados);
	liberarRecursosInsertLQL(nombreTabla, registroNuevo);
	/*size_t length = config_get_int_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "TAM_MEM");
	mem_hexdump(MEMORIA_PRINCIPAL->base, length);
	*/

}

void createLQL(operacionLQL* operacionCreate, int socketKernel) {
	char* mensaje = (char*) pedirALFS(operacionCreate);

	if(!mensaje) {
		enviarYLogearMensajeError(socketKernel, "ERROR: Hubo un error al pedir al LFS que realizara CREATE");
	}

	else{
	enviarOMostrarYLogearInfo(socketKernel, mensaje);

	free(mensaje);
	}
}

void describeLQL(operacionLQL* operacionCreate, int socketKernel) {
	void* bufferMetadata = pedirALFS(operacionCreate);

	if(!bufferMetadata) {
		enviarYLogearMensajeError(socketKernel, "ERROR: Hubo un error al pedir al LFS que realizara DESCRIBE");
		return;
	}

	metadata* unaMetadata = deserializarMetadata(bufferMetadata);

	serializarYEnviarMetadata(socketKernel, unaMetadata);

	free(unaMetadata->nombreTabla);
	free(unaMetadata);
	free(bufferMetadata);
}

// ------------------------------------------------------------------------ //
// 7) TIMED OPERATIONS //

void cargarSeeds(t_list* listaSeeds) {
	char** IPs = config_get_array_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "IP_SEEDS");
	char** puertos = config_get_array_value(ARCHIVOS_DE_CONFIG_Y_LOG->config, "PUERTO_SEEDS");

	int i = 0;
	while(*(IPs + i) != NULL && *(puertos + i) != NULL) {
		seed *unaSeed = malloc(sizeof(seed));
		unaSeed->ip = string_duplicate(*(IPs + i));
		unaSeed->puerto = string_duplicate(*(puertos + i));

		list_add(listaSeeds, unaSeed);

		free(*(IPs + i));
		free(*(puertos + i));
		i++;
	}

	free(IPs);
	free(puertos);
}

void intercambiarTablasGossip(int unSocketMemoria) {
	sem_wait(&MUTEX_TABLA_GOSSIP);
	serializarYEnviarTablaGossip(unSocketMemoria, TABLA_GOSSIP);
	recibirYGuardarEnTablaGossip(unSocketMemoria, TABLA_GOSSIP);
	sem_post(&MUTEX_TABLA_GOSSIP);
}

void intentarConexiones(t_list* listaSeeds) {
	void* intentarConexion(seed* unaSeed) {
		int socketMemoria = crearSocketCliente(unaSeed->ip, unaSeed->puerto);

		if(socketMemoria == -1) {
			sem_wait(&MUTEX_LOG_CONSOLA);
			log_info(LOGGER_CONSOLA, "No se pudo conectar con la memoriade IP \"%s\" y puerto \"%s\"", unaSeed->ip, unaSeed->puerto);
			sem_post(&MUTEX_LOG_CONSOLA);
			return NULL;
		}

		intercambiarTablasGossip(socketMemoria);
	}

	list_iterate(listaSeeds, intentarConexion);
}

void* timedGossip() {
	t_list* seeds = list_create();

	cargarSeeds(seeds);

	while(1) {
		intentarConexiones(seeds);

		sem_wait(&MUTEX_RETARDO_GOSSIP);
		int retardoGossip = RETARDO_GOSSIP * 1000;
		sem_post(&MUTEX_RETARDO_GOSSIP);

		usleep(retardoGossip);
	}
}
