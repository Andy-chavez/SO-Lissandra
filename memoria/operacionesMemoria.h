/*
 * operacionesMemoria.c
 *
 *  Created on: 4 may. 2019
 *      Author: utnso
 */
#include "structsYVariablesGlobales.h"

// ------------------------------------------------------------------------ //
// OPERACIONES SOBRE MEMORIA PRINCIPAL //

memoria* inicializarMemoria(datosInicializacion* datosParaInicializar, configYLogs* archivosDeConfigYLog) {
	memoria* nuevaMemoria = malloc(sizeof(memoria));
	int tamanioMemoria = config_get_int_value(archivosDeConfigYLog->config, "TAMANIOMEM");

	nuevaMemoria->base = malloc(tamanioMemoria);
	memset(nuevaMemoria->base, 0, tamanioMemoria);
	nuevaMemoria->limite = nuevaMemoria->base + tamanioMemoria;
	nuevaMemoria->tamanioMaximoValue = datosParaInicializar->tamanio;
	nuevaMemoria->tablaSegmentos = list_create();

	tamanioDeUnRegistroEnMemoria = calcularEspacioParaUnRegistro(nuevaMemoria->tamanioMaximoValue);

	if(nuevaMemoria == NULL || nuevaMemoria->base == NULL) {
		log_error(archivosDeConfigYLog->logger, "hubo un error al inicializar la memoria");
		return NULL;
	}

	log_info(archivosDeConfigYLog->logger, "Memoria inicializada.");
	return nuevaMemoria;
}

void* liberarSegmentos(segmento* unSegmento) {
	void* liberarPaginas(paginaEnTabla* unRegistro) {
		free(unRegistro);
	}

		free(unSegmento->nombreTabla);
		list_destroy_and_destroy_elements(unSegmento->tablaPaginas, liberarPaginas);
		free(unSegmento);
	}

void liberarMemoria() {
	free(memoriaPrincipal->base);
	list_destroy_and_destroy_elements(memoriaPrincipal->tablaSegmentos, liberarSegmentos);
	free(memoriaPrincipal);
}

int calcularEspacioParaUnRegistro(int tamanioMaximo) {
	int timeStamp = sizeof(time_t);
	int key = sizeof(uint16_t);
	int value = tamanioMaximo;
	return timeStamp + key + value;
}

void* encontrarEspacio() {
	void* espacioLibre = memoriaPrincipal->base;
	while(*((int*) espacioLibre) != 0) {
		espacioLibre = espacioLibre + tamanioDeUnRegistroEnMemoria;
	}
	return espacioLibre;
}

bool hayEspacioSuficienteParaUnRegistro(){
	return (memoriaPrincipal->limite - encontrarEspacio()) > tamanioDeUnRegistroEnMemoria;
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

void* guardar(registroConNombreTabla* unRegistro) {
	bufferDePagina *bufferAGuardar = armarBufferDePagina(unRegistro, memoriaPrincipal->tamanioMaximoValue);
	void *guardarDesde = encontrarEspacio();

	memcpy(guardarDesde, bufferAGuardar->buffer, bufferAGuardar->tamanio);

	liberarbufferDePagina(bufferAGuardar);

	return guardarDesde;
}

void* guardarEnMemoria(registroConNombreTabla* unRegistro) {

	if(hayEspacioSuficienteParaUnRegistro()){
		return guardar(unRegistro);
	} else {
		return NULL;
	}

}

paginaEnTabla* crearPaginaParaSegmento(registro* unRegistro) {
	paginaEnTabla* pagina = malloc(sizeof(paginaEnTabla));

	if(!(pagina->unRegistro = guardarEnMemoria(unRegistro))) {
		// TODO Avisar que no se pudo guardar en memoria.
		return NULL;
	};
	pagina->flag = NO;

	return pagina;
}

int agregarSegmento(registro* primerRegistro,char* tabla ){

	paginaEnTabla* primeraPagina = crearPaginaParaSegmento(primerRegistro);

	if(!primeraPagina) {
		return -1;
	}

	primeraPagina->numeroPagina = 0;
	segmento* segmentoNuevo = malloc(sizeof(segmento));
	segmentoNuevo->nombreTabla = string_duplicate(tabla);
	segmentoNuevo->tablaPaginas = list_create();
	list_add(memoriaPrincipal->tablaSegmentos, segmentoNuevo);

	list_add(segmentoNuevo->tablaPaginas, primeraPagina);

	return 0;
}

void agregarPaginaEnSegmento(segmento* unSegmento, registro* unRegistro, int socketKernel) {
	paginaEnTabla* paginaParaAgregar = crearPaginaParaSegmento(unRegistro);
	if(!paginaParaAgregar) {
		enviarYLogearMensajeError(archivosDeConfigYLog->logger, socketKernel, "ERROR: No se pudo guardar el registro en la memoria");
		return;
	}
	paginaParaAgregar->numeroPagina = list_size(unSegmento);
	list_add(unSegmento->tablaPaginas, paginaParaAgregar);
}

registro* leerDatosEnMemoria(paginaEnTabla* unRegistro) {
	registro* registroARetornar = malloc(sizeof(registro));

	memcpy(&(registroARetornar->timestamp),(time_t*) unRegistro->unRegistro, sizeof(time_t));
	int desplazamiento = sizeof(time_t);

	memcpy(&(registroARetornar->key), (uint16_t*) (unRegistro->unRegistro + desplazamiento), sizeof(uint16_t));
	desplazamiento += sizeof(uint16_t);

	int tamanioValue = obtenerTamanioValue((unRegistro->unRegistro + desplazamiento)) + 1;
	registroARetornar->value = malloc(tamanioValue);
	memcpy(registroARetornar->value, (unRegistro->unRegistro + desplazamiento), tamanioValue);

	return registroARetornar;
}

void cambiarDatosEnMemoria(paginaEnTabla* registroACambiar, registro* registroNuevo) {
	bufferDePagina* bufferParaCambio = armarBufferDePagina(registroNuevo, memoriaPrincipal->tamanioMaximoValue);
	memcpy(registroACambiar->unRegistro, bufferParaCambio->buffer, bufferParaCambio->tamanio);
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
// OPERACIONES SOBRE LISTAS, TABLAS Y PAGINAS //

registroConNombreTabla* pedirRegistroLFS(operacionLQL *operacion) {
	serializarYEnviarOperacionLQL(socketLissandraFS, operacion);

	void* bufferRespuesta = recibir(socketLissandraFS);
	registroConNombreTabla* paginaEncontradaEnLFS = deserializarRegistro(bufferRespuesta);

	return paginaEncontradaEnLFS;
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
	// TODO ver como resolver el malicioso free de fucking aca
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

	return (segmento*) list_find(memoriaPrincipal->tablaSegmentos, (void*)segmentoDeIgualNombre);
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

void enviarYLogearMensajeError(t_log *logger, int socket, char* mensaje) {
	log_error(logger, mensaje);
	enviar(socket, mensaje, strlen(mensaje) + 1);
}

// ------------------------------------------------------------------------ //
// OPERACIONESLQL //

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

			printf ("El valor es %s\n", value);
			int longitudValue = string_length(value) + 1;
			enviar(socketKernel, (void*) value, longitudValue);
			free(value);
		}
		else {
			registroConNombreTabla* registroLFS;
			if(!(registroLFS = pedirRegistroLFS(operacionSelect))) {
				enviarYLogearMensajeError(archivosDeConfigYLog->logger, socketKernel, "ERROR: No se encontro el registro en LFS, o hubo un error al buscarlo.");
			}
			else if(guardarEnMemoria(registroLFS)) {
				enviar(socketKernel, (void*) registroLFS->value, strlen(registroLFS->value) + 1);
			}
			else {
				enviarYLogearMensajeError(archivosDeConfigYLog->logger, socketKernel, "ERROR: Hubo un error al guardar el registro LFS en la memoria.");
			};
			// TODO else journal();
		}
	}

	else {
		registroConNombreTabla* registroLFS = pedirRegistroLFS(operacionSelect);
		if(agregarSegmento(registroLFS, nombreTabla)){
		enviar(socketKernel, (void*) registroLFS->value, strlen(registroLFS->value) + 1);
		}
		else {
			enviarYLogearMensajeError(archivosDeConfigYLog->logger, socketKernel, "ERROR: Hubo un error al agregar el segmento en la memoria.");
		}
	}
	liberarRecursosSelectLQL(nombreTabla, keyString);
	liberarParametrosSpliteados(parametrosSpliteados); // Por alguna magica razon no me deja liberarlos dentro de la funcion de liberar Recursos
	/*
	size_t length = config_get_int_value(archivosDeConfigYLog->config, "TAMANIOMEM");
	mem_hexdump(memoriaPrincipal->base, length);
	*/
}

void liberarRecursosInsertLQL(char* nombreTabla, registro* unRegistro, char** parametrosSpliteados) {
	free(nombreTabla);
	liberarRegistro(unRegistro);
	liberarParametrosSpliteados(parametrosSpliteados);
}

void insertLQL(operacionLQL* operacionInsert, int socketKernel){
	char** parametrosSpliteados = string_n_split(operacionInsert->parametros, 3, " ");
	char* nombreTabla = (char*) obtenerValorDe(parametrosSpliteados, 0);
	registro* registroNuevo = crearRegistroNuevo(parametrosSpliteados, memoriaPrincipal->tamanioMaximoValue);
	if(!registroNuevo) {
		enviarYLogearMensajeError(archivosDeConfigYLog->logger, socketKernel, "ERROR: El value era mayor al tamanio maximo del value posible.");
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
		} else {
			agregarPaginaEnSegmento(unSegmento, registroNuevo, socketKernel);
		}
	}

	else{
		agregarSegmento(registroNuevo,nombreTabla);
	}
	liberarRecursosInsertLQL(nombreTabla, registroNuevo, parametrosSpliteados);
	size_t length = config_get_int_value(archivosDeConfigYLog->config, "TAMANIOMEM");
	mem_hexdump(memoriaPrincipal->base, length);

}

