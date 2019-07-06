#include "serializacion.h"
#include <sys/socket.h>
#include <sys/types.h>

// ------------------------------------------------------------------------ //
// 1) FUNCIONES/PROCEDIMIENTOS DE ORDEN SUPERIOR //

void serializarYEnviarAlgo(int socket, void* algo, void*(funcionQueSerializa)(void*, int*)) {
	int tamanioBuffer;
	void* bufferAEnviar = funcionQueSerializa(algo, &tamanioBuffer);
	enviar(socket, bufferAEnviar, tamanioBuffer);
	free(bufferAEnviar);
}

void recibirYDeserializarPaqueteDeAlgoRealizando(int socket, void(*accion)(void*), void*(funcionQueDeserializa)(void*, int*), void*(funcionQueLibera)(void*)) {
	int desplazamiento = 4;
	int tamanioTotal;
	int tamanioUnaOperacion;
	void* bufferTotal;

	recv(socket, &tamanioTotal, sizeof(int), MSG_WAITALL);
	bufferTotal = malloc(tamanioTotal);
	recv(socket, bufferTotal, tamanioTotal, MSG_WAITALL);

	while(desplazamiento < tamanioTotal) {
		void* algo = funcionQueDeserializa(bufferTotal + desplazamiento, &tamanioUnaOperacion);
		accion(algo);
		funcionQueLibera(algo);
		desplazamiento += tamanioUnaOperacion;
	}
	free(bufferTotal);
}

void* serializarPaqueteDeAlgo(void* listaDeAlgo, int* tamanio, void*(funcionQueSerializa)(void*, int*), operacionProtocolo protocolo) {
	int tamanioTotal = sizeof(operacionProtocolo);
	t_list* buffersAFoldear = list_create();
	void* bufferTotal;
	int desplazamiento = 0;

	void* agregarOperacionAListaDeBuffers(void *algo) {
		bufferConTamanio* bufferOperacion = malloc(sizeof(bufferConTamanio));
		bufferOperacion->buffer = funcionQueSerializa(algo, &bufferOperacion->tamanio);
		tamanioTotal += bufferOperacion->tamanio;
		list_add(buffersAFoldear, bufferOperacion);
	}

	void* agregarBufferDeOperacionABufferTotal(bufferConTamanio* bufferDeOperacion) {
		memcpy(bufferTotal + desplazamiento, bufferDeOperacion->buffer, bufferDeOperacion->tamanio);
		desplazamiento += bufferDeOperacion->tamanio;
		free(bufferDeOperacion->buffer);
		free(bufferDeOperacion);
	}

	list_iterate((t_list*) listaDeAlgo, agregarOperacionAListaDeBuffers);
	bufferTotal = malloc(tamanioTotal);

	memcpy(bufferTotal, &protocolo, sizeof(operacionProtocolo));
	desplazamiento += sizeof(operacionProtocolo);

	list_iterate(buffersAFoldear, agregarBufferDeOperacionABufferTotal);

	*(tamanio) = tamanioTotal;

	list_destroy(buffersAFoldear);

	return bufferTotal;
}

// ------------------------------------------------------------------------ //
// 2) SERIALIZACIONES/DESERIALIZACIONES //

operacionProtocolo empezarDeserializacion(void **buffer) {
	operacionProtocolo protocolo;
	if(*buffer == NULL) {
		return DESCONEXION;
	}
	memcpy(&protocolo, *buffer, sizeof(operacionProtocolo));
	return protocolo;
}

void* serializarSeed(seed* unaSeed, int* tamanioBuffer) {
	int desplazamiento = 0;
	int tamanioIP = strlen(unaSeed->ip) + 1;
	int tamanioPuerto = strlen(unaSeed->puerto) + 1;
	void *buffer = malloc(3*sizeof(int) + tamanioIP + tamanioPuerto);

	memcpy(buffer + desplazamiento, &tamanioIP, sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(buffer + desplazamiento, unaSeed->ip, tamanioIP);
	desplazamiento += tamanioIP;
	memcpy(buffer + desplazamiento, &tamanioPuerto, sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(buffer + desplazamiento, unaSeed->puerto, tamanioPuerto);
	desplazamiento += tamanioPuerto;
	memcpy(buffer+desplazamiento, &(unaSeed->numero), sizeof(int));
	desplazamiento += sizeof(int);

	*(tamanioBuffer) = desplazamiento;

	return buffer;
};

seed* deserializarSeed(void* buffer, int* tamanioSeed) {
	int desplazamiento = 0;
	seed* unaSeed = malloc(sizeof(seed));
	int tamanioIP, tamanioPuerto;

	memcpy(&tamanioIP, buffer + desplazamiento, sizeof(int));
	desplazamiento += sizeof(int);
	unaSeed->ip = malloc(tamanioIP);

	memcpy(unaSeed->ip, buffer + desplazamiento, tamanioIP);
	desplazamiento += tamanioIP;

	memcpy(&tamanioPuerto, buffer + desplazamiento, sizeof(int));
	desplazamiento += sizeof(int);
	unaSeed->puerto = malloc(tamanioPuerto);

	memcpy(unaSeed->puerto, buffer + desplazamiento, tamanioPuerto);
	desplazamiento += tamanioPuerto;

	memcpy(&(unaSeed->numero), buffer + desplazamiento, sizeof(int));
	desplazamiento += sizeof(int);

	*(tamanioSeed) = desplazamiento;

	return unaSeed;
}

void* serializarHandshake(int tamanioValue, int* tamanioBuffer){
	int desplazamiento = 0;
	operacionProtocolo protocoloHandshake = HANDSHAKE;
	void *buffer= malloc((sizeof(int))*2);

	memcpy(buffer + desplazamiento, &protocoloHandshake, sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(buffer + desplazamiento, &tamanioValue, sizeof(int));
	desplazamiento += sizeof(int);

	*(tamanioBuffer) = desplazamiento;

	return buffer;
}

int deserializarHandshake(void* bufferHandshake){
	int desplazamiento = 4;
	int tamanioDelValue;

	memcpy(&tamanioDelValue, bufferHandshake + desplazamiento, sizeof(int));

	free(bufferHandshake);
	return tamanioDelValue;
}

registroConNombreTabla* deserializarRegistro(void* bufferRegistro) {
	int desplazamiento = 4;
	registroConNombreTabla* unRegistro = malloc(sizeof(registroConNombreTabla));
	int largoDeNombreTabla, tamanioTimestamp, tamanioKey, largoDeValue;

	memcpy(&largoDeNombreTabla, bufferRegistro + desplazamiento, sizeof(int));
	desplazamiento += sizeof(int);
	unRegistro->nombreTabla = malloc(largoDeNombreTabla);

	memcpy(unRegistro->nombreTabla, bufferRegistro + desplazamiento, largoDeNombreTabla);
	desplazamiento+= largoDeNombreTabla;

	memcpy(&tamanioTimestamp, bufferRegistro + desplazamiento, sizeof(int));
	desplazamiento+= sizeof(int);

	memcpy( &(unRegistro->timestamp), bufferRegistro + desplazamiento, tamanioTimestamp);
	desplazamiento+= tamanioTimestamp;

	memcpy(&tamanioKey, bufferRegistro + desplazamiento , sizeof(int));
	desplazamiento+= sizeof(int);

	memcpy(&(unRegistro->key), bufferRegistro + desplazamiento, tamanioKey);
	desplazamiento+= tamanioKey;

	memcpy(&largoDeValue, desplazamiento + bufferRegistro, sizeof(int));
	desplazamiento+= sizeof(int);
	unRegistro->value = malloc(largoDeValue);

	memcpy(unRegistro->value, bufferRegistro + desplazamiento, largoDeValue);

	free(bufferRegistro);

	return unRegistro;
}

void* serializarUnRegistro(registroConNombreTabla* unRegistro, int* tamanioBuffer) {

	int desplazamiento = 0;
	int largoDeNombreTabla = strlen(unRegistro->nombreTabla) + 1;
	int largoDeValue = strlen(unRegistro->value) + 1;
	operacionProtocolo protocoloRegistro = UNREGISTRO;
	int tamanioKey = sizeof(u_int16_t);
	int tamanioTimeStamp = sizeof(time_t);
	int tamanioTotalBuffer = 5*sizeof(int) + largoDeNombreTabla + largoDeValue + tamanioKey + tamanioTimeStamp;
	void *bufferRegistro= malloc(tamanioTotalBuffer);

	//protocolo
	memcpy(bufferRegistro + desplazamiento, &protocoloRegistro, sizeof(int));
	desplazamiento+= sizeof(int);
	//Tamaño de nombre de tabla
	memcpy(bufferRegistro + desplazamiento, &largoDeNombreTabla, sizeof(int));
	desplazamiento+= sizeof(int);
	//Nombre de tabla
	memcpy(bufferRegistro + desplazamiento, unRegistro->nombreTabla, largoDeNombreTabla);
	desplazamiento+= sizeof(char)*largoDeNombreTabla;
	//Tamaño de timestamp
	memcpy(bufferRegistro + desplazamiento, &tamanioTimeStamp, sizeof(int));
	desplazamiento+= sizeof(int);
	//Nombre de timestamp
	memcpy(bufferRegistro + desplazamiento, &(unRegistro->timestamp), tamanioTimeStamp);
	desplazamiento+= tamanioTimeStamp;
	//Tamaño de key
	memcpy(bufferRegistro + desplazamiento, &tamanioKey, sizeof(int));
	desplazamiento+= sizeof(int);
	//Nombre de key
	memcpy(bufferRegistro + desplazamiento, &(unRegistro->key), sizeof(u_int16_t));
	desplazamiento+= tamanioKey;
	//Tamaño de value
	memcpy(bufferRegistro + desplazamiento, &largoDeValue, sizeof(int));
	desplazamiento+= sizeof(int);
	//Nombre de value
	memcpy(bufferRegistro + desplazamiento, unRegistro->value, largoDeValue);
	desplazamiento+= largoDeValue;

	*(tamanioBuffer) = desplazamiento;

	return bufferRegistro;
}

operacionLQL* deserializarOperacionSinFree(void* bufferOperacion, int* tamanioTotal) {
	int desplazamiento = 4;
	int tamanioOperacion,largoDeParametros;
	operacionLQL* unaOperacion = malloc(sizeof(operacionLQL));

	memcpy(&tamanioOperacion,bufferOperacion + desplazamiento, sizeof(int));
	desplazamiento += sizeof(int);
	unaOperacion->operacion = malloc(tamanioOperacion);

	memcpy((unaOperacion->operacion),bufferOperacion + desplazamiento, tamanioOperacion);
	desplazamiento += tamanioOperacion;

	memcpy(&largoDeParametros ,bufferOperacion + desplazamiento, sizeof(int));
	desplazamiento += sizeof(int);
	unaOperacion->parametros = malloc(largoDeParametros);

	memcpy(unaOperacion->parametros,bufferOperacion + desplazamiento, largoDeParametros);
	desplazamiento += largoDeParametros;

	*(tamanioTotal) = desplazamiento;

	return unaOperacion;
}

operacionLQL* deserializarOperacionLQL(void* bufferOperacion){
	int yeahIWontUseThis; // i hate C
	operacionLQL *unaOperacion = deserializarOperacionSinFree(bufferOperacion, &yeahIWontUseThis);
	free(bufferOperacion);
	return unaOperacion;
}

/*
 * Serializa una operacion LQL.
 */
void* serializarOperacionLQL(operacionLQL* operacionLQL, int* tamanio) {

	int desplazamiento = 0;
	int tamanioOperacion = strlen(operacionLQL->operacion) + 1;
	operacionProtocolo protocolo = OPERACIONLQL;
	int tamanioParametros = strlen(operacionLQL->parametros) + 1;
	int tamanioTotalBuffer = 3*sizeof(int) + tamanioOperacion + tamanioParametros;

	void *bufferOperacion= malloc(tamanioTotalBuffer);

	// Operacion de Protocolo
	memcpy(bufferOperacion + desplazamiento, &protocolo, sizeof(int));
	desplazamiento+= sizeof(int);
	// Tamaño de operacion LQL
	memcpy(bufferOperacion + desplazamiento, &tamanioOperacion, sizeof(int));
	desplazamiento+= sizeof(int);
	// operacion LQL
	memcpy(bufferOperacion + desplazamiento, (operacionLQL->operacion), tamanioOperacion);
	desplazamiento+= sizeof(char)*tamanioOperacion;
	// Tamaño de parametros
	memcpy(bufferOperacion + desplazamiento, &tamanioParametros, sizeof(int));
	desplazamiento+= sizeof(int);
	// parametros
	memcpy(bufferOperacion + desplazamiento, (operacionLQL->parametros), tamanioParametros);
	desplazamiento+= tamanioParametros;

	*tamanio = desplazamiento;

	return bufferOperacion;
}

/*
	Serializa una metadata. Toma un parametro:
	unaMetadata: La metadata a serializar
*/
void* serializarMetadata(metadata* unMetadata, int *tamanioBuffer) {

	int desplazamiento = 0;
	int tamanioDelTipoDeConsistencia = sizeof(int);
	int tamanioDeCantidadDeParticiones = sizeof(int);
	int tamanioDelTiempoDeCompactacion = sizeof(int);
	int tamanioDelNombreTabla = strlen(unMetadata->nombreTabla) + 1;

	int tamanioProtocolo = sizeof(int);
	operacionProtocolo protocolo = METADATA;
	int tamanioTotalDelBuffer = 6*sizeof(int) + tamanioDelTipoDeConsistencia + tamanioDeCantidadDeParticiones + tamanioDelTiempoDeCompactacion + tamanioDelNombreTabla;
	void *bufferMetadata= malloc(tamanioTotalDelBuffer);

	//Tamaño de operacion Protocolo
	memcpy(bufferMetadata + desplazamiento, &tamanioProtocolo, sizeof(int));
	desplazamiento += sizeof(int);
	//Operacion de Protocolo
	memcpy(bufferMetadata + desplazamiento, &protocolo, sizeof(int));
	desplazamiento+= sizeof(int);
	//Tamaño del tipo de consistencia
	memcpy(bufferMetadata + desplazamiento, &tamanioDelTipoDeConsistencia, tamanioDelTipoDeConsistencia);
	desplazamiento+= sizeof(int);
	//Tipo de consistencia
	memcpy(bufferMetadata + desplazamiento, &(unMetadata->tipoConsistencia), tamanioDelTipoDeConsistencia);
	desplazamiento+= tamanioDelTipoDeConsistencia;
	//Tamaño de la cantidad de particiones
	memcpy(bufferMetadata + desplazamiento, &tamanioDeCantidadDeParticiones, tamanioDeCantidadDeParticiones);
	desplazamiento+= sizeof(int);
	//Cantidad de particiones
	memcpy(bufferMetadata + desplazamiento, &(unMetadata->cantParticiones), tamanioDeCantidadDeParticiones);
	desplazamiento+= tamanioDeCantidadDeParticiones;
	//Tamaño del tiempoDeCompactacion
	memcpy(bufferMetadata + desplazamiento, &tamanioDelTiempoDeCompactacion, tamanioDelTiempoDeCompactacion);
	desplazamiento+= sizeof(int);
	//Tiempo de compactacion
	memcpy(bufferMetadata + desplazamiento, &(unMetadata->tiempoCompactacion), tamanioDelTiempoDeCompactacion);
	desplazamiento+= tamanioDelTiempoDeCompactacion;
	//tamanio nombre tabla
	memcpy(bufferMetadata + desplazamiento, &tamanioDelNombreTabla, tamanioDelTiempoDeCompactacion);
	desplazamiento += sizeof(int);
	//nombre tabla
	memcpy(bufferMetadata + desplazamiento, (unMetadata->nombreTabla), tamanioDelNombreTabla);
	desplazamiento += tamanioDelNombreTabla;


	*tamanioBuffer = desplazamiento;

	return bufferMetadata;
}

metadata* deserializarMetadataSinFree(void* bufferMetadata, int *tamanio) {
	int desplazamiento = 8;
	metadata* unMetadata = malloc(sizeof(metadata));
	int tamanioDelTipoDeConsistencia,tamanioDeCantidadDeParticiones,tamanioDelTiempoDeCompactacion, tamanioNombreTabla;

	memcpy(&tamanioDelTipoDeConsistencia, bufferMetadata + desplazamiento, sizeof(int));
	desplazamiento += sizeof(int);

	memcpy(&(unMetadata->tipoConsistencia), bufferMetadata + desplazamiento, sizeof(int));
	desplazamiento+= sizeof(int);

	memcpy(&tamanioDeCantidadDeParticiones, bufferMetadata + desplazamiento, sizeof(int));
	desplazamiento += sizeof(int);

	memcpy(&(unMetadata->cantParticiones), bufferMetadata + desplazamiento, sizeof(int));
	desplazamiento+= sizeof(int);

	memcpy(&tamanioDelTiempoDeCompactacion, bufferMetadata + desplazamiento, sizeof(int));
	desplazamiento += sizeof(int);

	memcpy(&(unMetadata->tiempoCompactacion), bufferMetadata + desplazamiento, sizeof(int));
	desplazamiento+= sizeof(int);

	memcpy(&tamanioNombreTabla, bufferMetadata + desplazamiento, sizeof(int));
	desplazamiento+= sizeof(int);

	unMetadata->nombreTabla = malloc(tamanioNombreTabla);
	memcpy((unMetadata->nombreTabla), bufferMetadata + desplazamiento, tamanioNombreTabla);
	desplazamiento += tamanioNombreTabla;

	*(tamanio) = desplazamiento;

	return unMetadata;
}

metadata* deserializarMetadata(void* bufferMetadata) {
	int yeahIWontUseThis; // I still hate C
	metadata* unMetadata = deserializarMetadataSinFree(bufferMetadata, &yeahIWontUseThis);
	free(bufferMetadata);
	return unMetadata;
}

void enviarError(int socket) {
	operacionProtocolo protocoloError = ERROR;
	enviar(socket, (void*) &protocoloError, sizeof(operacionProtocolo));
}
// ------------------------------------------------------------------------ //
// 3) SERIALIZACIONES/DESERIALIZACIONES DE PAQUETES/TABLAS //

void* serializarTablaGossip(t_list* tablaGossip, int* tamanio) {
	serializarPaqueteDeAlgo((void*) tablaGossip, tamanio, serializarSeed, TABLAGOSSIP);
}
void* serializarPaqueteDeOperacionesLQL(t_list* operacionesLQL, int* tamanio) {
	serializarPaqueteDeAlgo((void*) operacionesLQL, tamanio, serializarOperacionLQL, PAQUETEOPERACIONES);
}

void* serializarPaqueteDeMetadatas(t_list* metadatas, int* tamanio) {
	serializarPaqueteDeAlgo((void*) metadatas, tamanio, serializarMetadata, PAQUETEMETADATAS);
}

void recibirYDeserializarPaqueteDeOperacionesLQLRealizando(int socket, void(*accion)(operacionLQL*)) {
	recibirYDeserializarPaqueteDeAlgoRealizando(socket, accion, (void*) deserializarOperacionSinFree, liberarOperacionLQL);
}

void recibirYDeserializarPaqueteDeMetadatasRealizando(int socket, void(*accion)(metadata*)) {
	recibirYDeserializarPaqueteDeAlgoRealizando(socket, accion, deserializarMetadataSinFree, liberarMetadata);
}

void recibirYDeserializarTablaDeGossipRealizando(int socket, void(*accion)(seed*)) {
	recibirYDeserializarPaqueteDeAlgoRealizando(socket, accion, deserializarSeed, liberarSeed);
}

// ------------------------------------------------------------------------ //
// 4) SERIALIZACIONES Y ENVIO EXPRESS //

void serializarYEnviarTablaGossip(int socket, t_list* tablaGossip) {
	serializarYEnviarAlgo(socket, (void*) tablaGossip, serializarTablaGossip);
}

void serializarYEnviarPaqueteOperacionesLQL(int socket, t_list* operacionesLQL) {
	serializarYEnviarAlgo(socket, (void*) operacionesLQL, serializarPaqueteDeOperacionesLQL);
}

void serializarYEnviarPaqueteMetadatas(int socket, t_list* metadatas) {
	serializarYEnviarAlgo(socket, (void*) metadatas, serializarPaqueteDeMetadatas);
}

void serializarYEnviarHandshake(int socket, int tamanioValue) {
	serializarYEnviarAlgo(socket, (void*) tamanioValue, serializarHandshake);
}

void serializarYEnviarRegistro(int socket, registroConNombreTabla* unRegistro) {
	serializarYEnviarAlgo(socket, (void*) unRegistro, serializarUnRegistro);
}

void serializarYEnviarOperacionLQL(int socket, operacionLQL* operacionLQL) {
	serializarYEnviarAlgo(socket, (void*) operacionLQL, serializarOperacionLQL);
}

void serializarYEnviarMetadata(int socket, metadata* unaMetadata) {
	serializarYEnviarAlgo(socket, (void*) unaMetadata, serializarMetadata);
}

// ------------------------------------------------------------------------ //
// 4) FUNCIONES QUE DEBERIAN DE ESTAR EN OTRO ARCHIVO DE LAS COMMONS PERO QUEDARON IGUAL ACA PARA NO HACER MUCHOS CAMBIOS YA FUE ESA DEUDA TECNICA //

int tieneTodosLosParametros(char* operacion, int cantidadParametros) {
	char** parametrosSpliteados = string_split(operacion, " ");
	int i = 0;

	while(*(parametrosSpliteados + i) != NULL) {
		i++;
	}

	string_iterate_lines(parametrosSpliteados, free);
	free(parametrosSpliteados);
	return i == cantidadParametros;
}

char* string_trim_quotation(char* string) {
	char* stringRespuesta = malloc(strlen(string) - 1);
	char* aux = string + 1;
	int i = 1;
	while(*(string + i) != '"') {
		i++;
	}
	i--;
	size_t tamanioString = i;
	strncpy(stringRespuesta, aux, tamanioString);
	*(stringRespuesta + i) = '\0';
	free(string);
	return stringRespuesta;
}

int esCero(char* key) {
	int length = string_length(key);
	int respuesta = 1;
	int i = 0;

	while(i < length) {
		if(*(key + i) != '0') {
			break;
		}
		i++;
	}

	if(i < length) {
		respuesta = 0;
	}

	return respuesta;
}

int esNumeroParseable(char* key) {
	return (atoi(key) > 0) || esCero(key);
}

int tieneValorParseable(char* value) {
	return !string_equals_ignore_case(value, "");
}

int esConsistenciaParseable(char* consistencia) {
	return string_equals_ignore_case(consistencia, "SC") || string_equals_ignore_case(consistencia, "EC") || string_equals_ignore_case(consistencia, "SHC");
}

int tieneTodosLosParametrosParaInsert(char** parametros) {
	int i = 0;

	while(*(parametros + i) != NULL) {
		i++;
	}

	return i == 2 || i == 3;
}

int tieneTablaYKey(char** insertTablaYKey) {
	int i = 0;

	while(*(insertTablaYKey + i) != NULL) {
		i++;
	}

	return i == 3;
}

int esInsertEjecutable(char* operacion) {
	char** parametrosSpliteadosPorComillas = string_split(operacion, "\"");

	if(!tieneTodosLosParametrosParaInsert(parametrosSpliteadosPorComillas)) return 0;

	char* value = *(parametrosSpliteadosPorComillas + 1);
	char** insertTablaYKey = string_split(*(parametrosSpliteadosPorComillas + 0), " ");

	if(!tieneTablaYKey(insertTablaYKey)) return 0;

	char* timestamp = *(parametrosSpliteadosPorComillas + 2);

	if(esNumeroParseable(*(insertTablaYKey + 2)) && tieneValorParseable(string_duplicate(value))){

		if(timestamp == NULL) {
			string_iterate_lines(parametrosSpliteadosPorComillas, free);
			string_iterate_lines(insertTablaYKey, free);
			free(parametrosSpliteadosPorComillas);
			free(insertTablaYKey);
			return 1;
		}
		else if(esNumeroParseable(timestamp)) {
			string_iterate_lines(parametrosSpliteadosPorComillas, free);
			string_iterate_lines(insertTablaYKey, free);
			free(parametrosSpliteadosPorComillas);
			free(insertTablaYKey);
			return 1;
		}

	}
	string_iterate_lines(parametrosSpliteadosPorComillas, free);
	string_iterate_lines(insertTablaYKey, free);
	free(parametrosSpliteadosPorComillas);
	free(insertTablaYKey);
	return 0;
}

int esSelectEjecutable(char* operacion) {
	if(!tieneTodosLosParametros(operacion, 3)) return 0;

	char** parametrosSpliteados = string_split(operacion, " ");
	if(esNumeroParseable(*(parametrosSpliteados + 2))){
		string_iterate_lines(parametrosSpliteados, free);
		free(parametrosSpliteados);
		return 1;
	}
	string_iterate_lines(parametrosSpliteados, free);
	free(parametrosSpliteados);
	return 0;
}

int esCreateEjecutable(char* operacion) {
	if(!tieneTodosLosParametros(operacion, 5)) return 0;

	char** parametrosSpliteados = string_split(operacion, " ");

	if(esConsistenciaParseable(*(parametrosSpliteados + 2)) && esNumeroParseable(*(parametrosSpliteados + 3)) && esNumeroParseable(*(parametrosSpliteados + 4))) {
		string_iterate_lines(parametrosSpliteados, free);
		free(parametrosSpliteados);
		return 1;
	}

	string_iterate_lines(parametrosSpliteados, free);
	free(parametrosSpliteados);
	return 0;
}

int esDropEjecutable(char* operacion) {
	if(!tieneTodosLosParametros(operacion, 2)) return 0;

	char** parametrosSpliteados = string_split(operacion, " ");
	if(*(parametrosSpliteados + 1) != NULL) {
		string_iterate_lines(parametrosSpliteados, free);
		free(parametrosSpliteados);
		return 1;
	}
	string_iterate_lines(parametrosSpliteados, free);
	free(parametrosSpliteados);
	return 0;
}

int esAddEjecutable(char* operacion) {
	if(!tieneTodosLosParametros(operacion, 5)) return 0;

	char** parametrosSpliteados = string_split(operacion, " ");

	if(esNumeroParseable(*(parametrosSpliteados + 2)) && esConsistenciaParseable(*(parametrosSpliteados + 4))) {
		string_iterate_lines(parametrosSpliteados, free);
		free(parametrosSpliteados);
		return 1;
	}
	string_iterate_lines(parametrosSpliteados, free);
	free(parametrosSpliteados);
	return 0;
}

int esOperacionEjecutable(char* unaOperacion) {
	if(string_starts_with(unaOperacion, "INSERT")) {
		return esInsertEjecutable(unaOperacion);
	}
	else if (string_starts_with(unaOperacion, "SELECT")) {
		return esSelectEjecutable(unaOperacion);
	}
	else if (string_starts_with(unaOperacion, "DESCRIBE")) {
		return 1; // no hace falta verificar el describe
	}
	else if (string_starts_with(unaOperacion, "CREATE")) {
		return esCreateEjecutable(unaOperacion);
	}
	else if (string_starts_with(unaOperacion, "DROP")) {
		return esDropEjecutable(unaOperacion);
	}
	else if (string_starts_with(unaOperacion, "JOURNAL")) {
		return 1;
	}
	else if(string_starts_with(unaOperacion, "HEXDUMP")) {
		return 1;
	}
	else if(string_starts_with(unaOperacion, "ADD")) {
		return esAddEjecutable(unaOperacion);
	}
	else {
		return 0;
	}
}

void liberarOperacionLQL(operacionLQL* operacion) {
	free(operacion->operacion);
	if(operacion->parametros) {
		free(operacion->parametros);
	}
	free(operacion);
}

operacionLQL* splitear_operacion(char* operacion){
	operacionLQL* operacionAux = malloc(sizeof(operacionLQL));

	if(string_equals_ignore_case(operacion, "JOURNAL") || string_equals_ignore_case(operacion, "DESCRIBE") || string_equals_ignore_case(operacion, "HEXDUMP")) {
		operacionAux->operacion = string_duplicate(operacion);
		operacionAux->parametros = string_duplicate("ALL");
	} else {
		char** opSpliteada = string_n_split(operacion,2," ");
		operacionAux->operacion=string_duplicate(*opSpliteada);
		if(*(opSpliteada+1)){
			operacionAux->parametros=string_duplicate(*(opSpliteada+1));
			free(*(opSpliteada+1));
		} else {
			operacionAux->parametros = NULL;
		}
		free(*opSpliteada);
		free(opSpliteada);
	}


	return operacionAux;
}

void liberarParametrosSpliteados(char** parametrosSpliteados) {
	int i = 0;
	while(*(parametrosSpliteados + i)) {
		free(*(parametrosSpliteados + i));
		i++;
	}
	free(parametrosSpliteados);
}

registroConNombreTabla* armarRegistroConNombreTabla(registro* unRegistro, char* nombreTabla) {
	registroConNombreTabla* registroParaEnviar = malloc(sizeof(registroConNombreTabla));

	registroParaEnviar->nombreTabla = string_duplicate(nombreTabla);
	registroParaEnviar->value = string_duplicate(unRegistro->value);
	registroParaEnviar->key = unRegistro->key;
	registroParaEnviar->timestamp = unRegistro->timestamp;

	return registroParaEnviar;
}

void liberarRegistroConNombreTabla(registroConNombreTabla* registro) {
	free(registro->nombreTabla);
	free(registro->value);
	free(registro);
}

void liberarMetadata(metadata* unaMetadata) {
	free(unaMetadata->nombreTabla);
	free(unaMetadata);
}

void liberarSeed(seed* unaSeed) {
	free(unaSeed->ip);
	free(unaSeed->puerto);
	free(unaSeed);
}

