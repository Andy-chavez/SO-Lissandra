#include "serializacion.h"

operacionProtocolo empezarDeserializacion(void **buffer) {
	operacionProtocolo protocolo;
	if(*buffer == NULL) {
		return DESCONEXION;
	}
	memcpy(&protocolo, *buffer, sizeof(operacionProtocolo));
	return protocolo;
}

void liberarOperacionLQL(operacionLQL* operacion) {
	free(operacion->operacion);
	free(operacion->parametros);
	free(operacion);
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

void serializarYEnviarHandshake(int socket, int tamanioValue) {
	int tamanioBuffer;
	void* bufferAEnviar = serializarHandshake(tamanioValue, &tamanioBuffer);
	enviar(socket, bufferAEnviar, tamanioBuffer);
	free(bufferAEnviar);
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
	memcpy(bufferRegistro + desplazamiento, &tamanioTimeStamp, tamanioTimeStamp);
	desplazamiento+= sizeof(int);
	//Nombre de timestamp
	memcpy(bufferRegistro + desplazamiento, &(unRegistro->timestamp), tamanioTimeStamp);
	desplazamiento+= tamanioTimeStamp;
	//Tamaño de key
	memcpy(bufferRegistro + desplazamiento, &tamanioKey, tamanioKey);
	desplazamiento+= sizeof(int);
	//Nombre de key
	memcpy(bufferRegistro + desplazamiento, &(unRegistro->key), sizeof(u_int16_t));
	desplazamiento+= tamanioKey;
	//Tamaño de value
	memcpy(bufferRegistro + desplazamiento, &largoDeValue, largoDeValue);
	desplazamiento+= sizeof(int);
	//Nombre de value
	memcpy(bufferRegistro + desplazamiento, unRegistro->value, largoDeValue);
	desplazamiento+= largoDeValue;

	*(tamanioBuffer) = desplazamiento;

	return bufferRegistro;
}

void serializarYEnviarRegistro(int socket, registroConNombreTabla* unRegistro) {
	int tamanioAEnviar;
	void* bufferAEnviar = serializarUnRegistro(unRegistro, &tamanioAEnviar);
	enviar(socket, bufferAEnviar, tamanioAEnviar);
	free(bufferAEnviar);
}

//void *memcpy(void *dest, const void *src, size_t n);
operacionLQL* deserializarOperacionLQL(void* bufferOperacion){
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

operacionLQL* splitear_operacion(char* operacion){
	operacionLQL* operacionAux = malloc(sizeof(operacionLQL));
	char** opSpliteada;

	if(string_equals_ignore_case(operacion, "JOURNAL") || string_equals_ignore_case(operacion, "DESCRIBE")) {
		operacionAux->operacion = operacion;
		operacionAux->parametros = "ALL";
	} else {
		opSpliteada = string_n_split(operacion,2," ");
		operacionAux->operacion=*opSpliteada;
		operacionAux->parametros=*(opSpliteada+1);
	}


	return operacionAux;
}

void serializarYEnviarOperacionLQL(int socket, operacionLQL* operacionLQL) {
	int tamanioBuffer;
	void* bufferAEnviar = serializarOperacionLQL(operacionLQL, &tamanioBuffer);
	enviar(socket, bufferAEnviar, tamanioBuffer);
	free(bufferAEnviar);
}

/*
	Serializa una metadata. Toma un parametro:
	unaMetadata: La metadata a serializar
*/
void* serializarMetadata(metadata* unMetadata) {

	int desplazamiento = 0;
	int tamanioDelTipoDeConsistencia = sizeof(int);
	int tamanioDeCantidadDeParticiones = sizeof(int);
	int tamanioDelTiempoDeCompactacion = sizeof(int);

	int tamanioProtocolo = sizeof(int);
	operacionProtocolo protocolo = METADATA;
	int tamanioTotalDelBuffer = tamanioDelTipoDeConsistencia + tamanioDeCantidadDeParticiones + tamanioDelTiempoDeCompactacion;
	void *bufferMetadata= malloc(tamanioTotalDelBuffer);

	//Tamaño de operacion Protocolo
	memcpy(bufferMetadata + desplazamiento, &tamanioProtocolo, sizeof(int));
	desplazamiento += sizeof(int);
	//Operacion de Protocolo
	memcpy(bufferMetadata + desplazamiento, &protocolo, sizeof(int));
	desplazamiento+= sizeof(int);
	//Tamaño del tipo de consistencia
	memcpy(bufferMetadata + desplazamiento, &tamanioDelTipoDeConsistencia, tamanioDelTipoDeConsistencia);
	desplazamiento+= tamanioDelTipoDeConsistencia;
	//Tipo de consistencia
	memcpy(bufferMetadata + desplazamiento, &(unMetadata->tipoConsistencia), tamanioDelTipoDeConsistencia);
	desplazamiento+= tamanioDelTipoDeConsistencia;
	//Tamaño de la cantidad de particiones
	memcpy(bufferMetadata + desplazamiento, &tamanioDeCantidadDeParticiones, tamanioDeCantidadDeParticiones);
	desplazamiento+= tamanioDeCantidadDeParticiones;
	//Cantidad de particiones
	memcpy(bufferMetadata + desplazamiento, &(unMetadata->cantParticiones), tamanioDeCantidadDeParticiones);
	desplazamiento+= tamanioDeCantidadDeParticiones;
	//Tamaño del tiempoDeCompactacion
	memcpy(bufferMetadata + desplazamiento, &tamanioDelTiempoDeCompactacion, tamanioDelTiempoDeCompactacion);
	desplazamiento+= tamanioDelTiempoDeCompactacion;
	//Tiempo de compactacion
	memcpy(bufferMetadata + desplazamiento, &(unMetadata->tiempoCompactacion), tamanioDelTiempoDeCompactacion);

	desplazamiento+= tamanioDelTiempoDeCompactacion;

	return bufferMetadata;
}

metadata* deserializarMetadata(void* bufferMetadata) {
	int desplazamiento = 4;
	metadata* unMetadata = malloc(sizeof(metadata));
	int tamanioDelTipoDeConsistencia,tamanioDeCantidadDeParticiones,tamanioDelTiempoDeCompactacion;

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

	return unMetadata;
}

char* string_trim_quotation(char* string) {
	char *stringRespuesta = malloc(strlen(string) - 1);

	int i = 1;
	while(*(string + i) != '"') {
		*(stringRespuesta + (i - 1)) = *(string + i);
		i++;
	}
	*(stringRespuesta + (i - 1)) = '\0';
	return stringRespuesta;
};
