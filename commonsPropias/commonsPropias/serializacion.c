#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdint.h>
#include <string.h>
#include <commons/collections/list.h>

typedef struct {
	time_t timestamp;
	u_int16_t key;
	char* value;
} registro;

typedef enum {
	SC,
	SH,
	EC
}consistencia;

typedef enum {
	OPERACIONLQL,
	PAQUETEREGISTROS,
	METADATA
} operacionProtocolo;

typedef struct {
  char* operacion;
  char* parametros;
} operacionLQL;

typedef struct {
	consistencia tipoConsistencia;
	int cantParticiones;
	int tiempoCompactacion;
} metadata;

operacionProtocolo empezarDeserializacion(void *buffer) {
	operacionProtocolo protocolo;
	memcpy(&protocolo, buffer, sizeof(int));
	return protocolo;
}

registro* deserializarRegistro(void* bufferRegistro, char* nombreTabla) {
	int desplazamiento = 0;
	registro* unRegistro = malloc(sizeof(registro));
	int largoDeNombreTabla, tamanioTimestamp, tamanioKey, largoDeValue;

	memcpy(&largoDeNombreTabla, bufferRegistro + desplazamiento, sizeof(int));
	desplazamiento += sizeof(int);

	memcpy(nombreTabla, bufferRegistro + desplazamiento, sizeof(char)*largoDeNombreTabla);
	desplazamiento+= sizeof(char)*largoDeNombreTabla;

	memcpy(&tamanioTimestamp, bufferRegistro + desplazamiento, sizeof(int));
	desplazamiento+= sizeof(int);

	memcpy( &(unRegistro->timestamp), bufferRegistro + desplazamiento, tamanioTimestamp); //nos parece con el magic que el tamanioTimestamp esta mal
	desplazamiento+= tamanioTimestamp;

	memcpy(&tamanioKey, bufferRegistro + desplazamiento , tamanioKey);
	desplazamiento+= sizeof(int);

	memcpy(&(unRegistro->key), bufferRegistro + desplazamiento, tamanioKey);
	desplazamiento+= tamanioKey;

	memcpy(&largoDeValue, bufferRegistro + desplazamiento, largoDeValue);
	desplazamiento+= sizeof(int);

	memcpy(&(unRegistro->value), bufferRegistro + desplazamiento, sizeof(char)*largoDeValue);

	return unRegistro;
}

/*
 * Serializa un registro. Toma dos parametros:
 * unRegistro: El registro a serializar.
 * nombreTabla: La tabla a la cual pertenece este Registro!!!
 */

void* serializarRegistro(registro* unRegistro,char* nombreTabla) {

	int desplazamiento = 0;
	int largoDeNombreTabla = strlen(nombreTabla) + 1;
	int largoDeValue = strlen(unRegistro->value) + 1;
	int tamanioKey = sizeof(u_int16_t);
	int tamanioTimeStamp = sizeof(time_t);
	int tamanioTotalBuffer = 4*sizeof(int) + largoDeNombreTabla + sizeof(char)*largoDeNombreTabla + tamanioKey + tamanioTimeStamp;
	void *bufferRegistro= malloc(tamanioTotalBuffer);

	//Tamaño de nombre de tabla
	memcpy(bufferRegistro + desplazamiento, &largoDeNombreTabla, sizeof(int));
	desplazamiento+= sizeof(int);
	//Nombre de tabla
	memcpy(bufferRegistro + desplazamiento, nombreTabla, sizeof(char)*largoDeNombreTabla);
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
	memcpy(bufferRegistro + desplazamiento, &(unRegistro->value), sizeof(char)*largoDeValue);
	desplazamiento+= sizeof(char)*largoDeValue;
	return bufferRegistro;
}

//void *memcpy(void *dest, const void *src, size_t n);
operacionLQL* deserializarOperacionLQL(void* bufferOperacion){
	int desplazamiento = 0;
	int tamanioOperacion,largoDeParametros,tamanioCantParametros;
	operacionLQL* unaOperacion;

	//deserialice desde el enum al string de valores

	//Agregar memcpy del tamanioCantParametros y Cant Parametros;

	memcpy(&tamanioOperacion,bufferOperacion + desplazamiento, sizeof(int));
	desplazamiento += sizeof(int);

	memcpy((unaOperacion->operacion),bufferOperacion + desplazamiento, sizeof(char)*tamanioOperacion);
	desplazamiento += sizeof(char)*tamanioOperacion;

	memcpy(&largoDeParametros ,bufferOperacion + desplazamiento, sizeof(int));
	desplazamiento += sizeof(int);

	memcpy(&(unaOperacion->parametros),bufferOperacion + desplazamiento, sizeof(char)*largoDeParametros);
	desplazamiento += sizeof(char)*largoDeParametros;

	return unaOperacion;
}

/*
 * Serializa una operacion LQL.
 */
void* serializarOperacionLQL(operacionLQL* operacionLQL) {

	int desplazamiento = 0;
	int tamanioOperacion = strlen(operacionLQL->operacion) + 1;
	int tamanioProtocolo = sizeof(int);
	operacionProtocolo protocolo = OPERACIONLQL;
	int tamanioParametros = strlen(operacionLQL->parametros) + 1;
	int tamanioTotalBuffer = 4*sizeof(int) + sizeof(char)*(tamanioOperacion + tamanioParametros);

	void *bufferOperacion= malloc(tamanioTotalBuffer);

	// Tamaño de operacion Protocolo
	memcpy(bufferOperacion + desplazamiento, &tamanioProtocolo, sizeof(int));
	desplazamiento += sizeof(int);
	// Operacion de Protocolo
	memcpy(bufferOperacion + desplazamiento, &protocolo, sizeof(int));
	desplazamiento+= sizeof(int);
	// Tamaño de operacion LQL
	memcpy(bufferOperacion + desplazamiento, &tamanioOperacion, sizeof(int));
	desplazamiento+= sizeof(int);
	// operacion LQL
	memcpy(bufferOperacion + desplazamiento, (operacionLQL->operacion), sizeof(char)*tamanioOperacion);
	desplazamiento+= sizeof(char)*tamanioOperacion;
	// Tamaño de parametros
	memcpy(bufferOperacion + desplazamiento, &tamanioParametros, sizeof(int));
	desplazamiento+= sizeof(int);
	// parametros
	memcpy(bufferOperacion + desplazamiento, (operacionLQL->parametros), sizeof(char)*tamanioParametros);
	desplazamiento+= sizeof(char)*tamanioOperacion;
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
	int desplazamiento = 0;
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


int main(){ return 0;}
