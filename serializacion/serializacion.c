#include<stdio.h>
#include<stdlib.h>
#include<time.h>
#include<stdint.h>
#include<string.h>

typedef struct {
	time_t timestamp;
	u_int16_t key;
	char* value;
} registro;

typedef enum {
	OPERACIONLQL,
	PAQUETEREGISTROS
} operacionesProtocolo;
/*
 * Serializa un registro. Toma dos parametros:
 * unRegistro: El registro a serializar.
 * nombreTabla: La tabla a la cual pertenece este Registro!!!
 */


void* serializarRegistro(registro* unRegistro,char* nombreTabla)
{
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
	memcpy(bufferRegistro + desplazamiento, &nombreTabla, sizeof(char)*largoDeNombreTabla);
	desplazamiento+= sizeof(char)*largoDeNombreTabla;
	//Tamaño de timestamp
	memcpy(bufferRegistro + desplazamiento, &tamanioTimeStamp, tamanioTimeStamp);
	desplazamiento+= sizeof(int);
	//Nombre de timestamp
	memcpy(bufferRegistro + desplazamiento, &(unRegistro->timestamp), sizeof(time_t));
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

/*
 *
 */
void* serializarOperacion(int unaOperacion, char* stringDeValores) {
	int desplazamiento = 0;
	int largoDeStringValue = strlen(stringDeValores) + 1;
	int tamanioOperacion = sizeof(int);
	operacionProtocolo protocolo = OPERACIONLQL;
	int tamanioTotalBuffer = 5*sizeof(int) + sizeof(char)*largoDeStringValue;
	void *bufferOperacion= malloc(tamanioTotalBuffer);


	//Tamaño de operacion Protocolo
	memcpy(bufferOperacion + desplazamiento, &tamanioOperacion, tamanioOperacion);
	desplazamiento += sizeof(int);
	//Operacion de Protocolo
	memcpy(bufferOperacion + desplazamiento, &protocolo, tamanioOperacion);
	desplazamiento+= sizeof(int);
	//Tamaño de operacion LQL(enum)
	memcpy(bufferOperacion + desplazamiento, &tamanioOperacion, tamanioOperacion);
	desplazamiento+= sizeof(int);
	//enum de operacion LQL
	memcpy(bufferOperacion + desplazamiento, &unaOperacion, sizeof(int));
	desplazamiento+= sizeof(int);
	//Tamaño de String de valores
	memcpy(bufferOperacion + desplazamiento, &largoDeStringValue, sizeof(int));
	desplazamiento+= sizeof(int);
	//String de valores
	memcpy(bufferOperacion + desplazamiento, &stringDeValores, sizeof(char)*largoDeStringValue);
	desplazamiento+= sizeof(char)*largoDeStringValue;
	return bufferOperacion;
}

/*
	Serializa una metadata. Toma un parametro:
	unaMetadata: La metadata a serializar
*/
void* serializarMetadata(metadata* unaMetadata)
{
	int desplazamiento = 0;
	int tamanioDelTipoDeConsistencia = sizeof(int);
	int tamanioDeCantidadDeParticiones = sizeof(int);
	int tamanioDelTiempoDeCompactacion = sizeof(int);
	int tamanioTotalDelBuffer = tamanioDelTipoDeConsistencia + tamanioDeCantidadDeParticiones + tamanioDelTiempoDeCompactacion;
	void *bufferMetadata= malloc(tamanioTotalDelBuffer);

	//Tamaño del tipo de consistencia
	memcpy(bufferRegistro + desplazamiento, &tamanioDelTipoDeConsistencia, tamanioDelTipoDeConsistencia);
	desplazamiento+= tamanioDelTipoDeConsistencia;
	//Tipo de consistencia
	memcpy(bufferRegistro + desplazamiento, &(unaMetadata->tipoConsistencia), tamanioDelTipoDeConsistencia);
	desplazamiento+= tamanioDelTipoDeConsistencia;
	//Tamaño de la cantidad de particiones
	memcpy(bufferRegistro + desplazamiento, &tamanioDeCantidadDeParticiones, tamanioDeCantidadDeParticiones);
	desplazamiento+= tamanioDeCantidadDeParticiones;
	//Cantidad de particiones
	memcpy(bufferRegistro + desplazamiento, &(unaMetadata->cantParticiones), tamanioDeCantidadDeParticiones);
	desplazamiento+= tamanioDeCantidadDeParticiones;
	//Tamaño del tiempoDeCompactacion
	memcpy(bufferRegistro + desplazamiento, &tamanioDelTiempoDeCompactacion, tamanioDelTiempoDeCompactacion);
	desplazamiento+= tamanioDelTiempoDeCompactacion;
	//Tiempo de compactacion
	memcpy(bufferRegistro + desplazamiento, &(unaMetadata->tiempoCompactacion), tamanioDelTiempoDeCompactacion);
	desplazamiento+= tamanioDelTiempoDeCompactacion;

	return bufferMetadata;
}


int main(){ return 0;}
