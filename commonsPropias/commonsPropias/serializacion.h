/*
 * serializacion.h
 *
 *  Created on: 15 abr. 2019
 *      Author: utnso
 */

#ifndef SERIALIZACION_H_
#define SERIALIZACION_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <commons/collections/list.h>
#include <commons/string.h>
#include <time.h>
#include <stdlib.h>
#include "conexiones.h"

typedef struct {
	void* buffer;
	int tamanio;
} bufferConTamanio;

typedef struct {
	time_t timestamp;
	u_int16_t key;
	char* value;
	char* nombreTabla;
} registroConNombreTabla;

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
	PAQUETEOPERACIONES,
	UNREGISTRO,
	METADATA,
	PAQUETEMETADATAS,
	HANDSHAKE,
	DESCONEXION,
	TABLAGOSSIP,
	ERROR
} operacionProtocolo;

typedef struct {
  char* operacion;
  char* parametros;
} operacionLQL;

typedef struct {
	consistencia tipoConsistencia;
	int cantParticiones;
	int tiempoCompactacion;
	char* nombreTabla;
} metadata;

typedef struct {
	char* ip;
	char* puerto;
	int numero;
} seed;

/*
 * Para saber que es lo que me estan mandando, utilizar
 * esta funcion. Devuelve la descripcion de lo que me enviaron
 * (enum de operacionProtocolo).
 *
 * NOTA = pasar por referencia al buffer, ya que va a modificar
 * el puntero del buffer hacia lo que se debe deserializar.
 */
operacionProtocolo empezarDeserializacion(void **buffer);

/*
 * deserializa un registro y el nombre de la tabla
 * a la que pertenece, devuelve un puntero a ese
 * registro.
 *
 * NOTA 1: pasar por referencia el puntero al nombre de la tabla!
 *
 * NOTA 2: Cuando no se use mas el buffer, realizar free!!!!
 */
registroConNombreTabla* deserializarRegistro(void* bufferRegistro);

/*
 * deserializa una metadata, devuelve un puntero a esa
 * metadata.
 *
 * NOTA: Cuando no se use mas el buffer, realizar free!!!!
 */
metadata* deserializarMetadata(void* bufferMetadata);

/*
 * deserializa una operacionLQL, devuelve un puntero a esa
 * operacionLQL.
 *
 * NOTA: Cuando no se use mas el buffer, realizar free!!!!
 */
operacionLQL* deserializarOperacionLQL(void* bufferOperacion);

/*
 * Serializa un registro. Toma dos parametros:
 * unRegistro: El registro a serializar.
 * nombreTabla: La tabla a la cual pertenece este Registro!!!
 * Devuelve un buffer con ese registro serializado.
 */
void* serializarUnRegistro(registroConNombreTabla* unRegistro, int* tamanioBuffer);
/*
 * Usar antes de serializar, pasandole un string para poder agilizar el armado de la operacion LQL
 * Devuelve el struct operacionLQL completo a partir de un string
 * con sus respectiva operacion y argumentos por separado
 *
 */
operacionLQL* splitear_operacion(char* operacion);
/*
 * Serializa una operacionLQL. devuelve un buffer donde
 * se encuentra la operacion serializada.
 */
void* serializarOperacionLQL(operacionLQL* unaOperacion, int* tamanio);

/*
 * Serializa una metadata. devuelve un buffer donde
 * se encuentra la metadata serializada.
 */
void* serializarMetadata(metadata* unaMetadata, int *tamanioBuffer);

void serializarYEnviarOperacionLQL(int socket, operacionLQL* operacionLQL);

void* serializarHandshake(int tamanioValue, int* tamanioBuffer);

void serializarYEnviarRegistro(int socket, registroConNombreTabla* unRegistro);

int deserializarHandshake(void* bufferHandshake);

// si bien no tiene nada que ver, nos va a servir a todos
char* string_trim_quotation(char* string);

void liberarOperacionLQL(operacionLQL* operacion);

void serializarYEnviarHandshake(int socket, int tamanioValue);

void serializarYEnviarMetadata(int socket, metadata* unaMetadata);

registroConNombreTabla* armarRegistroConNombreTabla(registro* unRegistro, char* nombreTabla);

void liberarRegistroConNombreTabla(registroConNombreTabla* registro);

void liberarMetadata(metadata* unaMetadata);

void* serializarPaqueteDeOperacionesLQL(t_list* operacionesLQL, int* tamanio);

void serializarYEnviarPaqueteOperacionesLQL(int socket, t_list* operacionesLQL);

void recibirYDeserializarPaqueteDeOperacionesLQLRealizando(int socket, void(*accion)(operacionLQL*));

void* serializarPaqueteDeMetadatas(t_list* metadatas, int* tamanio);

void serializarYEnviarPaqueteMetadatas(int socket, t_list* metadatas);

void recibirYDeserializarPaqueteDeMetadatasRealizando(int socket, void(*accion)(metadata*));

void liberarSeed(seed* unaSeed);

void liberarParametrosSpliteados(char** parametrosSpliteados);

void recibirYDeserializarPaqueteDeAlgoRealizando(int socket, void(*accion)(void*), void*(funcionQueDeserializa)(void*, int*), void*(funcionQueLibera)(void*));

metadata* _deserializarMetadataSinFree(void* bufferMetadata, int *tamanio);

operacionLQL* _deserializarOperacionSinFree(void* bufferOperacion, int* tamanioTotal);

seed* deserializarSeed(void* buffer, int* tamanioSeed);

void* serializarSeed(seed* unaSeed, int* tamanioBuffer);

void* serializarPaqueteDeAlgo(void* listaDeAlgo, int* tamanio, void*(funcionQueSerializa)(void*, int*), operacionProtocolo protocolo);

void serializarYEnviarAlgo(int socket, void* algo, void*(funcionQueSerializa)(void*, int*));

void recibirYDeserializarTablaDeGossipRealizando(int socket, void(*accion)(seed*));

void* serializarTablaGossip(t_list* tablaGossip, int* tamanio);

void serializarYEnviarTablaGossip(int socket, t_list* tablaGossip);

int esOperacionEjecutable(char* unaOperacion);

void enviarError(int socket);

#endif /* SERIALIZACION_H_ */
