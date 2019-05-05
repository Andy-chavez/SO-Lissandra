/*
 * serializacion.h
 *
 *  Created on: 15 abr. 2019
 *      Author: utnso
 */

#ifndef SERIALIZACION_H_
#define SERIALIZACION_H_

#include <time.h>
#include <stdlib.h>

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

/*
 * Para saber que es lo que me estan mandando, utilizar
 * esta funcion. Devuelve la descripcion de lo que me enviaron
 * (enum de operacionProtocolo).
 */
operacionProtocolo empezarDeserializacion(void *buffer);

/*
 * deserializa un registro y el nombre de la tabla
 * a la que pertenece, devuelve un puntero a ese
 * registro.
 *
 * NOTA 1: pasar por referencia el puntero al nombre de la tabla!
 *
 * NOTA 2: Cuando no se use mas el buffer, realizar free!!!!
 */
registro* deserializarRegistro(void* bufferRegistro, char** nombreTabla);

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
void* serializarRegistro(registro* unRegistro,char* nombreTabla);

/*
 * Serializa una operacionLQL. devuelve un buffer donde
 * se encuentra la operacion serializada.
 */
void* serializarOperacionLQL(operacionLQL* unaOperacion);

/*
 * Serializa una metadata. devuelve un buffer donde
 * se encuentra la metadata serializada.
 */
void* serializarMetadata(metadata* unaMetadata);

#endif /* SERIALIZACION_H_ */
