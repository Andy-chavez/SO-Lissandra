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

void* serializarRegistro(registro* unRegistro,char* nombreTabla);
void* serializarOperacion(int unaOperacion, char* stringDeValores);
void* serializarMetadata(metadata* unaMetadata);

#endif /* SERIALIZACION_H_ */
