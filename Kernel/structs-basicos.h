/*
 * structs-basicos.h
 *
 *  Created on: 25 abr. 2019
 *      Author: utnso
 */

#ifndef STRUCTS_BASICOS_H_
#define STRUCTS_BASICOS_H_
#include <commons/log.h>
#include <commons/config.h>
#include <commons/collections/list.h>
#include <stdlib.h>

t_list* cola_proc_nuevos;
t_list* cola_proc_listos;
t_list* cola_proc_terminados;
t_list* cola_proc_ejecutando;
t_list* cola_proc_bloqueados;

typedef struct{
	char* operacion;
	char* argumentos;
	t_list* instrruccion_siguiente;
	t_list* pcb_siguiente;
	//TODO agregar mas campos
}pcb;
typedef enum {
	SC, // UNA
	SH, // MUCHAS
	EC  // MUCHAS
}criterios;

typedef struct{
	int numeroDeMemoria;
	criterios *criterioAsociado; //malloc dps de saber cuantos criterios me devuelve el pool
}memoria;

typedef struct{
	criterios unCriterio;
	int *memoriasAsociadas; //malloc dps de saber cuantas memorias me devuelve el pool
}criterio;

typedef struct{
	char* nombreDeTabla;
	criterios criterioDeTabla;
}tabla; //ver si es necesario agregar algo mas

typedef enum {
	INSERT,
	CREATE,
	DESCRIBETABLE,
	DESCRIBEALL,
	DROP,
	JOURNAL,
	SELECT,
	RUN,
	METRICS,
	ADD
}caso;

typedef struct {
	t_config* config;
	t_log* log;
} configYLogs;

/************************************************************************/
void liberarConfigYLogs(configYLogs *archivos);

#endif /* STRUCTS_BASICOS_H_ */
