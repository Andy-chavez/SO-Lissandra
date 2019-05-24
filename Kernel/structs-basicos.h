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

int quantumMax; //sacar esto de archivo de config
t_list* cola_proc_nuevos;  //use esta en el caso del run
t_list* cola_proc_listos; //esto me da medio inncesario porque de new ->ready es como que no hay tanta diferencia, alias estructuras para crear
t_list* cola_proc_terminados;
t_list* cola_proc_ejecutando;
// t_list* cola_proc_bloqueados; es necesaria? al no haber tiempo io como que bloqueados no se en que casos llenarla

typedef struct{
	char* operacion;
	char* argumentos;
	int ejecutado; //0 si no se ejecuto, 1 si se ejecuto
	t_list* instruccion; //fila
	//t_list* pcb_siguiente;  //columna
	//TODO agregar mas campos 1
}pcb;
typedef struct{
	char* operacion;
	char* argumentos;
	int ejecutado; //0 si no se ejecuto, 1 si se ejecuto
	//t_list* instruccion_siguiente;
	//TODO agregar mas campos 2
}instruccion;
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

void liberarConfigYLogs(configYLogs *archivos) {
	log_destroy(archivos->log);
	config_destroy(archivos->config);
	free(archivos);
}

#endif /* STRUCTS_BASICOS_H_ */
