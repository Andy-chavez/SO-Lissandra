/*
 * structs-basicos.h
 *
 *  Created on: 25 abr. 2019
 *      Author: utnso
 */

#ifndef KERNEL_STRUCTS_BASICOS_H_
#define KERNEL_STRUCTS_BASICOS_H_
#include <commons/log.h>
#include <commons/config.h>
#include <commons/collections/list.h>
#include <commonsPropias/serializacion.h>
#include <stdlib.h>
#include <semaphore.h>

/******************************STRUCTS******************************************/
typedef struct{
	char* operacion;
	int ejecutado; //0 si no se ejecuto, 1 si se ejecuto
	t_list* instruccion; //fila
	//t_list* pcb_siguiente;  //columna
	//TODO agregar mas campos 1
}pcb;
typedef struct{
	char* operacion;
	int ejecutado; //0 si no se ejecuto, 1 si se ejecuto
	//t_list* instruccion_siguiente;
	//TODO agregar mas campos 2
}instruccion;

//typedef struct{
//	int numeroDeMemoria;
//	consistencia *criterioAsociado; //malloc dps de saber cuantos criterios me devuelve el pool
//}memoria;
//
//typedef struct{
//	consistencia unCriterio;
//	int *memoriasAsociadas; //malloc dps de saber cuantas memorias me devuelve el pool
//}criterio;

typedef struct{
	int numero;
	int puerto;
	consistencia consistencias[3];
}memoria;

typedef struct{
	char* nombreDeTabla;
	consistencia consistenciaDeTabla;
}tabla; //ver si es necesario agregar algo mas

/*typedef enum {
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
*/
typedef struct {
	t_config* config;
	t_log* log;
} configYLogs;

/******************************VARIABLES GLOBALES******************************************/
t_list* cola_proc_nuevos;  //use esta en el caso del run
t_list* cola_proc_listos;
t_list* cola_proc_terminados;
t_list* cola_proc_ejecutando;
t_list* memorias;
sem_t hayNew;
sem_t hayReady;
pthread_mutex_t colaNuevos;
pthread_mutex_t colaListos;
pthread_mutex_t colaTerminados;
char * pathConfig ="/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/Kernel/KERNEL_CONFIG_EJEMPLO";
char* ipMemoria;
char* puertoMemoria;
configYLogs *kernel_configYLog;
int quantumMax;
int multiprocesamiento;
int metadataRefresh;
int sleepEjecucion;


#endif /* KERNEL_STRUCTS_BASICOS_H_ */
