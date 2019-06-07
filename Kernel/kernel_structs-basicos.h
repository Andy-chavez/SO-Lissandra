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

#define HASH 2
#define STRONG 1
#define EVENTUAL 0
/******************************STRUCTS******************************************/
typedef struct{
	char* operacion;
	int ejecutado; //0 si no se ejecuto, 1 si se ejecuto
	t_list* instruccion; //fila
	//TODO agregar mas campos 1
}pcb;
typedef struct{
	char* operacion;
	int ejecutado; //0 si no se ejecuto, 1 si se ejecuto
	//t_list* instruccion_siguiente;
	//TODO agregar mas campos 2
}instruccion;

typedef struct{
	consistencia unCriterio;
	t_list* memorias;
}criterio;

typedef struct{
	int numero;
	char* puerto; //necesario?
	char* ip;
}memoria;

typedef struct{
	char* nombreDeTabla;
	consistencia consistenciaDeTabla;
}tabla; //ver si es necesario agregar algo mas

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
t_list* tablas;
criterio criterios[3];
sem_t hayNew;
sem_t hayReady;
pthread_mutex_t colaNuevos;
pthread_mutex_t colaListos;
pthread_mutex_t colaTerminados;
pthread_mutex_t log;
char * pathConfig ="/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/Kernel/KERNEL_CONFIG_EJEMPLO";
char* ipMemoria;
char* puertoMemoria;
configYLogs *kernel_configYLog;
int quantumMax;
int multiprocesamiento;
int metadataRefresh;
int sleepEjecucion;

//------------------------TEST CHEHCKPOINT --------------------------------------
int numPrueba = 1;


#endif /* KERNEL_STRUCTS_BASICOS_H_ */
