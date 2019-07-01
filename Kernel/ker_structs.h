#ifndef KER_STRUCTS_H_
#define KER_STRUCTS_H_

#include <commons/log.h>
#include <commons/config.h>
#include <commons/collections/list.h>
#include <stdlib.h>
#include <semaphore.h>
#include <commons/string.h>
#include <commonsPropias/serializacion.h>
#include <stdbool.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <unistd.h>

#define HASH 2
#define STRONG 1
#define EVENTUAL 0
/******************************STRUCTS******************************************/
typedef struct{
	char* operacion;
	int ejecutado; //0 si no se ejecuto, 1 si se ejecuto
	t_list* instruccion;
}pcb;
typedef struct{
	char* operacion;
	int ejecutado; //0 si no se ejecuto, 1 si se ejecuto
}instruccion;
typedef struct{
	consistencia unCriterio;
	int cantidadSelects;
	int cantidadInserts;
	clock_t tiempoSelects; //aca es el tiempo total, realizar division cuando loggee
	clock_t tiempoInserts;
	t_list* memorias;
}criterio;
typedef struct{
	int numero;
	char* puerto;
	char* ip;
	int cantidadIns;
	int cantidadSel;
}memoria;
typedef struct{
	char* nombreDeTabla;
	consistencia consistenciaDeTabla;
}tabla;
typedef struct {
	t_config* config;
	t_log* log;
} configYLogs;
typedef struct {
	pthread_t* thread;
	int numero;
} t_thread;

t_log* logMetrics;
/******************************VARIABLES GLOBALES******************************************/
t_list* cola_proc_nuevos;
t_list* cola_proc_listos;
t_list* cola_proc_terminados;
t_list* memorias;
t_list* tablas;
t_list* rrThreads;
criterio criterios[3];

sem_t hayNew;
sem_t hayReady;
sem_t finalizar;
sem_t modificables;

pthread_mutex_t quantum;
pthread_mutex_t sleepExec;
pthread_mutex_t mMetadataRefresh;
pthread_mutex_t mMemorias;
pthread_mutex_t mTablas;
pthread_mutex_t mEventual;
pthread_mutex_t mStrong;
pthread_mutex_t mHash;
pthread_mutex_t colaListos;
pthread_mutex_t colaNuevos;
pthread_mutex_t colaTerminados;
pthread_mutex_t mLog;
pthread_mutex_t mThread;
pthread_mutex_t mConexion;
pthread_mutex_t mLogMetrics;
//char * pathConfig ="/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/Kernel/KERNEL_CONFIG_EJEMPLO";

char* pathConfig;
char* ipMemoria;
char* puertoMemoria;

configYLogs *kernel_configYLog;

int destroy = 0;
int multiprocesamiento;
int timedGossip;
// -------------------- CAMBIAN EN TIEMPO DE EXEC ------------------------
int quantumMax;
int metadataRefresh;
int sleepEjecucion;

#endif /* KER_STRUCTS_H_ */
