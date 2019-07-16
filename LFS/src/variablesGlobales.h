/*
 * variablesGlobales.h
 *
 *  Created on: 22 jun. 2019
 *      Author: utnso
 */

#ifndef SRC_VARIABLESGLOBALES_H_
#define SRC_VARIABLESGLOBALES_H_
#include <commons/collections/list.h>
#include <commons/bitarray.h>
#include <semaphore.h>

sem_t mutexMemtable;
sem_t mutexLogger;
sem_t mutexLoggerConsola;
sem_t mutexListaDeTablas;
sem_t mutexBitarray;
sem_t mutexTiempoDump;
sem_t mutexRetardo;
sem_t mutexResultadosConsola;
sem_t mutexResultados;
sem_t binarioSocket;
sem_t MUTEX_TABLA_THREADS;

t_log* logger;
t_log* loggerConsola;
t_log* loggerResultados;
t_log* loggerResultadosConsola;

int tamanioBloques;
int cantDeBloques;
char* magicNumber;
t_config* archivoMetadata;
//hasta aca se saca del metadata

char* ipLisandra;
char* puertoLisandra;
char* puntoMontaje;
int tiempoDump;

int tamanioValue;
int retardo;
t_config* archivoDeConfig;
//hasta aca del archivo de config
t_list* memtable;
t_list* listaDeTablas;
t_list* TABLA_THREADS;
t_bitarray* bitarray;
sem_t binarioLFS;



#endif /* SRC_VARIABLESGLOBALES_H_ */
