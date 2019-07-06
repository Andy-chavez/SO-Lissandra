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

//pthread_mutex_t mutexMemtable;
pthread_mutex_t mutexLogger;
pthread_mutex_t mutexLoggerConsola;
pthread_mutex_t mutexListaDeTablas;
pthread_mutex_t mutexBitarray;
pthread_mutex_t mutexTiempoDump;
pthread_mutex_t mutexRetardo;


t_log* logger;
t_log* loggerConsola;

int tamanioBloques;
int cantDeBloques;
char* magicNumber;
t_config* archivoMetadata;
//hasta aca todo se saca del metadata

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
t_bitarray* bitarray;
sem_t binarioLFS;



#endif /* SRC_VARIABLESGLOBALES_H_ */
