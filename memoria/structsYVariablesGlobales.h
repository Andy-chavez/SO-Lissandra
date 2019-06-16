#ifndef STRUCTSYVARIABLESGLOBALES_H_
#define STRUCTSYVARIABLESGLOBALES_H_

#include <time.h>
#include <inttypes.h>
#include <commons/config.h>
#include <commons/log.h>
#include <stdlib.h>
#include <commons/collections/list.h>
#include <commons/memory.h>
#include <pthread.h>
#include <commons/string.h>
#include <stdbool.h>
#include <stdio.h>
#include <semaphore.h>

typedef struct {
	t_config* config;
	t_log* logger;
} configYLogs;

typedef enum {
	NO,
	SI
} flagModificado;

typedef struct {
	int numeroPagina;
	int marco;
	time_t timestamp;
	flagModificado flag;
} paginaEnTabla;

typedef struct {
	int marco;
	int estaEnUso;
	int tamanioValue;
	void* lugarEnMemoria;
} marco;

typedef struct {
	char *nombreTabla;
	t_list* tablaPaginas;
} segmento;

typedef struct {
	t_list* tablaSegmentos;
	void *base;
	void *limite;
	int tamanioMaximoValue;
} memoria;

typedef struct {
	void* buffer;
	int tamanio;
} bufferDePagina;

typedef struct {
	int tamanio;
} datosInicializacion;

int SOCKET_LFS;

int TAMANIO_UN_REGISTRO_EN_MEMORIA;
int RETARDO_GOSSIP;
int RETARDO_JOURNAL;
int RETARDO_MEMORIA;

sem_t MUTEX_LOG;
sem_t MUTEX_LOG_CONSOLA;

sem_t MUTEX_OPERACION;

sem_t BINARIO_SOCKET_KERNEL;
sem_t MUTEX_SOCKET_LFS;

sem_t MUTEX_RETARDO_MEMORIA;
sem_t MUTEX_RETARDO_GOSSIP;
sem_t MUTEX_RETARDO_JOURNAL;

sem_t MUTEX_TABLA_GOSSIP;

memoria* MEMORIA_PRINCIPAL;
t_list* TABLA_MARCOS;
t_list* TABLA_GOSSIP;

configYLogs *ARCHIVOS_DE_CONFIG_Y_LOG;
t_log* LOGGER_CONSOLA;



#endif
