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
	time_t timestamp;
	uint16_t key;
	char* value;
} registro;

typedef struct {
	int numeroPagina;
	void *unRegistro;
	flagModificado flag;
} paginaEnTabla;

typedef struct {
	char *nombreTabla;
	t_list* tablaPaginas;
} segmento;

typedef struct {
	t_list* tablaSegmentos;
	void *base;
	void *limite;
	int tamanioMaximoValue;
	int *seeds;
} memoria;

typedef struct {
	void* buffer;
	int tamanio;
} bufferDePagina;

typedef struct {
	int tamanio;
} datosInicializacion;

int SOCKET_LFS;

sem_t MUTEX_LOG;
sem_t MUTEX_OPERACION;
sem_t BINARIO_SOCKET_KERNEL;
sem_t MUTEX_SOCKET_LFS;
memoria* MEMORIA_PRINCIPAL;
configYLogs *ARCHIVOS_DE_CONFIG_Y_LOG;
int TAMANIO_UN_REGISTRO_EN_MEMORIA;

#endif
