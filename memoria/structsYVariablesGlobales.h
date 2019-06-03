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

int socketLissandraFS;

sem_t mutex_operacion;
sem_t binario_socket;
memoria* memoriaPrincipal;
configYLogs *archivosDeConfigYLog;
int tamanioDeUnRegistroEnMemoria;

#endif
