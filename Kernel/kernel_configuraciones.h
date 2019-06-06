/*
 * configuraciones.h
 *
 *  Created on: 26 may. 2019
 *      Author: utnso
 */

#ifndef KERNEL_CONFIGURACIONES_H_
#define KERNEL_CONFIGURACIONES_H_
#include <commons/config.h>
#include <commons/string.h>
#include "kernel_structs-basicos.h"

void liberarConfigYLogs();
void kernel_inicializar();

// _________________________________________.: LLENAR/VACIAR VARIABLES GLOBALES :.____________________________________________
void kernel_inicializarSemaforos(){
	pthread_mutex_init(&colaNuevos, NULL);
	pthread_mutex_init(&colaListos, NULL);
	pthread_mutex_init(&colaTerminados, NULL);
	sem_init(&hayNew,0,0);
	sem_init(&hayReady,0,0);
}
void kernel_crearListas(){
	cola_proc_nuevos = list_create();
	cola_proc_listos = list_create();
	cola_proc_terminados = list_create();
	cola_proc_ejecutando = list_create();
	memorias = list_create();
}
void kernel_inicializar(){
	kernel_configYLog= malloc(sizeof(configYLogs));
	kernel_configYLog->config = config_create(pathConfig);
	kernel_configYLog->log = log_create("KERNEL.log", "KERNEL", 1, LOG_LEVEL_INFO);
	ipMemoria = config_get_string_value(kernel_configYLog->config ,"IP_MEMORIA");
	puertoMemoria = config_get_string_value(kernel_configYLog->config,"PUERTO_MEMORIA");
	quantumMax = config_get_int_value(kernel_configYLog->config,"QUANTUM");
	multiprocesamiento =config_get_int_value(kernel_configYLog->config,"MULTIPROCESAMIENTO");
	metadataRefresh = config_get_int_value(kernel_configYLog->config,"METADATA_REFRESH");
	sleepEjecucion = config_get_int_value(kernel_configYLog->config,"SLEEP_EJECUCION");
	kernel_crearListas();
	kernel_inicializarSemaforos();
}
void liberarConfigYLogs() {
//	free(pathConfig);
//	free(kernel_configYLog->config);
//	free(kernel_configYLog->log);
//	free(ipMemoria);
//	free(puertoMemoria);
	log_destroy(kernel_configYLog->log);
	config_destroy(kernel_configYLog->config);
	free(kernel_configYLog);

}

#endif /* KERNEL_CONFIGURACIONES_H_ */
