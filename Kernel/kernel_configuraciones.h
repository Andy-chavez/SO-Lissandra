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
#include <commonsPropias/serializacion.h>
#include <commonsPropias/conexiones.h>

void kernel_inicializarSemaforos();
void kernel_crearListas();
void liberarConfigYLogs();
void kernel_inicializar();
void kernel_finalizar();
int kernel_inicializarMemoria();
// _________________________________________.: LLENAR/VACIAR VARIABLES GLOBALES :.____________________________________________
//-----------------INICIALIZAR KERNEL-----------------------------
void kernel_inicializarSemaforos(){
	pthread_mutex_init(&colaNuevos, NULL);
	pthread_mutex_init(&colaListos, NULL);
	pthread_mutex_init(&colaTerminados, NULL);
	pthread_mutex_init(&mLog, NULL);
	sem_init(&hayNew,0,0);
	sem_init(&hayReady,0,0);
}
void kernel_crearListas(){
	cola_proc_nuevos = list_create();
	cola_proc_listos = list_create();
	cola_proc_terminados = list_create();
	cola_proc_ejecutando = list_create();
	criterios[HASH].unCriterio = SH;
	criterios[HASH].memorias = list_create();
	criterios[STRONG].unCriterio = SC;
	criterios[STRONG].memorias = list_create();
	criterios[EVENTUAL].unCriterio = EC;
	criterios[EVENTUAL].memorias = list_create();
	//memorias = list_create();
	tablas = list_create();
	conexionesMemoria = list_create();
}
int kernel_inicializarMemoria(){ //TODO conectar a memoria y tener lista de conexiones hechas
	int socketClienteKernel = crearSocketCliente(ipMemoria,puertoMemoria);
	if(socketClienteKernel==-1){
		return -1;
	}
	serializarYEnviarHandshake(socketClienteKernel,0);
	//int recibido=  //todo devuelve pool de memorias
	memoria* conex = malloc(sizeof(memoria));
	conex->socket = socketClienteKernel;
	conex->ip = ipMemoria;
	conex->numero = (int) recibir(socketClienteKernel);
	conex->puerto = puertoMemoria;
	list_add(conexionesMemoria, conex);
	return 0;
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
	kernel_inicializarMemoria();
}
//-----------------FINALIZAR KERNEL-----------------------------
void liberarConfigYLogs() {
	log_destroy(kernel_configYLog->log);
	config_destroy(kernel_configYLog->config);
	free(kernel_configYLog);
}
void destruirSemaforos(){
	sem_destroy(&hayNew);
	sem_destroy(&hayReady);
	pthread_mutex_destroy(&colaNuevos);
	pthread_mutex_destroy(&colaListos);
	pthread_mutex_destroy(&colaTerminados);
	pthread_mutex_destroy(&mLog);
}
void liberarColas(pcb* element){
	free(element->operacion);
	free(element);
}
void liberarPCB(pcb* elemento) {
	void liberarInstrucciones(instruccion* listaIns) {
		free(listaIns->operacion);
		free(listaIns);
	}
	//list_destroy_and_destroy_elements(elemento->instruccion,(void*) liberarInstrucciones);
	free(elemento->operacion);
	free(elemento);
}
void liberarListas(){
	 list_destroy_and_destroy_elements(cola_proc_nuevos,free);
	 list_destroy_and_destroy_elements(cola_proc_listos,(void*) liberarPCB);
	 list_destroy_and_destroy_elements(cola_proc_terminados,(void*) liberarPCB);
}
void kernel_finalizar(){
	liberarConfigYLogs();
	liberarListas();
	destruirSemaforos();
}

#endif /* KERNEL_CONFIGURACIONES_H_ */
