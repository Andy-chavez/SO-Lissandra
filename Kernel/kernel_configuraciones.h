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
#include <commonsPropias/conexiones.h>
#include <commonsPropias/serializacion.h>
#include <sys/types.h>
#include <sys/inotify.h>

#define EVENT_SIZE_CONFIG (sizeof(struct inotify_event) + 15)
#define BUF_LEN_CONFIG (1 * EVENT_SIZE_CONFIG)

void kernel_inicializarSemaforos();
void kernel_crearListas();
void liberarConfigYLogs();
void kernel_inicializarVariables();
void kernel_finalizar();
int kernel_inicializarMemoria();
void loggearInfoEXEC(char* estado, int threadProcesador, char* operacion);
void loggearErrorEXEC(char* estado, int threadProcesador, char* operacion);
void loggearErrorEXEC(char* estado, int threadProcesador, char* operacion);
void loggearInfoEXEC(char* estado, int threadProcesador, char* operacion);
// _________________________________________.: LLENAR/VACIAR VARIABLES GLOBALES :.____________________________________________
//-----------------INICIALIZAR KERNEL-----------------------------
void kernel_inicializarSemaforos(){
	pthread_mutex_init(&colaNuevos, NULL);
	pthread_mutex_init(&colaListos, NULL);
	pthread_mutex_init(&colaTerminados, NULL);
	pthread_mutex_init(&mLog, NULL);
	sem_init(&hayNew,0,0);
	sem_init(&hayReady,0,0);
	sem_init(&modificables,0,0);
}
void kernel_crearListas(){
	cola_proc_nuevos = list_create();
	cola_proc_listos = list_create();
	cola_proc_terminados = list_create();
	criterios[HASH].unCriterio = SH;
	criterios[HASH].memorias = list_create();
	criterios[STRONG].unCriterio = SC;
	criterios[STRONG].memorias = list_create();
	criterios[EVENTUAL].unCriterio = EC;
	criterios[EVENTUAL].memorias = list_create();
	tablas = list_create();
	conexionesMemoria = list_create();
}
int kernel_inicializarMemoria(){ //TODO conectar a memoria y tener lista de conexiones hechas
	int socketClienteKernel = crearSocketCliente(ipMemoria,puertoMemoria);
	if(socketClienteKernel==-1){
		return -1;
	}
	serializarYEnviarHandshake(socketClienteKernel,0);
	memoria* memoriaDeConfig = malloc(sizeof(memoria));
	memoriaDeConfig->socket = socketClienteKernel;
	memoriaDeConfig->ip = ipMemoria;
	memoriaDeConfig->numero = *(int*) recibir(socketClienteKernel);
	memoriaDeConfig->puerto = puertoMemoria;
	list_add(conexionesMemoria, memoriaDeConfig);
	return 0;
}
void kernel_inicializarVariables(){
	kernel_configYLog= malloc(sizeof(configYLogs));
	kernel_configYLog->config = config_create(pathConfig);
	kernel_configYLog->log = log_create("KERNEL.log", "KERNEL", 0, LOG_LEVEL_INFO);
	ipMemoria = config_get_string_value(kernel_configYLog->config ,"IP_MEMORIA");
	puertoMemoria = config_get_string_value(kernel_configYLog->config,"PUERTO_MEMORIA");
	quantumMax = config_get_int_value(kernel_configYLog->config,"QUANTUM");
	multiprocesamiento =config_get_int_value(kernel_configYLog->config,"MULTIPROCESAMIENTO");
	metadataRefresh = config_get_int_value(kernel_configYLog->config,"METADATA_REFRESH");
	sleepEjecucion = config_get_int_value(kernel_configYLog->config,"SLEEP_EJECUCION");
}
void kernel_inicializarEstructuras(){
	kernel_crearListas();
	kernel_inicializarSemaforos();
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
	sem_destroy(&modificables);
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
void liberarListas(){ //todo agregar listas je
	 list_destroy_and_destroy_elements(cola_proc_nuevos,free);
	 list_destroy_and_destroy_elements(cola_proc_listos,(void*) liberarPCB);
	 list_destroy_and_destroy_elements(cola_proc_terminados,(void*) liberarPCB);
}
void kernel_finalizar(){
	liberarConfigYLogs();
	liberarListas();
	destruirSemaforos();
}
//----------------- CAMBIOS EN EJECUCION -----------------------------
void* cambiosConfig(){
	char buffer[BUF_LEN_CONFIG];
	int fdConfig = inotify_init();
	char* path = pathConfig;

 	if(fdConfig < 0) {
		pthread_mutex_lock(&mLog);
		log_error(kernel_configYLog->log,"Hubo un error con el inotify_init");
		pthread_mutex_unlock(&mLog);
	}

 	int watchDescriptorConfig = inotify_add_watch(fdConfig, path, IN_MODIFY);

 	while(1) {
		int size = read(fdConfig, buffer, BUF_LEN_CONFIG);

 		if(size<0) {
			pthread_mutex_lock(&mLog);
			log_error(kernel_configYLog->log,"Hubo un error al leer modificaciones del config");
			pthread_mutex_unlock(&mLog);
		}

 		t_config* configConNuevosDatos = config_create(path);

 		if(!configConNuevosDatos) {
			pthread_mutex_lock(&mLog);
			log_error(kernel_configYLog->log,"Hubo un error al abrir el archivo de config");
			pthread_mutex_unlock(&mLog);
		}

 		int desplazamiento = 0;

 		while(desplazamiento < size) {
			struct inotify_event *event = (struct inotify_event *) &buffer[desplazamiento];

 			if (event->mask & IN_MODIFY) {
 				pthread_mutex_lock(&mLog);
				log_info(kernel_configYLog->log,"Hubieron cambios en el archivo de config. Analizando y realizando cambios a retardos...");
				pthread_mutex_unlock(&mLog);
 				sem_wait(&modificables);
 				quantumMax = config_get_int_value(configConNuevosDatos, "RETARDO_GOSSIPING");
 				metadataRefresh = config_get_int_value(configConNuevosDatos, "METADATA_REFRESH");
 				sleepEjecucion = config_get_int_value(configConNuevosDatos, "SLEEP_EJECUCION");
				sem_post(&modificables);

			}

 			config_destroy(configConNuevosDatos);
			desplazamiento += sizeof (struct inotify_event) + event->len;
		}
	}
}
//----------------- LOGS -----------------------------
void loggearErrorYLiberarParametrosEXEC(char* recibido, operacionLQL *opAux){
	pthread_mutex_lock(&mLog);
	log_error(kernel_configYLog->log, "RECIBIDO: %s", recibido);
	pthread_mutex_unlock(&mLog);
	free(recibido);
	liberarOperacionLQL(opAux);
}
void loggearInfoYLiberarParametrosEXEC(char* recibido, operacionLQL *opAux){
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log, "RECIBIDO: %s", recibido);
	pthread_mutex_unlock(&mLog);
	free(recibido);
	liberarOperacionLQL(opAux);
}
void loggearErrorEXEC(char* estado, int threadProcesador, char* operacion){
	pthread_mutex_lock(&mLog);
	log_error(kernel_configYLog->log,"%s[%d]: %s",estado,threadProcesador, operacion);
	pthread_mutex_unlock(&mLog);
}
void loggearInfoEXEC(char* estado, int threadProcesador, char* operacion){
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log,"%s[%d]: %s",estado,threadProcesador, operacion);
	pthread_mutex_unlock(&mLog);
}
#endif /* KERNEL_CONFIGURACIONES_H_ */
