/*
 * configuraciones.h
 *
 *  Created on: 26 may. 2019
 *      Author: utnso
 */

#ifndef KERNEL_CONFIGURACIONES_H_
#define KERNEL_CONFIGURACIONES_H_
#include <commons/config.h>
#include "kernel_structs-basicos.h"
// _________________________________________.: VARIABLES GLOBALES :.____________________________________________
char * pathConfig ="/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/Kernel/KERNEL_CONFIG_EJEMPLO";
char* ipMemoria;
char* puertoMemoria;
configYLogs *kernel_configYLog;

void liberarConfigYLogs();
void kernel_obtener_configuraciones(char*);

// _________________________________________.: LLENAR/VACIAR VARIABLES GLOBALES :.____________________________________________
void kernel_obtener_configuraciones(char* path){ //TODO agregar quantum
	kernel_configYLog= malloc(sizeof(configYLogs));
	kernel_configYLog->config = config_create(path);
	kernel_configYLog->log = log_create("KERNEL.log", "KERNEL", 1, LOG_LEVEL_INFO);
	ipMemoria = config_get_string_value(kernel_configYLog->config ,"IP_MEMORIA");
	puertoMemoria = config_get_string_value(kernel_configYLog->config,"PUERTO_MEMORIA");
}
void liberarConfigYLogs() {
	free(kernel_configYLog->config);
	free(kernel_configYLog->log);
	log_destroy(kernel_configYLog->log);
	config_destroy(kernel_configYLog->config);
	free(ipMemoria);
	free(puertoMemoria);
	free(kernel_configYLog);
}

#endif /* KERNEL_CONFIGURACIONES_H_ */
