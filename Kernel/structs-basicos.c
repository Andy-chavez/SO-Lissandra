/*
 * structs-basicos.c
 *
 *  Created on: 25 abr. 2019
 *      Author: utnso
 */

#include "structs-basicos.h"


void liberarConfigYLogs(configYLogs *archivos) {
	log_destroy(archivos->logger);
	config_destroy(archivos->config);
	free(archivos);
}
