/*
 ============================================================================
 Name        : Kernel.c
 Author      : whyAreYouRunning?
 Version     :
 Copyright   : Your copyright notice
 Description :
 ============================================================================
 */
#include <stdio.h>
#include <stdlib.h>
#include <commonsPropias/conexiones.h>
#include <commons/config.h>
#include <pthread.h>
#include <string.h>
#include "kernel_operaciones.h"

/*
 * EJEMPLOS:
 * INSERT TABLA1 515 malesal
 * SELECT TABLA1 515
 * DESCRIBE TABLA1
 * CREATE TABLA1 SC 7 1000
 * DROP TABLA1
 * METRICS
 * JOURNAL
 * ADD MEMORY 5 TO EC
 *
 */

int main(int argc, char *argv[]){

	pthread_t threadConsola;
	pthread_t threadNew_Ready;
//	pthread_t threadPlanificador;
/*	pthread_create(&threadCliente, NULL,kernel_cliente, (void *)archivosDeConfigYLog);
	pthread_join(threadCliente, NULL);
*/
	//kernel_configYLog->log = log_create("KERNEL.log", "KERNEL", 1, LOG_LEVEL_INFO);
	//cola_proc_nuevos = list_create();
	//kernel_inicializar(pathConfig);
	kernel_crearListas();
	kernel_inicializarSemaforos();
	//sem_init(hayNew,0,0);
	pthread_create(&threadConsola, NULL,(void*)kernel_consola, NULL);
	pthread_join(threadConsola, NULL);
	pthread_create(&threadNew_Ready, NULL,(void*) kernel_pasar_a_ready, NULL);
	pthread_join(threadNew_Ready,NULL);
	//sem_destroy(hayNew);
//	pthread_create(&threadPlanificador, NULL,(void*) kernel_consola, NULL);
//	pthread_join(threadPlanificador, NULL);
	//kernel_consola();
//TODO frees de las colas
	//liberarConfigYLogs(kernel_configYLog);
	return EXIT_SUCCESS;
}
