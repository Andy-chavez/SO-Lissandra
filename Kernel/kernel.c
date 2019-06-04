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
	pthread_t threadRoundRobin;

	kernel_inicializar(pathConfig);

	pthread_create(&threadConsola, NULL,(void*)kernel_consola, NULL);
	pthread_create(&threadNew_Ready, NULL,(void*) kernel_pasar_a_ready, NULL);
	pthread_create(&threadRoundRobin, NULL,(void*) kernel_planificador, NULL);
	pthread_join(threadConsola, NULL);
	pthread_join(threadNew_Ready,NULL);
	pthread_join(threadRoundRobin,NULL);

	//TODO frees de las colas, destroy de semaforos
	liberarConfigYLogs();
	return EXIT_SUCCESS;
}
