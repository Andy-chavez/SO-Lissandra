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

#include "ker_operaciones.h"

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
	pthread_t threadInotify;
	if(argc==1){
		printf("Pruebe ingresando el path del archivo de configuracion como parametro del kernel ejecutable.\n");
		return EXIT_FAILURE;
	}
	pathConfig = (char*) argv[1];
	kernel_inicializarVariables();
	if(kernel_inicializarMemoria()==-1)
		return EXIT_FAILURE;
	kernel_inicializarEstructuras();
	pthread_create(&threadConsola, NULL,(void*)kernel_consola, NULL);
	pthread_create(&threadInotify, NULL,(void*)cambiosConfig, NULL);
	pthread_create(&threadNew_Ready, NULL,(void*) kernel_pasar_a_ready, NULL);
	for(int i = 0; i<multiprocesamiento;i++){
		pthread_t i;
		pthread_create(&i, NULL,(void*) kernel_roundRobin, (void*)i);
	}
	pthread_join(threadConsola, NULL);
	pthread_join(threadNew_Ready,NULL);
	for(int i = 0; i<multiprocesamiento;i++){
		pthread_join(i,NULL);
	}
	kernel_finalizar();
	return EXIT_SUCCESS;
}
