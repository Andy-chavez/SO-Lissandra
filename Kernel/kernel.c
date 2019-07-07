/*
 ============================================================================
 Name        : Kernel.c
 Author      : whyAreYouRunning?
 Description :
 ============================================================================
 */
#include <stdio.h>
#include <stdlib.h>
#include <commons/config.h>
#include <pthread.h>
#include <string.h>
#include "ker_operaciones.h"
#include <signal.h>
/* INSERT TABLA1 515 "malesal" * SELECT TABLA1 515 * DESCRIBE TABLA1
 * DESCRIBE * CREATE TABLA1 SC 7 1000 * DROP TABLA1 * METRICS * JOURNAL * ADD MEMORY 5 TO EC
 */
int main(int argc, char *argv[]){
	pthread_t threadConsola;
	pthread_t threadNew_Ready;
	pthread_t threadInotify;
	pthread_t threadDescribe;
	pthread_t threadGossip;
	pthread_t threadMetrics;
//	if(argc==1){
//		printf("Pruebe ingresando el path del archivo de configuracion como parametro del kernel ejecutable.\n");
//		return EXIT_FAILURE;
//	}
	pathConfig = "/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/Kernel/KER_CONFIG";
//	(char*) argv[1];
	kernel_inicializarVariablesYListas();
	if(kernel_inicializarMemoria()==-1){
		printf("No memory to initialize\n");
		return EXIT_FAILURE;
	}
	kernel_inicializarEstructuras();
	pthread_create(&threadDescribe, NULL,(void*)describeTimeado, NULL);
	pthread_create(&threadGossip, NULL,(void*)kernel_gossiping, NULL);
	pthread_create(&threadMetrics, NULL,(void*)metrics, NULL);
	pthread_create(&threadInotify, NULL,(void*)cambiosConfig, NULL);
	pthread_create(&threadNew_Ready, NULL,(void*) kernel_pasar_a_ready, NULL);
	pthread_create(&threadConsola, NULL,(void*)kernel_consola, NULL);
	for(int i = 0; i<multiprocesamiento;i++){
		crearThreadRR(i);
	}
	pthread_join(threadConsola, NULL);
	pthread_join(threadNew_Ready,NULL);
	pthread_join(threadDescribe,NULL);
	pthread_join(threadMetrics, NULL);
	pthread_join(threadGossip, NULL);
	for(int i = 0; i<multiprocesamiento;i++){
		joinThreadRR();
	}
//	struct sigaction terminar;
//	terminar.sa_handler = kernel_semFinalizar;
//	sigemptyset(&terminar.sa_mask);
//	terminar.sa_flags = SA_RESTART;
//	sigaction(SIGINT, &terminar, NULL);

	sem_wait(&finalizar);
	kernel_finalizar();

	return EXIT_SUCCESS;
}
