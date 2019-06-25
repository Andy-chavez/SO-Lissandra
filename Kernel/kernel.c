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
#include <commons/config.h>
#include <pthread.h>
#include <string.h>
#include "ker_operaciones.h"
#include <signal.h>

/* EJEMPLOS:
 * INSERT TABLA1 515 malesal
 * SELECT TABLA1 515
 * DESCRIBE TABLA1
 * DESCRIBE
 * CREATE TABLA1 SC 7 1000
 * DROP TABLA1
 * METRICS
 * JOURNAL
 * ADD MEMORY 5 TO EC
 */

int main(int argc, char *argv[]){

	pthread_t threadConsola;
	pthread_t threadNew_Ready;
	pthread_t threadInotify;
//	if(argc==1){
//		printf("Pruebe ingresando el path del archivo de configuracion como parametro del kernel ejecutable.\n");
//		return EXIT_FAILURE;
//	}

//  todo cambiar memorias eventual para que haga while(socket!=-1 and list_size(lista) con semaforo, sacar funcion de stack
	pathConfig = "/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/Kernel/KER_CONFIG";
			//(char*) argv[1];
	kernel_inicializarVariables();
	if(kernel_inicializarMemoria()==-1){
		printf("No memory to initialize");
		return EXIT_FAILURE;
	}
	kernel_inicializarEstructuras();
	pthread_create(&threadConsola, NULL,(void*)kernel_consola, NULL);
	pthread_create(&threadInotify, NULL,(void*)cambiosConfig, NULL);
	pthread_create(&threadNew_Ready, NULL,(void*) kernel_pasar_a_ready, NULL);
	//for(int i = 0; i<multiprocesamiento;i++){
		pthread_t i; //todo armar lista  que adentro del for se vaya creando haciendo sus respectivos mallocs
		pthread_create(&i, NULL,(void*) kernel_roundRobin, (void*)1);
//		pthread_t i2;
//		pthread_create(&i2, NULL,(void*) kernel_roundRobin, (void*)2);
	//}
	pthread_join(threadConsola, NULL);
	pthread_join(threadNew_Ready,NULL);
	for(int i = 0; i<multiprocesamiento;i++){
		pthread_join(i,NULL);
	}
	struct sigaction terminar;
	terminar.sa_handler = kernel_semFinalizar;
	sigemptyset(&terminar.sa_mask);
	terminar.sa_flags = SA_RESTART;
	sigaction(SIGINT, &terminar, NULL);

	sem_wait(&finalizar);

	kernel_finalizar();
	return EXIT_SUCCESS;
}
