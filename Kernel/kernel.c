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

#define CANTIDADCRITERIOS 2 //0-2
#define STRONG 0
#define HASH 1
#define EVENTUAL 2


criterio *inicializarCriterios();

criterio *inicializarCriterios(){
	criterio *datos = malloc(3*sizeof(criterio));
	datos[STRONG].unCriterio = SC; //Strong
	datos[HASH].unCriterio = SH; //Hash
	datos[EVENTUAL].unCriterio = EC; //Eventual
	for(int iter=0; iter <= CANTIDADCRITERIOS; iter++){
		datos[iter].memoriasAsociadas = malloc(sizeof(int));
		printf("Criterio: %d \n Memoria: %d \n",iter, *(datos[iter].memoriasAsociadas) );
	}
	return datos;
}

int main(int argc, char *argv[]){
//	criterio *criterios;
//	criterios = inicializarCriterios();
/*
 * pthread_t threadCliente;
	pthread_create(&threadCliente, NULL,kernel_cliente, (void *)archivosDeConfigYLog);
	pthread_join(threadCliente, NULL);
*/
	//kernel_configYLog->config = config_create("../KERNEL_CONFIG_EJEMPLO");//A modificar esto dependiendo del config que se quiera usar
	//kernel_configYLog->log = log_create("KERNEL.log", "KERNEL", 1, LOG_LEVEL_INFO);
	cola_proc_nuevos = list_create();
	kernel_consola();
//TODO frees de las colas
	//liberarConfigYLogs(kernel_configYLog);
	return EXIT_SUCCESS;
}
