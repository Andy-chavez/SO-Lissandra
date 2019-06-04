/*
 * kernel_operaciones.h
 *
 *  Created on: 16 may. 2019
 *      Author: utnso
 */

#ifndef KERNEL_OPERACIONES_H_
#define KERNEL_OPERACIONES_H_
#include <commons/string.h>
#include <commonsPropias/serializacion.h>
#include <stdbool.h>
#include <readline/readline.h>
#include <readline/history.h>

#include "kernel_configuraciones.h"
#include "kernel_structs-basicos.h"

/******************************DECLARACIONES******************************************/
void kernel_almacenar_en_cola(char*,char*);
void kernel_agregar_cola_proc_nuevos(char*);
void kernel_run(char*);
void kernel_consola();
void kernel_api(char*);

/* TODO implementaciones
 * los primeros 5 pasarlos a la memoria elegida por el criterio de la tabla
 * run -> ya esta hecho
 * metrics -> variables globales con semaforos
 * journal -> pasarselo a memoria
 * add -> agregar memoria a un criterio @MEMORIA tengo que avisarles?
 *
 */
void kernel_planificador();
void kernel_insert();
void kernel_select();
void kernel_describe();
void kernel_create();
void kernel_drop();
void kernel_journal();
void kernel_metrics();
void kernel_add();

void* kernel_cliente(void *archivo);

/******************************IMPLEMENTACIONES******************************************/
// _________________________________________.: OPERACIONES DE API :.____________________________________________
void kernel_insert(char* operacion){ //ya funciona, ver lo de seleccionar la memoria a la cual mandarle esto
	operacionLQL* opAux=splitear_operacion(operacion);
	//void * aEnviar = serializarOperacionLQL(opAux);
	int socketClienteKernel = crearSocketCliente(ipMemoria,puertoMemoria);
	//enviar(socketClienteKernel, aEnviar, 39);
	printf("\n\nEnviado\n\n");
	cerrarConexion(socketClienteKernel);
	free(opAux->operacion);
	free(opAux->parametros);
	free(opAux);
}
void kernel_select(char* operacion){
	operacionLQL* opAux=splitear_operacion(operacion);
	//void * aEnviar = serializarOperacionLQL(opAux);
	int socketClienteKernel = crearSocketCliente(ipMemoria,puertoMemoria);
	//enviar(socketClienteKernel, aEnviar, 31);
	printf("\nEnviado\n");
	char* recibido = (char*)recibir(socketClienteKernel);
	printf("\nEl valor recibido es: %s\n",recibido);
	//recibir valor
	//cerrarConexion(socketClienteKernel);
	free(opAux->operacion);
	free(opAux->parametros);
	free(opAux);
}
void kernel_create(char* operacion){
	operacionLQL* opAux=splitear_operacion(operacion);
	//void * aEnviar = serializarOperacionLQL(opAux);
	int socketClienteKernel = crearSocketCliente(ipMemoria,puertoMemoria);
	//enviar(socketClienteKernel, aEnviar, 39);
	printf("\n\nEnviado\n\n");
	//recibir valor
	//cerrarConexion(socketClienteKernel);
	free(opAux->operacion);
	free(opAux->parametros);
	free(opAux);
}
// _________________________________________.: PROCEDIMIENTOS INTERNOS :.____________________________________________
instruccion* obtener_ultima_instruccion(t_list* instruc){
	int size = list_size(instruc);
	instruccion *instrucAux = malloc(sizeof(instruccion));
	instrucAux = list_get(instruc,size);
	return instrucAux;
}

bool instruccion_no_ejecutada(instruccion* instruc){
	return instruc->ejecutado==0;
}
void kernel_planificador(){
	while(!list_is_empty(cola_proc_listos)){ //TODO agregar semaforo cuando para avisar que hay procesos en listo
		//TODO poner semaforo
		pcb* pcb_auxiliar = malloc(sizeof(pcb));
		//pcb_auxiliar->instruccion =list_create();
		pthread_mutex_lock(&colaListos);
		pcb_auxiliar = (pcb*) list_remove(cola_proc_listos,1);
		pthread_mutex_unlock(&colaListos);
		if( pcb_auxiliar->instruccion == NULL){
			if(pcb_auxiliar->ejecutado ==1){
				pthread_mutex_lock(&colaTerminados);
				list_add(cola_proc_terminados,pcb_auxiliar);
				pthread_mutex_unlock(&colaTerminados);
				free(pcb_auxiliar);
			}
			else{
				pcb_auxiliar->ejecutado=1;
				kernel_api(pcb_auxiliar->operacion);
				pthread_mutex_lock(&colaTerminados);
				list_add(cola_proc_terminados,pcb_auxiliar);
				pthread_mutex_unlock(&colaTerminados);
				free(pcb_auxiliar);
			}
		}
		else if(pcb_auxiliar->instruccion !=NULL){
			for(int quantum=0;quantum++;quantum<quantumMax){
				if(pcb_auxiliar->ejecutado ==0){
					kernel_api(pcb_auxiliar->operacion);
					pcb_auxiliar->ejecutado=1;
				}
				instruccion* instruc=malloc(sizeof(instruccion));
				instruc= list_find(pcb_auxiliar->instruccion,(void*)instruccion_no_ejecutada);
				instruc->ejecutado = 1;
				kernel_api(instruc->operacion);
			}
			if(!instruccion_no_ejecutada(obtener_ultima_instruccion(pcb_auxiliar->instruccion))){
				pthread_mutex_lock(&colaTerminados);
				list_add(cola_proc_terminados, pcb_auxiliar);
				pthread_mutex_unlock(&colaTerminados);

			}
			else{
				pthread_mutex_lock(&colaListos);
				list_add(cola_proc_listos, pcb_auxiliar);
				pthread_mutex_unlock(&colaListos);
			}
		}
		free(pcb_auxiliar);
	}
}
void kernel_crearPCB(char* operacion){
	pcb* pcb_auxiliar = malloc(sizeof(pcb));
	pcb_auxiliar->operacion= malloc(sizeof(*operacion));
	pcb_auxiliar->operacion = operacion;
	pcb_auxiliar->ejecutado = 0;
	pthread_mutex_lock(&colaNuevos);
	list_add(cola_proc_nuevos,pcb_auxiliar);
	pthread_mutex_unlock(&colaNuevos);
	free(pcb_auxiliar->operacion);
	free(pcb_auxiliar);

}
void kernel_pasar_a_ready(){
	sem_wait(&hayNew); //TODO sem_BINARIO
	char* operacion = NULL;
//	if (list_is_empty(cola_proc_listos)){ //TODO semaforo que solucione esto para sacarlo
//		return ;
//	}
	pthread_mutex_lock(&colaNuevos);
	operacion = (char*)list_remove(cola_proc_nuevos,1);
	pthread_mutex_unlock(&colaNuevos);
	string_to_upper(operacion);

	if (string_contains( "RUN" ,operacion)) {
		 //poner esto en log aclarando el path
		kernel_run(operacion);
	}
	else if(string_contains("SELECTINSERTCREATEDESCRIBEDROPJOURNALMETRICSADD", operacion)){
		kernel_crearPCB(operacion);
	}
	else{
		log_error(kernel_configYLog->log,"Sintaxis incorrecta: %s\n", operacion);
	}
	free(operacion);
}
void kernel_almacenar_en_new(char*operacion){

	pthread_mutex_lock(&colaNuevos);
	list_add(cola_proc_nuevos, operacion);
	pthread_mutex_unlock(&colaNuevos);
	free(operacion);
	sem_post(&hayNew);
	//TODO loggear que operacion se agrego a new
}

void kernel_consola(){
	printf("Por favor ingrese <OPERACION> seguido de los argumentos\n\n");
	char* linea= NULL;
	linea = readline(""); //TODO agregar while para leer de consola
	kernel_almacenar_en_new(linea);
	//free(linea);
}
void kernel_run(char* operacion){
	char** opYArg;
	opYArg = string_n_split(operacion ,2," ");
	FILE *archivoALeer= fopen(*(opYArg+1), "r");
	char *lineaLeida;
	size_t limite = 250;
	ssize_t leer;
	lineaLeida = (char *)malloc(limite * sizeof(char));
	if (archivoALeer == NULL){
		log_error(kernel_configYLog->log,"No se pudo ejecutar comando: %s, verifique existencia del archivo\n", operacion);
		free(lineaLeida);
		free(*(opYArg+1));
		free(*(opYArg));
		free(opYArg);
		exit(EXIT_FAILURE);
	}
	pcb* pcb_auxiliar = malloc(sizeof(pcb));
	pcb_auxiliar->operacion= malloc(sizeof(operacion));
	pcb_auxiliar->operacion = operacion;
	pcb_auxiliar->ejecutado = 1 ;
	pcb_auxiliar->instruccion =list_create();

	while((leer = getline(&lineaLeida, &limite, archivoALeer)) != -1){
		instruccion* instruccion_auxiliar = malloc(sizeof(instruccion));
		instruccion_auxiliar->ejecutado= 0;
		instruccion_auxiliar->operacion= malloc(sizeof(lineaLeida));
		instruccion_auxiliar->operacion= lineaLeida;
		list_add(pcb_auxiliar->instruccion,instruccion_auxiliar);
		free(instruccion_auxiliar->operacion);
		free(instruccion_auxiliar);
	}
	pthread_mutex_lock(&colaNuevos);
	list_add(cola_proc_nuevos,pcb_auxiliar);
	pthread_mutex_unlock(&colaNuevos);
	free(lineaLeida);
	free(pcb_auxiliar->operacion);
	free(pcb_auxiliar);
	free(*(opYArg+1));
	free(*(opYArg));
	free(opYArg);
	fclose(archivoALeer);
}
void kernel_api(char* operacionAParsear) //cuando ya esta en el rr
{
	if(string_starts_with(operacionAParsear, "INSERT")) {
		kernel_insert(operacionAParsear);
	}
	else if (string_starts_with(operacionAParsear, "SELECT")) {
			kernel_select(operacionAParsear);
	}
	else if (string_starts_with(operacionAParsear, "DESCRIBE")) {
		printf("DESCRIBE\n");
//TODO			kernel_describe();
	}
	else if (string_starts_with(operacionAParsear, "CREATE")) {
		printf("CREATE\n");
//TODO			kernel_create();
	}
	else if (string_starts_with(operacionAParsear, "DROP")) {
		printf("DROP\n");
//TODO			kernel_drop();
	}
	else if (string_starts_with(operacionAParsear, "JOURNAL")) {
		printf("JOURNAL\n");
//TODO			kernel_journal();
	}
	else if (string_starts_with(operacionAParsear, "RUN")) {
		printf("Ha utilizado el comando RUN, su archivo comenzará a ser ejecutado\n");
//TODO			kernel_run(*argumentos);
	}
	else if (string_starts_with(operacionAParsear, "METRICS")) {
		printf("METRICS\n");
//TODO			kernel_metrics();
	}
	else if (string_starts_with(operacionAParsear, "ADD")) {
		printf("ADD\n");
//TODO			kernel_add();
	}
	else {
		printf("Mi no entender esa operacion\n");
		}
}

#endif /* KERNEL_OPERACIONES_H_ */
