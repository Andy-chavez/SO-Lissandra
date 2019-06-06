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
void kernel_roundRobin();
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
// _____________________________.: OPERACIONES DE API PARA LAS CUALES SELECCIONAR MEMORIA SEGUN CRITERIO:.____________________________________________
void kernel_insert(char* operacion){ //ya funciona, ver lo de seleccionar la memoria a la cual mandarle esto
	operacionLQL* opAux=splitear_operacion(operacion);
	int socketClienteKernel = crearSocketCliente(ipMemoria,puertoMemoria);
	serializarYEnviarOperacionLQL(socketClienteKernel, opAux);
	printf("\n\nEnviado\n\n");
	char * recibido= (char*) recibir(socketClienteKernel);
	printf("\n\nValor recibido:%s\n\n",recibido);
	cerrarConexion(socketClienteKernel);
	free(recibido);
	free(opAux->operacion);
	free(opAux->parametros);
	free(opAux);
}
void kernel_select(char* operacion){
	operacionLQL* opAux=splitear_operacion(operacion);
	int socketClienteKernel = crearSocketCliente(ipMemoria,puertoMemoria);
	serializarYEnviarOperacionLQL(socketClienteKernel, opAux);
	printf("\n\nEnviado\n\n");
	char * recibido= (char*) recibir(socketClienteKernel);
	printf("\n\nValor recibido:%s\n\n",recibido);
	cerrarConexion(socketClienteKernel);
	free(recibido);
	free(opAux->operacion);
	free(opAux->parametros);
	free(opAux);
}
void kernel_create(char* operacion){
	operacionLQL* opAux=splitear_operacion(operacion);
	int socketClienteKernel = crearSocketCliente(ipMemoria,puertoMemoria);
	serializarYEnviarOperacionLQL(socketClienteKernel, opAux);
	printf("\n\nEnviado\n\n");
	char * recibido= (char*) recibir(socketClienteKernel); //todo cambiar recibido
	printf("\n\nValor recibido:%s\n\n",recibido);
	cerrarConexion(socketClienteKernel);
	free(recibido);
	free(opAux->operacion);
	free(opAux->parametros);
	free(opAux);
}
void kernel_drop(char* operacion){
	operacionLQL* opAux=splitear_operacion(operacion);
	int socketClienteKernel = crearSocketCliente(ipMemoria,puertoMemoria);
	serializarYEnviarOperacionLQL(socketClienteKernel, opAux);
	printf("\n\nEnviado\n\n");
	char * recibido= (char*) recibir(socketClienteKernel); //todo cambiar recibido
	printf("\n\nValor recibido:%s\n\n",recibido);
	cerrarConexion(socketClienteKernel);
	free(recibido);
	free(opAux->operacion);
	free(opAux->parametros);
	free(opAux);
}
// _____________________________.: OPERACIONES DE API DIRECTAS:.____________________________________________
void kernel_journal(){
	operacionLQL* opAux=splitear_operacion("JOURNAL");
	int socketClienteKernel = crearSocketCliente(ipMemoria,puertoMemoria);
	serializarYEnviarOperacionLQL(socketClienteKernel, opAux);
	printf("\n\nEnviado\n\n");
	char * recibido= (char*) recibir(socketClienteKernel); //todo cambiar recibido
	printf("\n\nValor recibido:%s\n\n",recibido);
	cerrarConexion(socketClienteKernel);
	free(recibido);
	free(opAux->operacion);
	free(opAux->parametros);
	free(opAux);
}
void kernel_metrics(){ //todo dps
	printf("Not yet");
}
void kernel_add(){

}
// _________________________________________.: PROCEDIMIENTOS INTERNOS :.____________________________________________
instruccion* obtener_ultima_instruccion(t_list* instruc){
	int size = list_size(instruc);
	instruccion *instrucAux;//= malloc(sizeof(instruccion));
	instrucAux = list_get(instruc,size);
	return instrucAux;
}

bool instruccion_no_ejecutada(instruccion* instruc){
	return instruc->ejecutado==0;
}
// ---------------.: THREAD ROUND ROBIN :.---------------
void kernel_roundRobin(){
	sem_wait(&hayReady);
	//while(!list_is_empty(cola_proc_listos)){ //TODO agregar semaforo cuando para avisar que hay procesos en listo
		//TODO poner semaforo
	//while(1){
	pcb* pcb_auxiliar;// = malloc(sizeof(pcb));
	pthread_mutex_lock(&colaListos);
	pcb_auxiliar = (pcb*) list_remove(cola_proc_listos,0);
	pthread_mutex_unlock(&colaListos);
	//printf("%s\n", pcb_auxiliar->operacion);
	if(pcb_auxiliar->instruccion == NULL){
			if(pcb_auxiliar->ejecutado ==1){
				pthread_mutex_lock(&colaTerminados);
				list_add(cola_proc_terminados,pcb_auxiliar);
				pthread_mutex_unlock(&colaTerminados);
				//free(pcb_auxiliar);
			}
			else{
				pcb_auxiliar->ejecutado=1;
				kernel_api(pcb_auxiliar->operacion);
				pthread_mutex_lock(&colaTerminados);
				list_add(cola_proc_terminados,pcb_auxiliar);
				pthread_mutex_unlock(&colaTerminados);
				//free(pcb_auxiliar);
			}
		}
		else if(pcb_auxiliar->instruccion !=NULL){
			for(int quantum=0;quantum<quantumMax;quantum++){
				if(pcb_auxiliar->ejecutado ==0){
					kernel_api(pcb_auxiliar->operacion);
					pcb_auxiliar->ejecutado=1;
				}
				instruccion* instruc=malloc(sizeof(instruccion));
				instruc= list_find(pcb_auxiliar->instruccion,(void*)instruccion_no_ejecutada);
				instruc->ejecutado = 1;
				kernel_api(instruc->operacion);
			}
			if(list_any_satisfy(pcb_auxiliar->instruccion,(void*)instruccion_no_ejecutada)){
				pthread_mutex_lock(&colaListos);
				list_add(cola_proc_listos, pcb_auxiliar);
				pthread_mutex_unlock(&colaListos);
			}
			else{
				pthread_mutex_lock(&colaTerminados);
				list_add(cola_proc_terminados, pcb_auxiliar);
				pthread_mutex_unlock(&colaTerminados);
			}
		}
	//}
//		free(pcb_auxiliar->operacion);
//		free(pcb_auxiliar);
}
void kernel_crearPCB(char* operacion){
	pcb* pcb_auxiliar = malloc(sizeof(pcb));
	pcb_auxiliar->operacion = operacion;
	pcb_auxiliar->ejecutado = 0;
	pcb_auxiliar->instruccion = NULL;
	pthread_mutex_lock(&colaNuevos);
	list_add(cola_proc_listos,pcb_auxiliar);
	pthread_mutex_unlock(&colaNuevos);
	sem_post(&hayReady);
}
void kernel_pasar_a_ready(){
	sem_wait(&hayNew);
	pthread_mutex_lock(&colaNuevos);
	char* operacion = NULL;
	operacion =(char*) list_remove(cola_proc_nuevos,0);
	pthread_mutex_unlock(&colaNuevos);

	string_to_upper(operacion);
	if (string_contains( operacion, "RUN")) {
		kernel_run(operacion);
	}
	else if(string_contains(operacion, "SELECT") || string_contains(operacion, "INSERT") || string_contains(operacion, "CREATE") ||
			 string_contains(operacion, "DESCRIBE") || string_contains(operacion, "DROP") ||
			 string_contains(operacion, "JOURNAL") || string_contains(operacion, "METRICS") || string_contains(operacion, "ADD")){ //splitear y comparar
		kernel_crearPCB(operacion);
	}
	else{
		log_error(kernel_configYLog->log,"Sintaxis incorrecta: %s\n", operacion);
	}
}
// ---------------.: THREAD CONSOLA A NEW :.---------------
void kernel_almacenar_en_new(char*operacion){

	pthread_mutex_lock(&colaNuevos);
	list_add(cola_proc_nuevos, operacion);
	pthread_mutex_unlock(&colaNuevos);
	sem_post(&hayNew);
	//TODO loggear que operacion se agrego a new
}

void kernel_consola(){
	printf("Por favor ingrese <OPERACION> seguido de los argumentos\n\n");
	char* linea= NULL;
	//while((linea = readline(""))!= NULL){
	 //TODO agregar while para leer de consola
		linea = readline("");
		kernel_almacenar_en_new(linea);
	//}
}
// ---------------.: THREAD NEW A READY :.---------------
void kernel_run(char* operacion){
	char** opYArg;
	opYArg = string_n_split(operacion ,2," ");
	string_to_lower(*(opYArg+1));
	FILE *archivoALeer;
	if ((archivoALeer= fopen((*(opYArg+1)), "r")) == NULL){
		log_error(kernel_configYLog->log,"No se pudo ejecutar comando: %s %s, verifique existencia del archivo\n", *opYArg, *(opYArg+1) ); //operacion);
		free(*(opYArg+1));
		free(*(opYArg));
		free(opYArg);
		exit(EXIT_FAILURE);
	}
	char *lineaLeida;
	size_t limite = 250;
	ssize_t leer;
	lineaLeida = NULL;
	pcb* pcb_auxiliar = malloc(sizeof(pcb));
	//pcb_auxiliar->operacion= malloc(sizeof(operacion));
	pcb_auxiliar->operacion = operacion;
	pcb_auxiliar->ejecutado = 1 ;
	pcb_auxiliar->instruccion =list_create();

	while((leer = getline(&lineaLeida, &limite, archivoALeer)) != -1){
		instruccion* instruccion_auxiliar = malloc(sizeof(instruccion));
		instruccion_auxiliar->ejecutado= 0;
		//instruccion_auxiliar->operacion= malloc(sizeof(lineaLeida));
		instruccion_auxiliar->operacion= lineaLeida;
		list_add(pcb_auxiliar->instruccion,instruccion_auxiliar);
		//free(instruccion_auxiliar->operacion);
		//free(instruccion_auxiliar);
	}
	pthread_mutex_lock(&colaNuevos);
	list_add(cola_proc_listos,pcb_auxiliar);
	pthread_mutex_unlock(&colaNuevos);
	free(lineaLeida);
//	free(pcb_auxiliar->operacion);
//	free(pcb_auxiliar);
	free(*(opYArg+1));
	free(*(opYArg));
	free(opYArg);
	fclose(archivoALeer);
	sem_post(&hayReady);
}
void kernel_api(char* operacionAParsear) //cuando ya esta en el rr
{
	printf("%s\n\n", operacionAParsear);
	if(string_contains(operacionAParsear, "INSERT")) {
		kernel_insert(operacionAParsear);
	}
	else if (string_contains(operacionAParsear, "SELECT")) {
			kernel_select(operacionAParsear);
	}
	else if (string_contains(operacionAParsear, "DESCRIBE")) {
		printf("DESCRIBE\n");
//TODO			kernel_describe();
	}
	else if (string_contains(operacionAParsear, "CREATE")) {
		printf("CREATE\n");
//TODO			kernel_create();
	}
	else if (string_contains(operacionAParsear, "DROP")) {
		printf("DROP\n");
//TODO			kernel_drop();
	}
	else if (string_contains(operacionAParsear, "JOURNAL")) {
		printf("JOURNAL\n");
//TODO			kernel_journal();
	}
	else if (string_contains(operacionAParsear, "RUN")) {
		printf("Ha utilizado el comando RUN, su archivo comenzará a ser ejecutado\n");
//TODO			kernel_run(*argumentos);
	}
	else if (string_contains(operacionAParsear, "METRICS")) {
		printf("METRICS\n");
//TODO			kernel_metrics();
	}
	else if (string_contains(operacionAParsear, "ADD")) {
		printf("ADD\n");
//TODO			kernel_add();
	}
	else {
		printf("Mi no entender esa operacion\n");
		}
}

#endif /* KERNEL_OPERACIONES_H_ */
