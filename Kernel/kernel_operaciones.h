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
int kernel_api(char*);
void guardarTablacreada(char*);
void eliminarTablaCreada(char* );
/* TODO implementaciones
 * los primeros 5 pasarlos a la memoria elegida por el criterio de la tabla
 * run -> ya esta hecho
 * metrics -> variables globales con semaforos
 * journal -> pasarselo a memoria
 * add -> agregar memoria a un criterio @MEMORIA tengo que avisarles?
 *
 */
void kernel_roundRobin();
int kernel_insert(char*);
int kernel_select(char*);
int kernel_describe(char*);
int kernel_create(char*);
int kernel_drop(char*);
int kernel_journal();
int kernel_metrics();
int kernel_add(char*);
//Corroborar sintaxis
//1.select
//2.insert
//3.create
//4.describe tabla

/******************************IMPLEMENTACIONES******************************************/
//------ TABLAS ---------
void guardarTablaCreada(char* parametros){
	char** opAux =string_n_split(parametros,3," ");
	tabla* tablaAux = malloc(sizeof(tabla));
	tablaAux->nombreDeTabla= *opAux;
	if(string_equals_ignore_case(*(opAux+1),"SC")){
		tablaAux->consistenciaDeTabla = SC;
	}
	else if(string_equals_ignore_case(*(opAux+1),"SH")){
		tablaAux->consistenciaDeTabla = SH;
	}
	else if(string_equals_ignore_case(*(opAux+1),"EC")){
		tablaAux->consistenciaDeTabla = EC;
	}
	list_add(tablas,tablaAux);
}
void eliminarTablaCreada(char* parametros){
	tabla* tablaAux = malloc(sizeof(tabla));
	bool tablaDeNombre(tabla* t){
			return t->nombreDeTabla == parametros;
		}
	tablaAux = list_remove_by_condition(tablas, (void*)tablaDeNombre);
	free(tablaAux->nombreDeTabla);
	free(tablaAux);
}
tabla* encontrarTablaPorNombre(char* nombre){
	bool tablaDeNombre(tabla* t){
			return t->nombreDeTabla == nombre;
		}
	return list_find(tablas,(void* ) tablaDeNombre);
}
//------ MEMORIAS ---------
memoria* encontrarMemoria(int numero){
	bool memoriaEsNumero(memoria* mem) {
		return mem->numero == numero;
	}
	memoria * memory = malloc(sizeof(memoria));
	memory = (memoria*) list_find(conexionesMemoria, (void*)memoriaEsNumero);
	return memory;
}
memoria* encontrarMemoriaStrong(){
//	bool memoriaRandom(memoria* mem) {
//		return mem->numero == numero;
//	}
//
//	return (memoria*) list_find(criterios[criterio].memorias, (void*)memoriaRandom);   de momento sale hardcodeo de la unica memoria que hay

//	memoria* mem = malloc(sizeof(memoria));
//	mem->ip = ipMemoria;
//	mem->puerto = puertoMemoria;
//	mem->numero = numPrueba;
//	list_add(criterios[STRONG].memorias,mem);
	return list_get(criterios[STRONG].memorias, 0);
}
//------ CRITERIOS ---------


//------ CONEXION ---------
int encontrarSocketDeMemoria(int numero){
	bool encontrarSocket(memoria* unaConex){
		return unaConex->numero == numero;
	}
	memoria* mem = list_find(conexionesMemoria,(void*) encontrarSocket);
	return mem->socket;
}

int socketMemoriaSolicitada(consistencia criterio){
	memoria* mem = NULL;
	switch (criterio){

		case SC:
			mem = encontrarMemoriaStrong();
			break;
		case SH:

			break;
		case EC:
			break;
	}

	return encontrarSocketDeMemoria(mem->numero);
}
//------ SINTAXIS CORRECTA ---------
int sintaxisCorrecta(char caso,char* parametros){
	int retorno = 0;
	switch(caso){
		case '1': //1.select
		case '2': //2.insert
//		{
//			char** parametrosSpliteados = string_split(parametros, " ");
//			if(atoi(*(parametrosSpliteados + 1)) && *(parametrosSpliteados + 1) == "0")
//				retorno = 1;
//		}
			retorno = 1;
			break;
		case '3': //3.create
			//todo verificar dps
			retorno = 1;
			break;
		case '4': //4.describe tabla
			if(encontrarTablaPorNombre(parametros))
				retorno = 1;
			break;
	}

	return retorno;
}
// _____________________________.: OPERACIONES DE API PARA LAS CUALES SELECCIONAR MEMORIA SEGUN CRITERIO:.____________________________________________
int kernel_insert(char* operacion){ //ya funciona, ver lo de seleccionar la memoria a la cual mandarle esto
	operacionLQL* opAux=splitear_operacion(operacion);
	if(sintaxisCorrecta('3',opAux->parametros)==0){
		//abortarProceso(char*operacion);
		return 0;
	}
//	memoria* mem =encontrarMemoriaStrong();
//	int socketClienteKernel = crearSocketCliente(mem->ip,mem->puerto);
	//if(socketClienteKernel){ //TODO AGREGAR IF CON STRING_CONTAINS ERROR:
		int socket = socketMemoriaSolicitada(SC); //todo verificar lo de la tabla
		serializarYEnviarOperacionLQL(socket, opAux);
		log_info(kernel_configYLog->log, "Enviado %s", operacion);
		char * recibido= (char*) recibir(socket);
		log_info(kernel_configYLog->log, "valor recibido: %s", recibido);
		//log_info(kernel_configYLog->log, "%s\n", recibido);
		//cerrarConexion(socket);
		free(recibido);
		return 1;
	//}
	free(opAux->operacion);
	free(opAux->parametros);
	free(opAux);
	return 0;
}
int kernel_select(char* operacion){
	operacionLQL* opAux=splitear_operacion(operacion);
	if(sintaxisCorrecta('3',opAux->parametros)==0){
		//abortarProceso(char*operacion);
		return 0;
	}
	//	memoria* mem =encontrarMemoriaStrong();
	//	int socketClienteKernel = crearSocketCliente(mem->ip,mem->puerto);
		//if(socketClienteKernel){
			int socket = socketMemoriaSolicitada(SC); //todo verificar lo de la tabla
			serializarYEnviarOperacionLQL(socket, opAux);
			log_info(kernel_configYLog->log, "Enviado %s", operacion);
			char * recibido= (char*) recibir(socket);
			log_info(kernel_configYLog->log, "valor recibido: %s", recibido);
			//cerrarConexion(socket);
			free(recibido);
			return 1;
		//}
	free(opAux->operacion);
	free(opAux->parametros);
	free(opAux);
	return 0;
}
int kernel_create(char* operacion){
	operacionLQL* opAux=splitear_operacion(operacion);
	if(sintaxisCorrecta('3',opAux->parametros)==0){
		//abortarProceso(char*operacion);
		return 0;
	}
	guardarTablaCreada(opAux->parametros);
	//	memoria* mem =encontrarMemoriaStrong();
	//	int socketClienteKernel = crearSocketCliente(mem->ip,mem->puerto);
		//if(socketClienteKernel){
			int socket = socketMemoriaSolicitada(SC); //todo verificar lo de la tabla
			serializarYEnviarOperacionLQL(socket, opAux);
			log_info(kernel_configYLog->log, "Enviado %s", operacion);
			char * recibido= (char*) recibir(socket);
			log_info(kernel_configYLog->log, "valor recibido: %s", recibido);
			//cerrarConexion(socket);
			free(recibido);
			return 1;
		//}
	free(opAux->operacion);
	free(opAux->parametros);
	free(opAux);
	return 0;
}
int kernel_describe(char* operacion){
	//printf("Almost done describe\n");
	operacionLQL* opAux=splitear_operacion(operacion);
	if(sintaxisCorrecta('3',opAux->parametros)==0){
			//abortarProceso(char*operacion);
			return 0;
		}
	int socket = socketMemoriaSolicitada(SC); //todo verificar lo de la tabla
	serializarYEnviarOperacionLQL(socket, opAux);
	log_info(kernel_configYLog->log, "Enviado %s", operacion);
	void* recibirBuffer = recibir(socket);;
	metadata* met = deserializarMetadata(recibirBuffer);

	char * recibido= (char*)
	log_info(kernel_configYLog->log, "valor recibido: %s", recibido);
	cerrarConexion(socket);
	free(recibido);
	//return 1;
	free(opAux->operacion);
	free(opAux->parametros);
	free(opAux);
	return 0;
}
int kernel_drop(char* operacion){
	printf("Not yet -> drop\n");
	return 1;
//	operacionLQL* opAux=splitear_operacion(operacion);
//	int socketClienteKernel = crearSocketCliente(ipMemoria,puertoMemoria);
//	serializarYEnviarOperacionLQL(socketClienteKernel, opAux);
//	printf("\n\nEnviado\n\n");
//	char * recibido= (char*) recibir(socketClienteKernel);
//	printf("\n\nValor recibido:%s\n\n",recibido);
//	cerrarConexion(socketClienteKernel);
//	free(recibido);
//	free(opAux->operacion);
//	free(opAux->parametros);
//	free(opAux);
//	return 1;
}
// _____________________________.: OPERACIONES DE API DIRECTAS:.____________________________________________
int kernel_journal(){
	printf("Not yet -> journal\n");
	return 1;
//	operacionLQL* opAux=splitear_operacion("JOURNAL");
//	int socketClienteKernel = crearSocketCliente(ipMemoria,puertoMemoria);
//	serializarYEnviarOperacionLQL(socketClienteKernel, opAux);
//	printf("\n\nEnviado\n\n");
//	char * recibido= (char*) recibir(socketClienteKernel); //todo cambiar recibido
//	printf("\n\nValor recibido:%s\n\n",recibido);
//	cerrarConexion(socketClienteKernel);
//	free(recibido);
//	free(opAux->operacion);
//	free(opAux->parametros);
//	free(opAux);
}
int kernel_metrics(){
	printf("Not yet -> metrics\n");
	return 1;
}

void liberarParametrosSpliteados(char** parametrosSpliteados) {
	int i = 0;
	while(*(parametrosSpliteados + i)) {
		free(*(parametrosSpliteados + i));
		i++;
	}
	free(parametrosSpliteados);
}
int kernel_add(char* operacion){
	//printf("Almost done add memory\n");
	char** opAux = string_n_split(operacion,5," ");
	int numero = atoi(*(opAux+2));
	memoria* mem;
	if((mem = encontrarMemoria(numero))){
		if(string_equals_ignore_case(*(opAux+4),"HASH")){
			list_add(criterios[HASH].memorias, mem ); //todo journal cada vez que se agregue una aca
		}
		else if(string_equals_ignore_case(*(opAux+4),"STRONG")){
			list_add(criterios[STRONG].memorias, mem );
		}
		else if(string_equals_ignore_case(*(opAux+4),"EVENTUAL")){
			list_add(criterios[EVENTUAL].memorias, mem );
		}
		return 1;
	}
	else{
		log_error(kernel_configYLog->log,"No se pudo ejecutar comando: %s debido a la falta de conexion de dicha memoria\n", operacion);
		return 0;
	}
	liberarParametrosSpliteados(opAux);
}
// _________________________________________.: PROCEDIMIENTOS INTERNOS :.____________________________________________
bool instruccion_no_ejecutada(instruccion* instruc){
	return instruc->ejecutado==0;
}
// ---------------.: THREAD ROUND ROBIN :.---------------
void kernel_roundRobin(){
	//while(!list_is_empty(cola_proc_listos)){
		//TODO poner semaforo
	while(1){
	sem_wait(&hayReady);
	pcb* pcb_auxiliar;// = malloc(sizeof(pcb));
	pthread_mutex_lock(&colaListos);
	pcb_auxiliar = (pcb*) list_remove(cola_proc_listos,0);
	pthread_mutex_unlock(&colaListos);
	//printf("%s\n", pcb_auxiliar->operacion);
	if(pcb_auxiliar->instruccion == NULL){
//			if(pcb_auxiliar->ejecutado ==1){  innecesario?
//				pthread_mutex_lock(&colaTerminados);
//				list_add(cola_proc_terminados,pcb_auxiliar);
//				pthread_mutex_unlock(&colaTerminados);
//				//free(pcb_auxiliar);
//			}
//			else
//			{
				pcb_auxiliar->ejecutado=1;
				if(kernel_api(pcb_auxiliar->operacion)==0)
					log_error(kernel_configYLog->log,"No se pudo ejecutar %s\n", pcb_auxiliar->operacion);
				pthread_mutex_lock(&colaTerminados);
				list_add(cola_proc_terminados,pcb_auxiliar);
				pthread_mutex_unlock(&colaTerminados);
				//free(pcb_auxiliar);
//			}
		}
		else if(pcb_auxiliar->instruccion !=NULL){
			int ERROR= 0;
			for(int quantum=0;quantum<quantumMax;quantum++){
				if(pcb_auxiliar->ejecutado ==0){
					pcb_auxiliar->ejecutado=1;
					if(kernel_api(pcb_auxiliar->operacion)==0){
						log_error(kernel_configYLog->log,"No se pudo ejecutar %s\n", pcb_auxiliar->operacion);
//						pthread_mutex_lock(&colaTerminados);
//						list_add(cola_proc_terminados,pcb_auxiliar);
//						pthread_mutex_unlock(&colaTerminados);
						ERROR = -1;
						break;
					}
				}
				//instruccion* instruc=malloc(sizeof(instruccion));
				instruccion* instruc = list_find(pcb_auxiliar->instruccion,(void*)instruccion_no_ejecutada);
				//printf("%s", instruc->operacion);
				instruc->ejecutado = 1;
				if(kernel_api(instruc->operacion)==0){
					log_error(kernel_configYLog->log,"No se pudo ejecutar %s\n", pcb_auxiliar->operacion);
//					pthread_mutex_lock(&colaTerminados);
//					list_add(cola_proc_terminados,pcb_auxiliar);
//					pthread_mutex_unlock(&colaTerminados);
					ERROR = -1;
					break;
				}
			}
			if(list_any_satisfy(pcb_auxiliar->instruccion,(void*)instruccion_no_ejecutada) && ERROR !=-1){
				pthread_mutex_lock(&colaListos);
				list_add(cola_proc_listos, pcb_auxiliar);
				pthread_mutex_unlock(&colaListos);
				sem_post(&hayReady);
			}
			else{
				pthread_mutex_lock(&colaTerminados);
				list_add(cola_proc_terminados, pcb_auxiliar);
				pthread_mutex_unlock(&colaTerminados);
			}
		}
		//sleep(sleepEjecucion);
	}
//		free(pcb_auxiliar->operacion);
//		free(pcb_auxiliar);
}

// ---------------.: THREAD CONSOLA A NEW :.---------------
void kernel_almacenar_en_new(char*operacion){

	pthread_mutex_lock(&colaNuevos);
	list_add(cola_proc_nuevos, operacion);
	pthread_mutex_unlock(&colaNuevos);
	sem_post(&hayNew);
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log, "Se agregó a la cola de new al proceso: %s", operacion);
	pthread_mutex_unlock(&mLog);
}

void kernel_consola(){
	printf("Por favor ingrese <OPERACION> seguido de los argumentos\n\n");
	char* linea= NULL;
	while(1){
		linea = readline("");
		kernel_almacenar_en_new(linea);
	}
}
// ---------------.: THREAD NEW A READY :.---------------
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
	while(1){
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
}
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
	pcb_auxiliar->operacion = operacion;
	pcb_auxiliar->ejecutado = 1 ;
	pcb_auxiliar->instruccion =list_create();

	//instruccion** instruccion_auxiliar;
	while((leer = getline(&lineaLeida, &limite, archivoALeer)) != -1){
		instruccion* instruccion_auxiliar = malloc(sizeof(instruccion));
		instruccion_auxiliar->ejecutado= 0;
		if(*(lineaLeida + leer - 1) == '\n') {
			*(lineaLeida + leer - 1) = '\0';
		}
		instruccion_auxiliar->operacion= string_duplicate(lineaLeida);
		list_add(pcb_auxiliar->instruccion,instruccion_auxiliar);
	}
//	instruccion* in1 = list_get(pcb_auxiliar->instruccion,0);
//	printf("%s\n",in1->operacion);
//	instruccion* in2 = list_get(pcb_auxiliar->instruccion,1);
//	printf("%s\n",in2->operacion);
//	instruccion* in3 = list_get(pcb_auxiliar->instruccion,2);
//	printf("%s\n",in3->operacion);
//	instruccion* in4 = list_get(pcb_auxiliar->instruccion,3);
//	printf("%s\n",in4->operacion);
//	instruccion* in5 = list_get(pcb_auxiliar->instruccion,4);
//	printf("%s\n",in5->operacion);
//	instruccion* in6 = list_get(pcb_auxiliar->instruccion,5);
//	printf("%s\n",in6->operacion);
//	instruccion* in7= list_get(pcb_auxiliar->instruccion,6);
//	printf("%s\n",in7->operacion);
//	instruccion* in8 = list_get(pcb_auxiliar->instruccion,7);
//	printf("%s\n",in8->operacion);
//	instruccion* in9 = list_get(pcb_auxiliar->instruccion,8);
//	printf("%s\n",in9->operacion);
//	instruccion* in10 = list_get(pcb_auxiliar->instruccion,9);
//	printf("%s\n",in10->operacion);
//

	pthread_mutex_lock(&colaNuevos);
	list_add(cola_proc_listos,pcb_auxiliar);
	pthread_mutex_unlock(&colaNuevos);
//free(lineaLeida);
//free(pcb_auxiliar->operacion);
//free(pcb_auxiliar);
//free(lineaAGuardar);
	free(*(opYArg+1));
	free(*(opYArg));
	free(opYArg);
	fclose(archivoALeer);
	sem_post(&hayReady);
}
int kernel_api(char* operacionAParsear) //cuando ya esta en el rr
{
	//printf("%s\n\n", operacionAParsear);
	if(string_contains(operacionAParsear, "INSERT")) {
		return kernel_insert(operacionAParsear);
	}
	else if (string_contains(operacionAParsear, "SELECT")) {
		return kernel_select(operacionAParsear);
	}
	else if (string_contains(operacionAParsear, "DESCRIBE")) {
		return kernel_describe(operacionAParsear);
	}
	else if (string_contains(operacionAParsear, "CREATE")) {
		return kernel_create(operacionAParsear);
	}
	else if (string_contains(operacionAParsear, "DROP")) {
		return kernel_drop(operacionAParsear);
	}
	else if (string_contains(operacionAParsear, "ADD")){
		return kernel_add(operacionAParsear);
	}
	else if (string_contains(operacionAParsear, "JOURNAL")) {
		free(operacionAParsear);
		return kernel_journal();
	}
//	else if (string_contains(operacionAParsear, "RUN")) {
//		return kernel_run(operacionAParsear);
//	}
	else if (string_contains(operacionAParsear, "METRICS")) {
		free(operacionAParsear);
		return kernel_metrics();
	}
	else {
		log_error(kernel_configYLog->log,"No se pudo ejecutar comando: %s, verifique existencia del archivo\n", operacionAParsear ); //operacion);
		return 0;
	}
}
#endif /* KERNEL_OPERACIONES_H_ */
