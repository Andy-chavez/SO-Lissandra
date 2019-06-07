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
int  kernel_create(char*);
int kernel_drop(char*);
void kernel_journal();
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

	return (memoria*) list_find(memorias, (void*)memoriaEsNumero);
}
memoria* encontrarMemoriaStrong(){
//	bool memoriaRandom(memoria* mem) {
//		return mem->numero == numero;
//	}
//
//	return (memoria*) list_find(criterios[criterio].memorias, (void*)memoriaRandom);   de momento sale hardcodeo de la unica memoria que hay
	memoria* mem = malloc(sizeof(memoria));
	mem->ip = ipMemoria;
	mem->puerto = puertoMemoria;
	mem->numero = numPrueba;
	list_add(criterios[STRONG].memorias,mem);
	return mem;
}
//------ CRITERIOS ---------

//------ SINTAXIS CORRECTA ---------
int sintaxisCorrecta(char caso,char* parametros){
	int retorno = 0;
	switch(caso){
		case '1': //1.select
		case '2': //2.insert
		{
			char** parametrosSpliteados = string_split(parametros, " ");
			if(atoi(*(parametrosSpliteados + 1)) && *(parametrosSpliteados + 1) == "0")
				retorno = 1;
		}
			break;
		case '3': //3.create
			//verificar dps todo
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
	if(!sintaxisCorrecta('2',opAux->parametros)){
		//abortarProceso(char*operacion);
		return 0;
	}
	memoria* mem =encontrarMemoriaStrong();
	int socketClienteKernel = crearSocketCliente(mem->ip,mem->puerto);
	serializarYEnviarOperacionLQL(socketClienteKernel, opAux);
	printf("\n\nEnviado\n\n");
	char * recibido= (char*) recibir(socketClienteKernel);
	printf("\n\nValor recibido:%s\n\n",recibido);
	cerrarConexion(socketClienteKernel);
	free(recibido);
	free(opAux->operacion);
	free(opAux->parametros);
	free(opAux);
	return 1;
}
int kernel_select(char* operacion){
	operacionLQL* opAux=splitear_operacion(operacion);
	if(!sintaxisCorrecta('1',opAux->parametros)){
		//abortarProceso(char*operacion);
		return 0;
	}
	memoria* mem =encontrarMemoriaStrong();
	int socketClienteKernel = crearSocketCliente(mem->ip,mem->puerto);
	serializarYEnviarOperacionLQL(socketClienteKernel, opAux);
	printf("\n\nEnviado\n\n");
	char * recibido= (char*) recibir(socketClienteKernel);
	printf("\n\nValor recibido:%s\n\n",recibido);
	cerrarConexion(socketClienteKernel);
	free(recibido);
	free(opAux->operacion);
	free(opAux->parametros);
	free(opAux);
	return 1;
}
int kernel_create(char* operacion){
	operacionLQL* opAux=splitear_operacion(operacion);
	if(!sintaxisCorrecta('3',opAux->parametros)){
		//abortarProceso(char*operacion);
		return 0;
	}
	guardarTablaCreada(opAux->parametros);
	memoria* mem =encontrarMemoriaStrong();
	int socketClienteKernel = crearSocketCliente(mem->ip,mem->puerto);
	serializarYEnviarOperacionLQL(socketClienteKernel, opAux);
	printf("\n\nEnviado\n\n");
	char * recibido= (char*) recibir(socketClienteKernel); //todo cambiar recibido
	printf("\n\nValor recibido:%s\n\n",recibido);
	cerrarConexion(socketClienteKernel);
	free(recibido);
	free(opAux->operacion);
	free(opAux->parametros);
	free(opAux);
	return 1;
}
int kernel_describe(char* operacion){
	operacionLQL* opAux=splitear_operacion(operacion);
	if(!sintaxisCorrecta('4',opAux->parametros)){
			//abortarProceso(char*operacion);
			return 0;
		}
	return 1;
}
int kernel_drop(char* operacion){
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
	return 1;
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
int kernel_metrics(){ //todo dps
	printf("Not yet");
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
	printf("Almost done add memory");
	char** opAux = string_n_split(operacion,5," ");
	int numero = (int)*(opAux+2);
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
	//list_find(memorias,memoriaNumero(void*));
}
// _________________________________________.: PROCEDIMIENTOS INTERNOS :.____________________________________________
//instruccion* obtener_ultima_instruccion(t_list* instruc){
//	int size = list_size(instruc);
//	instruccion *instrucAux;//= malloc(sizeof(instruccion));
//	instrucAux = list_get(instruc,size);
//	return instrucAux;
//}
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
	char** lineaAGuardar = malloc(limite);
	int i = 0;

	//instruccion** instruccion_auxiliar;
	while((leer = getline(&lineaLeida, &limite, archivoALeer)) != -1){
		lineaAGuardar[i] = malloc(leer-1);
		strcpy(lineaAGuardar[i],lineaLeida);
		instruccion* instruccion_auxiliar = malloc(sizeof(instruccion));
		instruccion_auxiliar->ejecutado= 0;
		instruccion_auxiliar->operacion= lineaAGuardar[i];
		list_add(pcb_auxiliar->instruccion,instruccion_auxiliar);
		i ++;
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
	free(lineaAGuardar);
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
		printf("DESCRIBE\n");
		return kernel_describe(operacionAParsear);
	}
	else if (string_contains(operacionAParsear, "CREATE")) {
		printf("CREATE\n");
		return kernel_create(operacionAParsear);
	}
	else if (string_contains(operacionAParsear, "DROP")) {
		printf("DROP\n");
		return kernel_drop(operacionAParsear);
	}
	else if (string_contains(operacionAParsear, "ADD")) {
		//printf("ADD\n");
		return kernel_add(operacionAParsear);
	}
	else {
		printf("Mi no entender esa operacion\n");
		return 0;
		}
	//	else if (string_contains(operacionAParsear, "JOURNAL")) {
//		printf("JOURNAL\n");
//TODO	return kernel_journal();
//	}
//	else if (string_contains(operacionAParsear, "RUN")) {
//		printf("Ha utilizado el comando RUN, su archivo comenzará a ser ejecutado\n");
//TODO	return kernel_run(*argumentos);
//	}
//	else if (string_contains(operacionAParsear, "METRICS")) {
//		printf("METRICS\n");
//		return kernel_metrics();
//	}
}
#endif /* KERNEL_OPERACIONES_H_ */
