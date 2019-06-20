#ifndef KER_OPERACIONES_H_
#define KER_OPERACIONES_H_

#include "ker_configuraciones.h"
#include "ker_structs.h"

/******************************DECLARACIONES******************************************/
/* TODO describe, metrics y journal(a cada una de las memorias asociadas a los criterios), hash de criterio y eventual
 * los primeros 5 pasarlos a la memoria elegida por el criterio de la tabla
 * metrics -> variables globales con semaforos
 * journal -> pasarselo a memoria
 */
int kernel_create(char* operacion);
int kernel_describe(char* operacion);
int kernel_journal();
int kernel_metrics();
int kernel_api(char* operacionAParsear);
int kernel_add(char* operacion);
int enviarJournal(int socket);
int kernel_drop(char* operacion);
int kernel_select(char* operacion);
int kernel_insert(char* operacion);

void journal_strong();
void journal_hash();
void journal_eventual();
void kernel_almacenar_en_new(char*operacion);
void kernel_crearPCB(char* operacion);
void kernel_run(char* operacion);
void kernel_pasar_a_ready();
void kernel_consola();
void kernel_roundRobin(int threadProcesador);
/******************************IMPLEMENTACIONES******************************************/
// _____________________________.: OPERACIONES DE API PARA LAS CUALES SELECCIONAR MEMORIA SEGUN CRITERIO:.____________________________________________
int kernel_insert(char* operacion){ //todo, ver lo de seleccionar la memoria a la cual mandarle esto
	operacionLQL* opAux=splitear_operacion(operacion);
	char** parametros = string_n_split(operacion,2," ");
	consistencia consistenciaSolicitada = encontrarConsistenciaDe(*(parametros));
	int socket = socketMemoriaSolicitada(consistenciaSolicitada);
	serializarYEnviarOperacionLQL(socket, opAux);
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log, "ENVIADO: %s", operacion);
	pthread_mutex_unlock(&mLog);
	char* recibido = (char*) recibir(socket);
	if(recibidoContiene(recibido, "ERROR")){
		loggearErrorYLiberarParametrosEXEC(recibido,opAux);
		liberarParametrosSpliteados(parametros);
		return -1;
	}
	else if(recibidoContiene(recibido, "FULL")){
		enviarJournal(socket);
	}
	loggearInfoYLiberarParametrosEXEC(recibido,opAux);
	liberarParametrosSpliteados(parametros);
	return 0;
}
int kernel_select(char* operacion){
	operacionLQL* opAux=splitear_operacion(operacion);
	char** parametros = string_n_split(operacion,2," ");
	consistencia consistenciaSolicitada = encontrarConsistenciaDe(*(parametros));
	int socket = socketMemoriaSolicitada(consistenciaSolicitada);
	serializarYEnviarOperacionLQL(socket, opAux);
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log, "ENVIADO: %s", operacion);
	pthread_mutex_unlock(&mLog);
	char* recibido = (char*) recibir(socket);
	if(recibidoContiene(recibido, "ERROR")){
		loggearErrorYLiberarParametrosEXEC(recibido,opAux);
		liberarParametrosSpliteados(parametros);
		return -1;
	}
	loggearInfoYLiberarParametrosEXEC(recibido,opAux);
	liberarParametrosSpliteados(parametros);
	return 0;
}
int kernel_create(char* operacion){
	operacionLQL* opAux=splitear_operacion(operacion);
	guardarTablaCreada(opAux->parametros);
	char** parametros = string_n_split(operacion,2," ");
	consistencia consistenciaSolicitada = encontrarConsistenciaDe(*(parametros));
	int socket = socketMemoriaSolicitada(consistenciaSolicitada);
	serializarYEnviarOperacionLQL(socket, opAux);
	char* recibido = (char*) recibir(socket);
	if(recibidoContiene(recibido, "ERROR")){
		loggearErrorYLiberarParametrosEXEC(recibido,opAux);
		liberarParametrosSpliteados(parametros);
		return -1;
	}
	loggearInfoYLiberarParametrosEXEC(recibido,opAux);
	liberarParametrosSpliteados(parametros);
	return 0;
}
int kernel_describe(char* operacion){
	operacionLQL* opAux=splitear_operacion(operacion);
	consistencia consistenciaSolicitada = encontrarConsistenciaDe(opAux->parametros);
	int socket = socketMemoriaSolicitada(consistenciaSolicitada);
	serializarYEnviarOperacionLQL(socket, opAux);
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log, "ENVIADO: %s", operacion);
	pthread_mutex_unlock(&mLog);
	metadata* met = deserializarMetadata(recibir(socket));
	//TODO ACTUALIZAR ESTRUCTURAS y arreglar esto
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log, "RECIBIDO: %s %d", met->nombreTabla, met->tipoConsistencia);
	pthread_mutex_unlock(&mLog);
	liberarMetadata(met);
	liberarOperacionLQL(opAux);
	return 0;
}
int kernel_drop(char* operacion){
	operacionLQL* opAux=splitear_operacion(operacion);
	consistencia consistenciaSolicitada = encontrarConsistenciaDe(opAux->parametros);
	int socket = socketMemoriaSolicitada(consistenciaSolicitada);
	serializarYEnviarOperacionLQL(socket, opAux);
	char* recibido = (char*) recibir(socket);
	if(recibidoContiene(recibido, "ERROR")){
		loggearErrorYLiberarParametrosEXEC(recibido,opAux);
		return -1;
	}
	loggearInfoYLiberarParametrosEXEC(recibido,opAux);
	return 0;
}
// _____________________________.: OPERACIONES DE API DIRECTAS:.____________________________________________
int kernel_journal(){
	operacionLQL* opAux=splitear_operacion("JOURNAL");
	int socket = socketMemoriaSolicitada(SC);
	serializarYEnviarOperacionLQL(socket, opAux);
	char* recibido = (char*) recibir(socket);
	if(recibidoContiene(recibido, "ERROR")){
		loggearErrorYLiberarParametrosEXEC(recibido,opAux);
		return -1;
	}
	loggearInfoYLiberarParametrosEXEC(recibido,opAux);
	return 0;
}
int kernel_metrics(){
	printf("Not yet -> metrics\n");
	return 0;
}
void journal_strong(){
	enviarJournal(socketMemoriaSolicitada(SC));
}
void journal_hash(){

}
void journal_eventual(){

}
int kernel_add(char* operacion){ //TODO preguntar si mem full cuado
	char** opAux = string_n_split(operacion,5," ");
	int numero = atoi(*(opAux+2));
	memoria* mem;
	if((mem = encontrarMemoria(numero))){
		if(string_equals_ignore_case(*(opAux+4),"HASH")){
			mem->socket = realizarConexion(mem);
			list_add(criterios[HASH].memorias, mem );
			kernel_journal();
			//todo journal cada vez que se agregue una aca a TODAS las memorias de este criterio
		}
		else if(string_equals_ignore_case(*(opAux+4),"STRONG")){
			if(list_size(criterios[STRONG].memorias)==0){
				mem->socket = realizarConexion(mem);
				list_add(criterios[STRONG].memorias, mem );
			}
			else{
				pthread_mutex_lock(&mLog);
				log_error(kernel_configYLog->log,"EXEC: %s.Ya se posee una memoria del tipo STRONG.", operacion);
				pthread_mutex_unlock(&mLog);
			}

		}
		else if(string_equals_ignore_case(*(opAux+4),"EVENTUAL")){
			mem->socket = realizarConexion(mem);
			list_add(criterios[EVENTUAL].memorias, mem );
		}
		return 0;
	}
	else{
		pthread_mutex_lock(&mLog);
		log_error(kernel_configYLog->log,"EXEC: %s.Memmoria no conectada.", operacion);
		pthread_mutex_unlock(&mLog);
		return -1;
	}
	liberarParametrosSpliteados(opAux);
}
// _________________________________________.: PROCEDIMIENTOS INTERNOS :.____________________________________________
// ---------------.: THREAD ROUND ROBIN :.---------------
void kernel_roundRobin(int threadProcesador){
	while(1){
		sem_wait(&hayReady);
		pcb* pcb_auxiliar;
		pthread_mutex_lock(&colaListos);
		pcb_auxiliar = (pcb*) list_remove(cola_proc_listos,0);
		pthread_mutex_unlock(&colaListos);
		int sleep;
		pthread_mutex_lock(&sleepExec);
		sleep = sleepEjecucion*1000;
		pthread_mutex_unlock(&sleepExec);
		int q;
		pthread_mutex_lock(&quantum);
		q = quantumMax;
		pthread_mutex_unlock(&quantum);
		if(pcb_auxiliar->instruccion == NULL){
			pcb_auxiliar->ejecutado=1;
			if(kernel_api(pcb_auxiliar->operacion)==-1){
				loggearErrorEXEC("EXEC",threadProcesador, pcb_auxiliar->operacion);
			}
			agregarALista(cola_proc_terminados, pcb_auxiliar,colaTerminados);
			loggearInfoEXEC("FINISHED",threadProcesador, pcb_auxiliar->operacion);
			usleep(sleep);
			continue;
		}
		else if(pcb_auxiliar->instruccion !=NULL){
			int ERROR= 0;

			for(int quantum=0;quantum<q;quantum++){
				if(pcb_auxiliar->ejecutado ==0){
					pcb_auxiliar->ejecutado=1;
					if(kernel_api(pcb_auxiliar->operacion)==-1){
						loggearErrorEXEC("EXEC",threadProcesador, pcb_auxiliar->operacion);
						ERROR = -1;
						break;
					}
				}
				instruccion* instruc = list_find(pcb_auxiliar->instruccion,(void*)instruccion_no_ejecutada);
				if (instruc == NULL){
					break;
				}
				instruc->ejecutado = 1;
				if(kernel_api(instruc->operacion)==-1){
					loggearErrorEXEC("EXEC",threadProcesador, pcb_auxiliar->operacion);
					ERROR = -1;
					break;
				}
				usleep(sleep);
			}
			if(list_any_satisfy(pcb_auxiliar->instruccion,(void*)instruccion_no_ejecutada) && ERROR !=-1){
				agregarALista(cola_proc_listos, pcb_auxiliar,colaListos);
				sem_post(&hayReady);
			}
			else{
				agregarALista(cola_proc_terminados, pcb_auxiliar,colaTerminados);
				loggearInfoEXEC("FINISHED",threadProcesador, pcb_auxiliar->operacion);
				usleep(sleep);
				continue;
			}

		}
	}
}
// ---------------.: THREAD CONSOLA A NEW :.---------------
void kernel_almacenar_en_new(char*operacion){
	pthread_mutex_lock(&colaNuevos);
	list_add(cola_proc_nuevos, operacion);
	pthread_mutex_unlock(&colaNuevos);
	sem_post(&hayNew);
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log, "NEW: %s", operacion);
	pthread_mutex_unlock(&mLog);
}
void kernel_consola(){
	printf("Proceso Kernel:\n	Ingrese la operacion que desea ejecutar y siga su ejecución mediante el archivo KERNEL.log\n");
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
	agregarALista(cola_proc_listos, pcb_auxiliar,colaListos);
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
		else if(string_contains(operacion, "SELECT") || string_contains(operacion, "INSERT") ||
				string_contains(operacion, "CREATE") || string_contains(operacion, "DESCRIBE") ||
				string_contains(operacion, "DROP") ||  string_contains(operacion, "JOURNAL") ||
				string_contains(operacion, "METRICS") || string_contains(operacion, "ADD")){
			kernel_crearPCB(operacion);
		}
		else{
			pthread_mutex_lock(&mLog);
			log_error(kernel_configYLog->log,"NEW: %s", operacion);
			pthread_mutex_unlock(&mLog);
		}
	}
}
void kernel_run(char* operacion){
	char** opYArg;
	opYArg = string_n_split(operacion ,2," ");
	string_to_lower(*(opYArg+1));
	FILE *archivoALeer;
	if ((archivoALeer= fopen((*(opYArg+1)), "r")) == NULL){
		pthread_mutex_lock(&mLog);
		log_error(kernel_configYLog->log,"EXEC: %s %s. (Consejo: verifique existencia del archivo)", *opYArg, *(opYArg+1) ); //operacion);
		pthread_mutex_unlock(&mLog);
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

	while((leer = getline(&lineaLeida, &limite, archivoALeer)) != -1){
		instruccion* instruccion_auxiliar = malloc(sizeof(instruccion));
		instruccion_auxiliar->ejecutado= 0;
		if(*(lineaLeida + leer - 1) == '\n') {
			*(lineaLeida + leer - 1) = '\0';
		}
		instruccion_auxiliar->operacion= string_duplicate(lineaLeida);
		list_add(pcb_auxiliar->instruccion,instruccion_auxiliar);
	}
	agregarALista(cola_proc_listos, pcb_auxiliar,colaListos);
	free(*(opYArg+1));
	free(*(opYArg));
	free(opYArg);
	fclose(archivoALeer);
	sem_post(&hayReady);
}
int kernel_api(char* operacionAParsear)
{
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
	else if (string_contains(operacionAParsear, "METRICS")) {
		free(operacionAParsear);
		return kernel_metrics();
	}
	else {
		pthread_mutex_lock(&mLog);
		log_error(kernel_configYLog->log,"EXEC: %s", operacionAParsear );
		pthread_mutex_unlock(&mLog);
		free(operacionAParsear);
		return 0;
	}
}
#endif /* KER_OPERACIONES_H_ */
