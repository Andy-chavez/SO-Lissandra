#ifndef KER_OPERACIONES_H_
#define KER_OPERACIONES_H_

#include "ker_configuraciones.h"
#include "ker_structs.h"

/******************************DECLARACIONES******************************************/
/* TODO metrics, hash de criterio y eventual
 * metrics -> variables globales con semaforos
 * journal -> pasarselo a memoria
 */
bool kernel_create(char* operacion);
bool kernel_describe(char* operacion);
bool kernel_journal();
bool kernel_metrics();
bool kernel_api(char* operacionAParsear);
bool kernel_add(char* operacion);
bool kernel_drop(char* operacion);
bool kernel_select(char* operacion);
bool kernel_insert(char* operacion);

void journal_consistencia(int consistencia);
void kernel_almacenar_en_new(char*operacion);
void kernel_crearPCB(char* operacion);
void kernel_run(char* operacion);
void kernel_pasar_a_ready();
void kernel_consola();
void kernel_roundRobin(int threadProcesador);
void joinThreadRR();
void crearThreadRR(int numero);
/******************************IMPLEMENTACIONES******************************************/
// _____________________________.: OPERACIONES DE API PARA LAS CUALES SELECCIONAR MEMORIA SEGUN CRITERIO:.____________________________________________
bool kernel_insert(char* operacion, int thread){
	operacionLQL* opAux=splitear_operacion(operacion);
	char** parametros = string_n_split(operacion,3," ");
	consistencia consist =encontrarConsistenciaDe(*(parametros+1));
	if(consist == -1){
		return false;
	}
	int index =  obtenerIndiceDeConsistencia(consist);
	if((enviarOperacion(opAux,index))== -1){
		return false;
	}
	liberarParametrosSpliteados(parametros);
	return true;
}
bool kernel_select(char* operacion, int thread){
	operacionLQL* opAux=splitear_operacion(operacion);
	char** parametros = string_n_split(opAux->parametros,2," ");
	consistencia consist =encontrarConsistenciaDe(*(parametros));
	if(consist == -1){
		return false;
	}
	int index =  obtenerIndiceDeConsistencia(consist);
	if((enviarOperacion(opAux,index))== -1){
		return false;
	}
	liberarParametrosSpliteados(parametros);
	return true;
}
bool kernel_create(char* operacion, int thread){
	operacionLQL* opAux=splitear_operacion(operacion);
	guardarTablaCreada(opAux->parametros);
	char** parametros = string_n_split(opAux->parametros,2," ");
	consistencia consist =encontrarConsistenciaDe(*(parametros));
	if(consist == -1){
		return false;
	}
	int index =  obtenerIndiceDeConsistencia(consist);
	if((enviarOperacion(opAux,index))== -1){
		eliminarTablaCreada(*(parametros+1));
		return false;
	}
	liberarParametrosSpliteados(parametros);
	return true;
}
bool kernel_describe(char* operacion, int thread){
	if(string_length(operacion) <= string_length("describe ")){
		operacionLQL* opAux=splitear_operacion(operacion);
		int socket = obtenerSocketAlQueSeEnvio(opAux,EVENTUAL);
		if(socket != -1){
			recibirYDeserializarPaqueteDeMetadatasRealizando(socket, actualizarListaMetadata);
			pthread_mutex_lock(&mLog);
			log_info(kernel_configYLog->log, " RECIBIDO: Describe realizado"); //ver este tema del log cuando probemos
			pthread_mutex_unlock(&mLog);
			cerrarConexion(socket);
			return true;
		}
		return false;
	}
	operacionLQL* operacionAux = malloc(sizeof(operacionLQL));
	char** opSpliteada = string_n_split(operacion,2," ");
	operacionAux->operacion=string_duplicate(*opSpliteada);
	if(*(opSpliteada+1))
		operacionAux->parametros=string_duplicate(*(opSpliteada+1));
	free(*(opSpliteada+1));
	free(*opSpliteada);
	free(opSpliteada);
	consistencia consist =encontrarConsistenciaDe(operacionAux->parametros);
	if(consist == -1){
		return false;
	}
	int index =  obtenerIndiceDeConsistencia(consist);
	int socket = obtenerSocketAlQueSeEnvio(operacionAux,index);
	if(socket == -1)
		return false;
	void* buffer =recibir(socket);
	if(buffer == NULL){
		return false;
	}
	actualizarListaMetadata(deserializarMetadata(buffer));
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log, " RECIBIDO: Describe realizado"); //ver este tema del log cuando probemos
	pthread_mutex_unlock(&mLog);
	cerrarConexion(socket);
	liberarOperacionLQL(operacionAux);
	return true;
}
bool kernel_drop(char* operacion, int thread){
	operacionLQL* opAux=splitear_operacion(operacion);
	char** parametros = string_n_split(operacion,2," ");
	consistencia consist =encontrarConsistenciaDe(opAux->parametros);
	if(consist == -1){
		return false;
	}
	int index =  obtenerIndiceDeConsistencia(consist);
	if((enviarOperacion(opAux,index))== -1){
		guardarTablaCreada(opAux->parametros);
		return false;
	}
	eliminarTablaCreada(*(parametros+1));
	liberarParametrosSpliteados(parametros);
	return 0;
}
// _____________________________.: OPERACIONES DE API DIRECTAS:.____________________________________________
bool kernel_journal(){
	journal_consistencia(0);
	journal_consistencia(1);
	//journal_consistencia(2); todo
	return true;
}
bool kernel_metrics(){
	printf("Not yet -> metrics\n");
	return 0;
}
void journal_consistencia(int consistencia){
	void realizarJournal(memoria * mem){
		int socket = crearSocketCliente(mem->ip,mem->puerto);
		log_info(kernel_configYLog->log, "@@ journal memoria: %d", mem->numero);
		enviarJournal(socket);
	}
	list_iterate(criterios[consistencia].memorias,(void*)realizarJournal);
}
bool kernel_add(char* operacion){
	char** opAux = string_n_split(operacion,5," ");
	int numero = atoi(*(opAux+2));
	memoria* mem;
	if((mem = encontrarMemoria(numero))){
		if(string_contains(*(opAux+4),"HASH")){
			agregarALista(criterios[HASH].memorias, mem, mHash);
			journal_consistencia(HASH);
			liberarParametrosSpliteados(opAux);
			return true;
		}
		else if(string_contains(*(opAux+4),"STRONG")){
			if(list_size(criterios[STRONG].memorias)==0){
				agregarALista(criterios[STRONG].memorias, mem, mStrong);
				liberarParametrosSpliteados(opAux);
				return true;
			}
			else{
				pthread_mutex_lock(&mLog);
				log_error(kernel_configYLog->log,"EXEC: %s.Ya se posee una memoria del tipo STRONG.", operacion);
				pthread_mutex_unlock(&mLog);
				liberarParametrosSpliteados(opAux);
				return false;
			}
		}
		else if(string_contains(*(opAux+4),"EVENTUAL")){
			agregarALista(criterios[EVENTUAL].memorias, mem, mEventual);
			liberarParametrosSpliteados(opAux);
			return true;
		}
		pthread_mutex_lock(&mLog);
		log_error(kernel_configYLog->log,"EXEC: %s.Consistencia invalida.", operacion);
		pthread_mutex_unlock(&mLog);
		liberarParametrosSpliteados(opAux);
		return false;
	}
	else{
		liberarParametrosSpliteados(opAux);
		return false;
	}
}
// _________________________________________.: PROCEDIMIENTOS INTERNOS :.____________________________________________
// ---------------.: THREAD ROUND ROBIN :.---------------
void crearThreadRR(int numero){
	t_thread* t = malloc(sizeof(t_thread));
	t->thread = malloc(sizeof(pthread_t*));
	t->numero = numero;
	pthread_create(t->thread, NULL,(void*) kernel_roundRobin, (void*)t->numero);
	agregarALista(rrThreads,t,mThread);
}
void joinThreadRR(){
	void realizarJoin(t_thread* t){
		pthread_join(*(t->thread),NULL);
	}
	pthread_mutex_lock(&mThread);
	list_iterate(rrThreads, (void*)realizarJoin);
	pthread_mutex_unlock(&mThread);
}
void kernel_roundRobin(int threadProcesador){
	while(!destroy){
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
			if(!kernel_api(pcb_auxiliar->operacion)){
				thread_loggearInfoEXEC("@ EXEC",threadProcesador, pcb_auxiliar->operacion);
				usleep(sleep);
				continue;
			}
			thread_loggearInfoEXEC("EXEC",threadProcesador, pcb_auxiliar->operacion);
			agregarALista(cola_proc_terminados, pcb_auxiliar,colaTerminados);
			thread_loggearInfoEXEC("FINISHED",threadProcesador, pcb_auxiliar->operacion);
			usleep(sleep);
			continue;
		}
		else if(pcb_auxiliar->instruccion !=NULL){
			int ERROR = 0;
			for(int quantum=0;quantum<q;quantum++){
				if(pcb_auxiliar->ejecutado ==0){
					pcb_auxiliar->ejecutado=1;
					if(kernel_api(pcb_auxiliar->operacion)==false){
						thread_loggearInfoEXEC("@ EXEC",threadProcesador, pcb_auxiliar->operacion);
						ERROR = -1;
						break;
					}
					thread_loggearInfoEXEC("EXEC",threadProcesador, pcb_auxiliar->operacion);
					usleep(sleep);
					continue;
				}
				instruccion* instruc = list_find(pcb_auxiliar->instruccion,(void*)instruccion_no_ejecutada);
				if (instruc == NULL){
					break;
				}
				instruc->ejecutado = 1;
				if(kernel_api(instruc->operacion)==false){
					thread_loggearInfoEXEC("@ EXEC",threadProcesador, pcb_auxiliar->operacion);
					ERROR = -1;
					break;
				}
				thread_loggearInfoEXEC("EXEC",threadProcesador, pcb_auxiliar->operacion);
				usleep(sleep);
			}
			if(list_any_satisfy(pcb_auxiliar->instruccion,(void*)instruccion_no_ejecutada && ERROR !=-1)){
				thread_loggearInfoEXEC("NEW",threadProcesador, pcb_auxiliar->operacion);
				agregarALista(cola_proc_listos, pcb_auxiliar,colaListos);
				sem_post(&hayReady);
				usleep(sleep);
				continue;
			}
			else if(ERROR ==-1){
				agregarALista(cola_proc_terminados, pcb_auxiliar,colaTerminados);
				thread_loggearInfoEXEC("@ FINISHED",threadProcesador, pcb_auxiliar->operacion);
				usleep(sleep);
				continue;
			}
			else{
				agregarALista(cola_proc_terminados, pcb_auxiliar,colaTerminados);
				thread_loggearInfoEXEC("FINISHED",threadProcesador, pcb_auxiliar->operacion);
				usleep(sleep);
				continue;
			}

		}
	}
}
// ---------------.: THREAD CONSOLA A NEW :.---------------
void kernel_almacenar_en_new(char*operacion){
	if (string_contains( operacion, "x")||string_contains( operacion, "X")) {
		void kernel_destroy();
		return;
	}
	agregarALista(cola_proc_nuevos,operacion,colaNuevos);
	sem_post(&hayNew);
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log, " NEW: %s", operacion);
	pthread_mutex_unlock(&mLog);
}
void kernel_consola(){
	printf(">> Welcome to Kernel <<\n"
			">> Ingrese alguna de las siguientes operaciones:\n"
			">> SELECT [NOMBRE_TABLA] [KEY]\n"
			">> INSERT [NOMBRE_TABLA] [KEY] \"[VALUE]\" [TIMESTAMP] <timestamp opcional>\n"
			">> CREATE [NOMBRE_TABLA] [TIPO_CONSISTENCIA] [NUMERO_PARTICIONES] [NUMERO_PARTICIONES] [COMPACTATION_TIME]\n"
			">> DESCRIBE [NOMBRE_TABLA] <nombre de tabla opcional>\n"
			">> DROP [NOMBRE_TABLA]\n"
			">> METRICS\n"
			">> JOURNAL\n"
			">> ADD MEMORY [NUMERO] TO [STRONG/HASH/EVENTUAL]\n"
			">> DROP [NOMBRE_TABLA]\n"
			">> RUN [PATH_ARCHIVO]\n"
			">> Y siga su ejecucion mediante el archivo Kernel.log\n");
	char* linea= NULL;
	while(!destroy){
		printf(" ");
		linea = readline("");
		kernel_almacenar_en_new(linea);
	}
	free(linea);
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
	while(!destroy){
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
		return;
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
bool kernel_api(char* operacionAParsear, int thread)
{
	if(string_contains(operacionAParsear, "INSERT")) {
		return kernel_insert(operacionAParsear,thread);
	}
	else if (string_contains(operacionAParsear, "SELECT")) {
		return kernel_select(operacionAParsear,thread);
	}
	else if (string_contains(operacionAParsear, "DESCRIBE")) {
		return kernel_describe(operacionAParsear,thread);
	}
	else if (string_contains(operacionAParsear, "CREATE")) {
		return kernel_create(operacionAParsear,thread);
	}
	else if (string_contains(operacionAParsear, "DROP")) {
		return kernel_drop(operacionAParsear,thread);
	}
	else if (string_contains(operacionAParsear, "ADD")){
		return kernel_add(operacionAParsear,thread);
	}
	else if (string_contains(operacionAParsear, "JOURNAL")) {
		return kernel_journal();
	}
	else if (string_contains(operacionAParsear, "METRICS")) {
		return kernel_metrics();
	}
	else {
//		pthread_mutex_lock(&mLog);
//		log_error(kernel_configYLog->log,"EXEC: %s", operacionAParsear );
//		pthread_mutex_unlock(&mLog);
		return false;
	}
}
#endif /* KER_OPERACIONES_H_ */
