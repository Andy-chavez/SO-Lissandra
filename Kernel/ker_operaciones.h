#ifndef KER_OPERACIONES_H_
#define KER_OPERACIONES_H_

#include "ker_configuraciones.h"
#include "ker_structs.h"

/******************************IMPLEMENTACIONES******************************************/
void metrics(){
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);
	metrics_resetVariables();
	while(!destroy){
		kernel_metrics(0);
		metrics_resetVariables();
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
		usleep(30000*1000);
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);
	}
}
// _____________________________.: OPERACIONES DE API PARA LAS CUALES SELECCIONAR MEMORIA SEGUN CRITERIO:.____________________________________________
bool kernel_insert(char* operacion, int thread){
	time_t tiempo =time(NULL);
	operacionLQL* opAux=splitear_operacion(operacion);
	char** parametros = string_n_split(operacion,3," ");
	consistencia consist =encontrarConsistenciaDe(*(parametros+1));
	if(consist == -1){
		liberarParametrosSpliteados(parametros);
		return false;
	}
	int index =  obtenerIndiceDeConsistencia(consist);
	if((enviarOperacion(opAux,index,thread))== -1){
		tiempo = time(NULL) - tiempo;
		criterios[index].tiempoInserts += tiempo; //((double)tiempo)/CLOCKS_PER_SEC;
		liberarParametrosSpliteados(parametros);
		return false;
	}
	tiempo = time(NULL) - tiempo;
	criterios[index].tiempoInserts += tiempo; //((double)tiempo)/CLOCKS_PER_SEC;
	liberarParametrosSpliteados(parametros);
	return true;
}
bool kernel_select(char* operacion, int thread){
//	struct timeval horaInicio;
//	struct timeval horaFin;
	unsigned long tiempo = time(NULL);

	operacionLQL* opAux=splitear_operacion(operacion);
	char** parametros = string_n_split(opAux->parametros,2," ");
	consistencia consist =encontrarConsistenciaDe(*(parametros));
	if(consist == -1){
		return false;
	}
	int index =  obtenerIndiceDeConsistencia(consist);
	if((enviarOperacion(opAux,index,thread))== -1){
		tiempo = time(NULL) - tiempo;
//		gettimeofday(&horaInicio);
		actualizarTiemposSelect(index,tiempo);
		return false;
	}
//	gettimeofday(&horaFin);
	tiempo = time(NULL) - tiempo;
	actualizarTiemposSelect(index,tiempo);
	liberarParametrosSpliteados(parametros);
	return true;
}
bool kernel_create(char* operacion, int thread){
	operacionLQL* opAux=splitear_operacion(operacion);
	guardarTablaCreada(opAux->parametros);
	char** parametros = string_n_split(opAux->parametros,2," ");
	consistencia consist =encontrarConsistenciaDe(*(parametros));
	if(consist == -1){
		liberarParametrosSpliteados(parametros);
		return false;
	}
	int index =  obtenerIndiceDeConsistencia(consist);
	if((enviarOperacion(opAux,index,thread))== -1){
		eliminarTablaCreada(*(parametros+1));
		liberarParametrosSpliteados(parametros);
		return false;
	}
	liberarParametrosSpliteados(parametros);
	return true;
}
bool kernel_describe(char* operacion, int thread){
	if(string_length(operacion) <= string_length("describe ")){
		operacionLQL* opAux=splitear_operacion(operacion);
		int socket = crearSocketCliente(ipMemoria,puertoMemoria);
		if(socket != -1){
			serializarYEnviarOperacionLQL(socket, opAux);
			pthread_mutex_lock(&mLog);
			log_info(kernel_configYLog->log, " ENVIADO: %s %s", opAux->operacion, opAux->parametros);
			pthread_mutex_unlock(&mLog);
			pthread_mutex_lock(&mLogResultados);
			log_info(logResultados, " [E] %s %s", opAux->operacion, opAux ->parametros);
			pthread_mutex_unlock(&mLogResultados);
			void* bufferProtocolo = recibir(socket);
			if(bufferProtocolo == NULL){
				pthread_mutex_lock(&mLog);
				log_info(kernel_configYLog->log, " @ DESCONEXION DE MEMORIA: No se realizo describe all");
				pthread_mutex_unlock(&mLog);
				free(bufferProtocolo);
				return false;
			}
			operacionProtocolo protocolo = empezarDeserializacion(&bufferProtocolo);
			if(protocolo == METADATA){
				metadata * met = deserializarMetadata(bufferProtocolo);
				actualizarListaMetadata(met);
			}
			if(protocolo == PAQUETEMETADATAS)
				recibirYDeserializarPaqueteDeMetadatasRealizando(socket, actualizarListaMetadata);
			if(protocolo == ERROR){
				pthread_mutex_lock(&mLog);
				log_info(kernel_configYLog->log, "@ RECIBIDO: Describe realizado");
				pthread_mutex_unlock(&mLog);
			}
			pthread_mutex_lock(&mLog);
			log_info(kernel_configYLog->log, " RECIBIDO: Describe realizado");
			pthread_mutex_unlock(&mLog);
			cerrarConexion(socket);
			free(bufferProtocolo);
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
		free(buffer);
		return false;
	}
	actualizarListaMetadata(deserializarMetadata(buffer));
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log, " RECIBIDO: Describe realizado"); //ver este tema del log cuando probemos
	pthread_mutex_unlock(&mLog);
	cerrarConexion(socket);
	free(buffer);
	liberarOperacionLQL(operacionAux);
	return true;
}
bool kernel_drop(char* operacion, int thread){
	operacionLQL* opAux=splitear_operacion(operacion);
	char** parametros = string_n_split(operacion,2," ");
	consistencia consist =encontrarConsistenciaDe(opAux->parametros);
	if(consist == -1){
		liberarParametrosSpliteados(parametros);
		return false;
	}
	int index =  obtenerIndiceDeConsistencia(consist);
	if((enviarOperacion(opAux,index,thread))== -1){
		guardarTablaCreada(*(parametros+1));
		liberarParametrosSpliteados(parametros);
		return false;
	}
	eliminarTablaCreada(*(parametros+1));
	liberarParametrosSpliteados(parametros);
	//liberarOperacionLQL(opAux);
	return true;
}
// _____________________________.: OPERACIONES DE API DIRECTAS:.____________________________________________
bool kernel_journal(){
	journal_consistencia(0);
	journal_consistencia(1);
	journal_consistencia(2);
	return true;
}
bool kernel_metrics(int consolaOLog){ // consola 1 log 0
	pthread_mutex_lock(&mHash);
	int hash_cantidadSelect = criterios[HASH].cantidadSelects;
	int hash_cantidadInsert = criterios[HASH].cantidadInserts;
	unsigned long hash_tiempoSelect = criterios[HASH].tiempoSelects;
	unsigned long hash_tiempoInsert = criterios[HASH].tiempoInserts;
	pthread_mutex_unlock(&mHash);
	pthread_mutex_lock(&mStrong);
	int strong_cantidadSelect = criterios[STRONG].cantidadSelects;
	int strong_cantidadInsert = criterios[STRONG].cantidadInserts;
	unsigned long strong_tiempoSelect = criterios[STRONG].tiempoSelects;
	unsigned long strong_tiempoInsert = criterios[STRONG].tiempoInserts;
	pthread_mutex_unlock(&mStrong);
	pthread_mutex_lock(&mEventual);
	int eventual_cantidadSelect = criterios[EVENTUAL].cantidadSelects;
	int eventual_cantidadInsert = criterios[EVENTUAL].cantidadInserts;
	unsigned long eventual_tiempoSelect = criterios[EVENTUAL].tiempoSelects;
	unsigned long eventual_tiempoInsert = criterios[EVENTUAL].tiempoInserts;
	pthread_mutex_unlock(&mEventual);
	void printearMetrics(memoria* mem){
		printf(">MEMORIA[%d]: Selects %d Inserts %d\n",mem->numero,mem->cantidadSel, mem->cantidadIns);
	}
	void loggearMetrics(memoria* mem){
		pthread_mutex_lock(&mLogMetrics);
		log_info(logMetrics,">MEMORIA[%d]: Selects %d Inserts %d\n",mem->numero,mem->cantidadSel, mem->cantidadIns);
		pthread_mutex_unlock(&mLogMetrics);
	}
	if(consolaOLog==0){
		pthread_mutex_lock(&mLogMetrics);
		log_info(logMetrics, "METRICS: HASH\n"
				">tiempo en selects de HASH: %lu,\n>tiempo en inserts de HASH: %lu,\n"
				">cantidad inserts en HASH : %d,\n>cantidad selects en HASH : %d\n",
				hash_tiempoSelect,hash_tiempoInsert,hash_cantidadInsert,hash_cantidadSelect);
		pthread_mutex_lock(&mHash);
		list_iterate(criterios[HASH].memorias,(void*)loggearMetrics);
		pthread_mutex_unlock(&mHash);
		pthread_mutex_unlock(&mLogMetrics);

		pthread_mutex_lock(&mLogMetrics);
		log_info(logMetrics, "METRICS: STRONG\n"
				">tiempo en selects de STRONG: %lu,\n>tiempo en inserts de STRONG: %lu,\n"
				">cantidad inserts en STRONG : %d,\n>cantidad selects en STRONG : %d\n",
				strong_tiempoSelect,strong_tiempoInsert,strong_cantidadInsert,strong_cantidadSelect);
		pthread_mutex_lock(&mStrong);
		list_iterate(criterios[STRONG].memorias,(void*)loggearMetrics);
		pthread_mutex_unlock(&mStrong);
		pthread_mutex_unlock(&mLogMetrics);

		pthread_mutex_lock(&mLogMetrics);
		log_info(logMetrics, "METRICS: EVENTUAL\n"
				">tiempo en selects de EVENTUAL: %lu,\n>tiempo en inserts de EVENTUAL: %lu,\n"
				">cantidad inserts en EVENTUAL : %d,\n>cantidad selects en EVENTUAL : %d\n",
				eventual_tiempoSelect,eventual_tiempoInsert,eventual_cantidadInsert,eventual_cantidadSelect);
		pthread_mutex_lock(&mEventual);
		list_iterate(criterios[EVENTUAL].memorias,(void*)loggearMetrics);
		pthread_mutex_unlock(&mEventual);
		pthread_mutex_unlock(&mLogMetrics);
		return true;
	}
	else if (consolaOLog==1){
		printf("METRICS: HASH\n"
						">tiempo en selects de HASH: %lu,\n>tiempo en inserts de HASH: %lu,\n"
						">cantidad inserts en HASH : %d,\n>cantidad selects en HASH : %d\n",
						hash_tiempoSelect,hash_tiempoInsert,hash_cantidadInsert,hash_cantidadSelect);
		pthread_mutex_lock(&mHash);
		list_iterate(criterios[HASH].memorias,(void*)printearMetrics);
		pthread_mutex_unlock(&mHash);
		printf("METRICS: STRONG\n"
						">tiempo en selects de STRONG: %lu,\n>tiempo en inserts de STRONG: %lu,\n"
						">cantidad inserts en STRONG : %d,\n>cantidad selects en STRONG : %d\n",
						strong_tiempoSelect,strong_tiempoInsert,strong_cantidadInsert,strong_cantidadSelect);
		pthread_mutex_lock(&mStrong);
		list_iterate(criterios[STRONG].memorias,(void*)printearMetrics);
		pthread_mutex_unlock(&mStrong);
		printf("METRICS: EVENTUAL\n"
						">tiempo en selects de EVENTUAL: %lu,\n>tiempo en inserts de EVENTUAL: %lu,\n"
						">cantidad inserts en EVENTUAL : %d,\n>cantidad selects en EVENTUAL : %d\n",
						eventual_tiempoSelect,eventual_tiempoInsert,eventual_cantidadInsert,eventual_cantidadSelect);
		pthread_mutex_lock(&mEventual);
		list_iterate(criterios[EVENTUAL].memorias,(void*)printearMetrics);
		pthread_mutex_unlock(&mEventual);
		return true;
	}
	return 0;
}
void journal_consistencia(int consistencia){
	void realizarJournal(memoria * mem){
		pthread_mutex_lock(&mConexion);
		int socket = crearSocketCliente(mem->ip,mem->puerto);
		pthread_mutex_unlock(&mConexion);
		//log_info(kernel_configYLog->log, "@@ journal memoria: %d", mem->numero);
		enviarJournal(socket);
	}
	list_iterate(criterios[consistencia].memorias,(void*)realizarJournal);
}
bool kernel_add(char* operacion){
	char** opAux = string_n_split(operacion,5," ");
	int numero = atoi(*(opAux+2));
	memoria* mem;
	if((mem = encontrarMemoria(numero))){
		if(string_contains(*(opAux+4),"SHC")){
			agregarCriterioVerificandoSiLaTengo(mem,HASH,mHash);
			journal_consistencia(HASH);
			liberarParametrosSpliteados(opAux);
			return true;
		}
		else if(string_contains(*(opAux+4),"SC")){
			if(list_size(criterios[STRONG].memorias)==0){
				agregarCriterioVerificandoSiLaTengo(mem,STRONG,mStrong);
				liberarParametrosSpliteados(opAux);
				return true;
			}
			else{
				pthread_mutex_lock(&mLog);
				log_info(kernel_configYLog->log,"@ EXEC: %s.Ya se posee una memoria del tipo STRONG.", operacion);
				pthread_mutex_unlock(&mLog);
				liberarParametrosSpliteados(opAux);
				return false;
			}
		}
		else if(string_contains(*(opAux+4),"EC")){
			agregarCriterioVerificandoSiLaTengo(mem,EVENTUAL,mEventual);
			liberarParametrosSpliteados(opAux);
			return true;
		}
		pthread_mutex_lock(&mLog);
		log_info(kernel_configYLog->log,"@ EXEC: %s.Consistencia invalida.", operacion);
		pthread_mutex_unlock(&mLog);
		liberarParametrosSpliteados(opAux);
		return false;
	}
	else{
		liberarParametrosSpliteados(opAux);
		return false;
	}
}
bool kernel_memories(){
	void printearMemories(memoria* mem){
		printf(">MEMORIA %d IP %s PUERTO %s\n",mem->numero,mem->ip, mem->puerto);
	}
	//pthread_mutex_lock(&consola);
	printf("MEMORIES:\n");
	list_iterate(memorias,(void*)printearMemories);
	//pthread_mutex_unlock(&consola);
	return true;
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
void cancelThreadRR(){
	void realizarCancel(t_thread* t){
		pthread_cancel(*(t->thread));
	}
	pthread_mutex_lock(&mThread);
	list_iterate(rrThreads, (void*)realizarCancel);
	pthread_mutex_unlock(&mThread);
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
		//pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);
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
			thread_loggearInfo("EXEC",threadProcesador, pcb_auxiliar->operacion);
			if(kernel_api(pcb_auxiliar->operacion,threadProcesador)== false){
				thread_loggearInfo("@ FINISHED",threadProcesador, pcb_auxiliar->operacion);
				agregarALista(cola_proc_terminados, pcb_auxiliar,colaTerminados);
				//pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
				usleep(sleep);
				continue;
			}
			agregarALista(cola_proc_terminados, pcb_auxiliar,colaTerminados);
			thread_loggearInfo("FINISHED",threadProcesador, pcb_auxiliar->operacion);
			//pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
			usleep(sleep);
			continue;
		}
		else if(pcb_auxiliar->instruccion !=NULL){
			int ERROR = 0;
			for(int quantum=0;quantum<q;quantum++){
				if(pcb_auxiliar->ejecutado ==0){
					pcb_auxiliar->ejecutado=1;
					thread_loggearInfo("EXEC",threadProcesador, pcb_auxiliar->operacion);
					if(kernel_api(pcb_auxiliar->operacion,threadProcesador)==false){
//						thread_loggearInfo("@ EXEC",threadProcesador, pcb_auxiliar->operacion);
						ERROR = -1;
						break;
					}
					usleep(sleep);
					continue;
				}
				instruccion* instruc = list_find(pcb_auxiliar->instruccion,(void*)instruccion_no_ejecutada);
				if (instruc == NULL){
					break;
				}
				instruc->ejecutado = 1;
				thread_loggearInfoInstruccion("EXEC",threadProcesador, pcb_auxiliar->operacion, instruc->operacion);
				if(kernel_api(instruc->operacion, threadProcesador)==false){
					ERROR = -1;
					break;
				}
				//thread_loggearInfo("EXEC",threadProcesador, pcb_auxiliar->operacion);
				usleep(sleep);
			}
			if(list_any_satisfy(pcb_auxiliar->instruccion,(void*)instruccion_no_ejecutada) && ERROR !=-1){
				thread_loggearInfo("NEW",threadProcesador, pcb_auxiliar->operacion);
				agregarALista(cola_proc_listos, pcb_auxiliar,colaListos);
				sem_post(&hayReady);
				//pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
				usleep(sleep);
				continue;
			}
			else if(ERROR ==-1){
				thread_loggearInfo("@ FINISHED",threadProcesador, pcb_auxiliar->operacion);
				agregarALista(cola_proc_terminados, pcb_auxiliar,colaTerminados);
				//pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
				usleep(sleep);
				continue;
			}
			else{
				agregarALista(cola_proc_terminados, pcb_auxiliar,colaTerminados);
				thread_loggearInfo("FINISHED",threadProcesador, pcb_auxiliar->operacion);
				//pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
				usleep(sleep);
				continue;
			}

		}
	}
}
// ---------------.: THREAD CONSOLA A NEW :.---------------
void kernel_almacenar_en_new(char*operacion){
	agregarALista(cola_proc_nuevos,operacion,colaNuevos);
	sem_post(&hayNew);
	string_to_upper(operacion);
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log, " NEW: %s", operacion);
	pthread_mutex_unlock(&mLog);
	//pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
}
void kernel_consola(){

	printf(">> ¡Welcome to Kernel! Ingrese alguna de las siguientes operaciones: \n"
			"> SELECT [TABLA] [KEY]\n"
			"> INSERT [TABLA] [KEY] \"[VALUE]\" [TIMESTAMP] \n"
			"> CREATE [TABLA] [SC/SHC/EC] [NUMERO_PARTICIONES] [COMPACTATION_TIME]\n"
			"> DESCRIBE [TABLA] \n"
			"> DROP [TABLA]\n"
			"> METRICS\n"
			"> JOURNAL\n"
			"> ADD MEMORY [NUMERO] TO [SC/SHC/EC]\n"
			"> DROP [NOMBRE_TABLA]\n"
			"> RUN [PATH_ARCHIVO]\n"
			"> CERRAR\n"
			"> Y siga su ejecucion mediante el archivo Kernel.log\n");

	char* linea= NULL;
	while(!destroy){
		//pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);

		printf(">");
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
		//pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);
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
				string_contains(operacion, "METRICS") || string_contains(operacion, "ADD")
				|| string_contains(operacion, "CERRAR")|| string_contains(operacion, "MEMORIES")){
			kernel_crearPCB(operacion);
		}
		else{
			pthread_mutex_lock(&mLog);
			log_info(kernel_configYLog->log,"@ NEW: %s Operacion Invalida", operacion);
			pthread_mutex_unlock(&mLog);
		}
		//pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
	}
}
void kernel_run(char* operacion){
	char** opYArg;
	opYArg = string_n_split(operacion ,2," ");
	string_to_lower(*(opYArg+1));
	FILE *archivoALeer;
	if ((archivoALeer= fopen((*(opYArg+1)), "r")) == NULL){
		pthread_mutex_lock(&mLog);
		log_info(kernel_configYLog->log,"@ EXEC: %s %s. (Consejo: verifique existencia del archivo)", *opYArg, *(opYArg+1) ); //operacion);
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
bool kernel_api(char* operacionAParsear, int thread){
	if(esOperacionEjecutable(operacionAParsear)){
		if(string_contains(operacionAParsear, "INSERT")) {
			return kernel_insert(operacionAParsear,thread);
		}
		else if (string_contains(operacionAParsear, "SELECT")) {
			return kernel_select(operacionAParsear,thread);
		}
		else if (string_contains(operacionAParsear, "MEMORIES")) {
			return kernel_memories();
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
			return kernel_add(operacionAParsear);
		}
		else if (string_contains(operacionAParsear, "JOURNAL")) {
			return kernel_journal();
		}
		else if (string_contains(operacionAParsear, "METRICS")) {
			return kernel_metrics(1);
		}
		else if (string_contains(operacionAParsear, "CERRAR")) {
			kernel_semFinalizar();
			return true;
		}
		else
			return false;
	}
	else
		return false;
}
#endif /* KER_OPERACIONES_H_ */
