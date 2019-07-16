#ifndef KER_AUXILIARES_H_
#define KER_AUXILIARES_H_

#include "ker_structs.h"


/******************************IMPLEMENTACIONES******************************************/
//------ MEGA AUXILIARES ---------
void actualizarTiemposInsert(int index, float tiempo){
	if(index == STRONG){
		pthread_mutex_lock(&mStrong);
		criterios[index].tiempoInserts += tiempo; //((double)tiempo)/CLOCKS_PER_SEC;
		pthread_mutex_unlock(&mStrong);
	}
	else if(index == HASH){
		pthread_mutex_lock(&mHash);
		criterios[index].tiempoInserts += tiempo; //((double)tiempo)/CLOCKS_PER_SEC;
		pthread_mutex_unlock(&mHash);
	}
	else if(index == EVENTUAL){
		pthread_mutex_lock(&mEventual);
		criterios[index].tiempoInserts += tiempo; //((double)tiempo)/CLOCKS_PER_SEC;
		pthread_mutex_unlock(&mEventual);
	}
}
void actualizarTiemposSelect(int index, float tiempo){
	if(index == STRONG){
		pthread_mutex_lock(&mStrong);
		criterios[index].tiempoSelects += tiempo; //((double)tiempo)/CLOCKS_PER_SEC;
		pthread_mutex_unlock(&mStrong);
	}
	else if(index == HASH){
		pthread_mutex_lock(&mHash);
		criterios[index].tiempoSelects += tiempo; //((double)tiempo)/CLOCKS_PER_SEC;
		pthread_mutex_unlock(&mHash);
	}
	else if(index == EVENTUAL){
		pthread_mutex_lock(&mEventual);
		criterios[index].tiempoSelects += tiempo; //((double)tiempo)/CLOCKS_PER_SEC;
		pthread_mutex_unlock(&mEventual);
	}
}
void freeMemoria(memoria* mem3){
	free(mem3->ip);
	free(mem3->puerto);
	free(mem3);
}
int random_int(int min, int max)
{
	return min + rand() % (max - min);
}
void guardarMemorias(seed* unaSeed){
	memoria* memAux = malloc(sizeof(memoria));
	memAux->numero = unaSeed->numero;
	memAux->puerto = string_duplicate(unaSeed->puerto);
	memAux->ip = string_duplicate(unaSeed->ip);
	memAux->cantidadIns = 0;
	memAux->cantidadSel = 0;
	agregarMemoriaVerificandoSiLaTengo(memAux);
}
void agregarMemoriaVerificandoSiLaTengo(memoria* memAux){
	bool yaGuardeMemoria(memoria* mem){
		return mem->numero == memAux->numero;
	}
	pthread_mutex_lock(&mMemorias);
	bool boleanFind = list_find(memorias,(void*)yaGuardeMemoria);
	pthread_mutex_unlock(&mMemorias);
	if(!boleanFind){
		agregarALista(memorias,memAux,mMemorias);
	}
	else
		liberarMemoria(memAux);
}
void agregarCriterioVerificandoSiLaTengo(memoria* memAux,int index,pthread_mutex_t semaphore){
	bool yaGuardeMemoria(memoria* mem){
		return mem->numero == memAux->numero;
	}
	pthread_mutex_lock(&semaphore);
	bool boleanFind = list_find(criterios[index].memorias,(void*)yaGuardeMemoria);
	pthread_mutex_unlock(&semaphore);
	if(!boleanFind){
		agregarALista(criterios[index].memorias,memAux,mMemorias);
	}
}
void actualizarListaMetadata(metadata* met){
	tabla* t = malloc(sizeof(tabla));
	pthread_mutex_lock(&mLogResultados);
	log_info(logResultados, " [R] DESCRIBE %s %d %d %d", met->nombreTabla,
			met->tipoConsistencia, met->cantParticiones, met->tiempoCompactacion);
	pthread_mutex_unlock(&mLogResultados);
	t->nombreDeTabla = string_duplicate(met->nombreTabla);
	t->consistenciaDeTabla = met->tipoConsistencia;
	agregarTablaVerificandoSiLaTengo(t);
}
//------ TIMED ---------
void kernel_gossiping(){
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);
	while(destroy==0){
		pthread_mutex_lock(&mConexion);
		int socket = crearSocketCliente(ipMemoria,puertoMemoria);
		pthread_mutex_unlock(&mConexion);
		if(socket==-1){
			pthread_mutex_lock(&mLog);
			log_info(kernel_configYLog->log, "@@ Gossip no se pudo realizar");
			pthread_mutex_unlock(&mLog);
			pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
			usleep(timedGossip*1000);
			continue;
		}
		operacionProtocolo protocoloGossip = TABLAGOSSIP;
		enviar(socket,(void*)&protocoloGossip, sizeof(operacionProtocolo));
		t_list* tabla = list_create();
		serializarYEnviarTablaGossip(socket,tabla);
		recibirYDeserializarTablaDeGossipRealizando(socket,guardarMemorias);
		free(tabla);
		cerrarConexion(socket);
		pthread_mutex_lock(&mLog);
		log_info(kernel_configYLog->log, "@@ Gossip hecho");
		pthread_mutex_unlock(&mLog);
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
		usleep(timedGossip*1000);
	}

}
void describeTimeado(){
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);
	while(destroy==0){
		operacionLQL* opAux = malloc(sizeof(operacionLQL));
		opAux ->operacion = string_duplicate("DESCRIBE");
		opAux->parametros = string_duplicate("ALL");
		pthread_mutex_lock(&mConexion);
		int socket = crearSocketCliente(ipMemoria,puertoMemoria);
		pthread_mutex_unlock(&mConexion);
		if(socket != -1){
			serializarYEnviarOperacionLQL(socket, opAux);
			void* bufferProtocolo = recibir(socket);
			if(bufferProtocolo == NULL){
				pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
				usleep(metadataRefresh*1000);
				pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);
			}
			operacionProtocolo protocolo = empezarDeserializacion(&bufferProtocolo);
			if(protocolo == METADATA){
				pthread_mutex_lock(&mTablas);
				list_clean_and_destroy_elements(tablas,(void*)liberarTabla);
				//tablas = list_create();
				pthread_mutex_unlock(&mTablas);
				metadata * met = deserializarMetadata(bufferProtocolo);
				actualizarListaMetadata(met);
				pthread_mutex_lock(&mLog);
				log_info(kernel_configYLog->log, " @@ Describe realizado");
				pthread_mutex_unlock(&mLog);
			}
			else if(protocolo == PAQUETEMETADATAS){
				pthread_mutex_lock(&mTablas);
				list_clean_and_destroy_elements(tablas,(void*)liberarTabla);
				//tablas = list_create();
				pthread_mutex_unlock(&mTablas);
				recibirYDeserializarPaqueteDeMetadatasRealizando(socket, actualizarListaMetadata);
				pthread_mutex_lock(&mLog);
				log_info(kernel_configYLog->log, " @@ Describe realizado");
				pthread_mutex_unlock(&mLog);
			}
			else if(protocolo == ERROR){
				pthread_mutex_lock(&mLog);
				log_info(kernel_configYLog->log, "@@ Error: Describe timeado");
				pthread_mutex_unlock(&mLog);
			}

			cerrarConexion(socket);
			free(bufferProtocolo);
		}
		liberarOperacionLQL(opAux);
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
		usleep(metadataRefresh*1000);
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);
	}
}
//------ ENVIOS Y SOCKETS ---------
int enviarOperacion(operacionLQL* opAux,int index, int thread){
	int socket = obtenerSocketAlQueSeEnvio(opAux,index);
	if(socket != -1){
		char* recibido = (char*) recibir(socket);
		if(recibido == NULL){
			thread_loggearInfo("@ RECIBIDO",thread, "DESCONEXION/ERROR EN MEMORIA");
			cerrarConexion(socket);
			return 1;
		}
		if(recibidoContiene(recibido, "ERROR")){
			thread_loggearInfo("@ RECIBIDO",thread, recibido);
			free(recibido);
			cerrarConexion(socket);
			return -1;
		}
		else{
			while(recibidoContiene(recibido, "FULL")){
				enviarJournal(socket);
				serializarYEnviarOperacionLQL(socket, opAux);
				free(recibido);
				recibido = NULL;
				recibido = (char*) recibir(socket);
				if(recibidoContiene(recibido, "ERROR")){
					thread_loggearInfo("@ RECIBIDO",thread, recibido);
					free(recibido);
					cerrarConexion(socket);
					return -1;
				}
			}
			if(string_contains(opAux->operacion,"SELECT")){
				pthread_mutex_lock(&mLogResultados);
				log_info(logResultados, " [R] %s", recibido);
				pthread_mutex_unlock(&mLogResultados);
			}
			thread_loggearInfoYLiberarParametrosRECIBIDO(thread,recibido,opAux);
			cerrarConexion(socket);
			return 1;
		}
	}
	else{
		pthread_mutex_lock(&mLog);
		log_error(kernel_configYLog->log, "@ CONEXION[%d]: %s %s",thread, opAux->operacion, opAux->parametros);
		pthread_mutex_unlock(&mLog);
		liberarOperacionLQL(opAux);
		return -1;
	}
}
int strong_obtenerSocketAlQueSeEnvio(operacionLQL* opAux){
	int socket = -1;
	bool pudeConectarYEnviar(memoria* mem){
		pthread_mutex_lock(&mConexion);
		socket = crearSocketCliente(mem->ip,mem->puerto);
		pthread_mutex_unlock(&mConexion);
		if(socket){
			if(string_contains(opAux->operacion,"INSERT")){
				mem->cantidadIns ++;

			}
			else if(string_contains(opAux->operacion,"SELECT")){
				mem->cantidadSel ++;
				pthread_mutex_lock(&mLogResultados);
				log_info(logResultados, " [E] %s %s", opAux->operacion, opAux->parametros);
				pthread_mutex_unlock(&mLogResultados);
			}
			serializarYEnviarOperacionLQL(socket, opAux);
			pthread_mutex_lock(&mLog);
			log_info(kernel_configYLog->log, " ENVIADO: %s %s", opAux->operacion, opAux->parametros);
			pthread_mutex_unlock(&mLog);
			return true;
		}
		else{
			bool memoriaASacar(memoria* mem2){
				return mem2->numero == mem->numero;
			}
			pthread_mutex_lock(&mStrong);
		 	list_remove_and_destroy_by_condition(criterios[STRONG].memorias,(void*)memoriaASacar, (void*)freeMemoria);
		 	pthread_mutex_unlock(&mStrong);
			pthread_mutex_lock(&mMemorias);
		 	list_remove_and_destroy_by_condition(memorias,(void*)memoriaASacar, (void*)freeMemoria);
		 	pthread_mutex_unlock(&mMemorias);
		 	return false;
		}
	}
	pthread_mutex_lock(&mStrong);
	list_find(criterios[STRONG].memorias,(void*)pudeConectarYEnviar);
	if(string_contains(opAux->operacion,"INSERT")){
		criterios[STRONG].cantidadInserts ++;
	}
	else if(string_contains(opAux->operacion,"SELECT")){
		criterios[STRONG].cantidadSelects ++;
	}
	pthread_mutex_unlock(&mStrong);
	return socket;
}
int hash_obtenerSocketAlQueSeEnvio(operacionLQL* opAux){
	int socket = -1;
	int tamLista;
	memoria* mem;
	if(string_equals_ignore_case(opAux->operacion, "SELECT") || string_equals_ignore_case(opAux->operacion, "INSERT")){
		char** operacion = string_n_split(opAux->parametros,3," ");
		pthread_mutex_lock(&mHash);
		tamLista = list_size(criterios[HASH].memorias);
		pthread_mutex_unlock(&mHash);
		int indice = atoi(*(operacion+1)) % tamLista;
		pthread_mutex_lock(&mHash);
		mem = list_get(criterios[HASH].memorias,indice);
		pthread_mutex_unlock(&mHash);
		if(string_contains(opAux->operacion,"INSERT")){
			criterios[HASH].cantidadInserts ++;
		}
		else if(string_contains(opAux->operacion,"SELECT")){
			criterios[HASH].cantidadSelects ++;
		}
	}
	else{
		pthread_mutex_lock(&mHash);
		mem = list_get(criterios[HASH].memorias,0);
		pthread_mutex_unlock(&mHash);
	}
	if(mem==NULL)
		return socket;
	pthread_mutex_lock(&mConexion);
	socket = crearSocketCliente(mem->ip,mem->puerto);
	pthread_mutex_unlock(&mConexion);
	if(socket){
		if(string_contains(opAux->operacion,"INSERT")){
			mem->cantidadIns += 1;
			pthread_mutex_lock(&mLogResultados);
			log_info(logResultados, " [E] %s %s", opAux->operacion, opAux->parametros);
			pthread_mutex_unlock(&mLogResultados);
		}
		else if(string_contains(opAux->operacion,"SELECT")){
			mem->cantidadSel += 1;
			pthread_mutex_lock(&mLogResultados);
			log_info(logResultados, " [E] %s %s", opAux->operacion, opAux->parametros);
			pthread_mutex_unlock(&mLogResultados);
		}
		serializarYEnviarOperacionLQL(socket, opAux);
		pthread_mutex_lock(&mLog);
		log_info(kernel_configYLog->log, " ENVIADO: %s %s", opAux->operacion, opAux->parametros);
		pthread_mutex_unlock(&mLog);
	}
	else{
		bool memoriaASacar(memoria* mem2){
			return mem2->numero == mem->numero;
		}
		pthread_mutex_lock(&mHash);
	 	list_remove_and_destroy_by_condition(criterios[HASH].memorias,(void*)memoriaASacar, (void*)freeMemoria);
	 	pthread_mutex_unlock(&mHash);
		pthread_mutex_lock(&mMemorias);
	 	list_remove_and_destroy_by_condition(memorias,(void*)memoriaASacar, (void*)freeMemoria);
	 	pthread_mutex_unlock(&mMemorias);
	}
	return socket;
}
int eventual_obtenerSocketAlQueSeEnvio(operacionLQL* opAux){
	int socket = -1;
	int tamLista;
	memoria* mem;
	pthread_mutex_lock(&mEventual);
	tamLista = list_size(criterios[EVENTUAL].memorias);
	if(string_contains(opAux->operacion,"INSERT")){
		criterios[EVENTUAL].cantidadInserts ++;
	}
	else if(string_contains(opAux->operacion,"SELECT")){
		criterios[EVENTUAL].cantidadSelects ++;
	}
	pthread_mutex_unlock(&mEventual);
	while(socket==-1 && tamLista >0){
		int rand = random_int(0,tamLista);
		pthread_mutex_lock(&mEventual);
		mem = list_get(criterios[EVENTUAL].memorias, rand);
		pthread_mutex_unlock(&mEventual);
		pthread_mutex_lock(&mConexion);
		socket = crearSocketCliente(mem->ip,mem->puerto);
		pthread_mutex_unlock(&mConexion);
		if(socket){
			if(string_contains(opAux->operacion,"INSERT")){
				mem->cantidadIns += 1;
				pthread_mutex_lock(&mLogResultados);
				log_info(logResultados, " [E] %s %s", opAux->operacion, opAux->parametros);
				pthread_mutex_unlock(&mLogResultados);
			}
			else if(string_contains(opAux->operacion,"SELECT")){
				mem->cantidadSel += 1;
				pthread_mutex_lock(&mLogResultados);
				log_info(logResultados, " [E] %s %s", opAux->operacion, opAux->parametros);
				pthread_mutex_unlock(&mLogResultados);
			}
			serializarYEnviarOperacionLQL(socket, opAux);
			pthread_mutex_lock(&mLog);
			log_info(kernel_configYLog->log, " ENVIADO: %s %s", opAux->operacion, opAux->parametros);
			pthread_mutex_unlock(&mLog);
			break;
		}
		else{
			bool memoriaASacar(memoria* mem2){
				return mem2->numero == mem->numero;
			}
			pthread_mutex_lock(&mEventual);
		 	list_remove_and_destroy_by_condition(criterios[EVENTUAL].memorias,(void*)memoriaASacar, (void*)freeMemoria);
		 	pthread_mutex_unlock(&mEventual);
			pthread_mutex_lock(&mMemorias);
		 	list_remove_and_destroy_by_condition(memorias,(void*)memoriaASacar, (void*)freeMemoria);
		 	pthread_mutex_unlock(&mMemorias);
			return false;
		}
		pthread_mutex_lock(&mEventual);
		tamLista = list_size(criterios[EVENTUAL].memorias);
		pthread_mutex_unlock(&mEventual);
	}
	return socket;
}
int obtenerSocketAlQueSeEnvio(operacionLQL* opAux, int index){
	if(index == EVENTUAL){
		return eventual_obtenerSocketAlQueSeEnvio(opAux);
	}
	else if(index == STRONG){
		return strong_obtenerSocketAlQueSeEnvio(opAux);
	}
	else if (index == HASH){
		return hash_obtenerSocketAlQueSeEnvio(opAux);
	}
	return -1;
}
int obtenerIndiceDeConsistencia(consistencia unaConsistencia){
	if(unaConsistencia == SC){
		return STRONG;
	}
	else if(unaConsistencia == EC){
		return EVENTUAL;
	}
	else if(unaConsistencia == SHC){
		return HASH;
	}
	return -1;
}
//------ OPERACIONES LQL ---------
void enviarJournal(int socket){
	operacionLQL* opAux=splitear_operacion("JOURNAL");
	serializarYEnviarOperacionLQL(socket, opAux);
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log, " ENVIADO: JOURNAL");
	pthread_mutex_unlock(&mLog);
	char* recibido = (char*) recibir(socket);
	if(recibido == NULL){
		pthread_mutex_lock(&mLog);
		log_info(kernel_configYLog->log, "@ RECIDIBO:DESCONEXION/ERROR EN MEMORIA");
		pthread_mutex_unlock(&mLog);
		free(recibido);
		liberarOperacionLQL(opAux);
		return;
	}
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log, "RECIBIDO: %s",recibido);
	pthread_mutex_unlock(&mLog);
	free(recibido);
	liberarOperacionLQL(opAux);
	//cerrarConexion(socket);
}
//------ INSTRUCCIONES DE PCB ---------
bool instruccion_no_ejecutada(instruccion* instruc){
	return instruc->ejecutado==0;
}
//------ TABLAS ---------
void guardarTablaCreada(char* parametros){
	char** opAux =string_n_split(parametros,3," ");
	tabla* tablaAux = malloc(sizeof(tabla));
	tablaAux->nombreDeTabla= string_duplicate(*opAux);
	if(string_equals_ignore_case(*(opAux+1),"SC")){
		tablaAux->consistenciaDeTabla = SC;
	}
	else if(string_equals_ignore_case(*(opAux+1),"SHC")){
		tablaAux->consistenciaDeTabla = SHC;
	}
	else if(string_equals_ignore_case(*(opAux+1),"EC")){
		tablaAux->consistenciaDeTabla = EC;
	}
	agregarTablaVerificandoSiLaTengo(tablaAux);
	liberarParametrosSpliteados(opAux); //cambie esto
}
void agregarTablaVerificandoSiLaTengo(tabla* t){
	bool yaGuardeTabla(tabla* tab){
		return string_equals_ignore_case(t->nombreDeTabla,tab->nombreDeTabla);// && t->consistenciaDeTabla == tab->consistenciaDeTabla;
	}
	pthread_mutex_lock(&mTablas);
	bool boleanFind = list_find(tablas,(void*)yaGuardeTabla);
	pthread_mutex_unlock(&mTablas);
	if(!boleanFind){
		agregarALista(tablas,t,mTablas);
	}
	else{
		liberarTabla(t);
	}
}
void eliminarTablaCreada(char* parametros){
	bool tablaDeNombre(tabla* t){
			if(string_equals_ignore_case(t->nombreDeTabla,parametros)){
				free(t->nombreDeTabla);
				free(t);
				return true;
			}
			return false;
		}
	pthread_mutex_lock(&mTablas);
	/*tabla* aBorrar = */list_remove_by_condition(tablas, (void*)tablaDeNombre); //todo
	pthread_mutex_unlock(&mTablas);

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
	pthread_mutex_lock(&mMemorias);
	memoria* memory = (memoria*) list_find(memorias, (void*)memoriaEsNumero);
	pthread_mutex_unlock(&mMemorias);
	return memory;
}
//------ CRITERIOS ---------
consistencia encontrarConsistenciaDe(char* nombreTablaBuscada){
	consistencia c = -1;
	bool encontrarTabla(tabla* t){
		return string_equals_ignore_case(t->nombreDeTabla, nombreTablaBuscada);
	}
	pthread_mutex_lock(&mTablas);
	tabla* retorno =(tabla*) list_find(tablas,(void*)encontrarTabla);
	pthread_mutex_unlock(&mTablas);
	if(retorno){
		c = retorno->consistenciaDeTabla;
	}
	return c;
}
//------ ERRORES ---------
bool recibidoContiene(char* recibido, char* contiene){
	string_to_upper(recibido);
	return string_contains(recibido, contiene);
}
//------ CERRAR ---------
void kernel_semFinalizar() {
	sem_post(&finalizar);
	destroy = 1;
}
//----------------- LOGS -----------------------------
void thread_loggearInfoYLiberarParametrosRECIBIDO(int thread,char* recibido, operacionLQL *opAux){
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log," RECIBIDO[%d]: %s",thread, recibido);
	pthread_mutex_unlock(&mLog);
	free(recibido);
	liberarOperacionLQL(opAux);
}
void thread_loggearInfo(char* estado, int threadProcesador, char* operacion){
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log," %s[%d]: %s",estado,threadProcesador, operacion);
	pthread_mutex_unlock(&mLog);
}
void thread_loggearInfoInstruccion(char* estado, int threadProcesador,char* archivoRun, char* operacion){
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log," %s[%d] de %s, %s",estado,threadProcesador,archivoRun, operacion);
	pthread_mutex_unlock(&mLog);
}
//------ LISTAS ---------

void agregarALista(t_list* lista, void* elemento, pthread_mutex_t semaphore){
	pthread_mutex_lock(&semaphore);
	list_add(lista,elemento);
	pthread_mutex_unlock(&semaphore);
}

#endif /* KER_AUXILIARES_H_ */
