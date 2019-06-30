#ifndef KER_AUXILIARES_H_
#define KER_AUXILIARES_H_
#include <commons/log.h>
#include <commons/config.h>
#include <commons/collections/list.h>
#include <commons/string.h>
#include <commonsPropias/serializacion.h>
#include <commonsPropias/conexiones.h>
#include <stdlib.h>
#include <semaphore.h>
#include <stdbool.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <unistd.h>
#include "ker_structs.h"

/******************************DECLARACIONES******************************************/
bool recibidoContiene(char* recibido, char* contiene);
bool instruccion_no_ejecutada(instruccion* instruc);

void describeTimeado();
void kernel_destroy();
void loggearErrorYLiberarParametrosEXEC(char* recibido, operacionLQL *opAux);
void loggearInfoYLiberarParametrosEXEC(char* recibido, operacionLQL *opAux);
//void thread_loggearErrorEXEC(char* estado, int threadProcesador, char* operacion);
void thread_loggearInfoEXEC(char* estado, int threadProcesador, char* operacion);
void agregarALista(t_list* lista, void* elemento, pthread_mutex_t semaphore);
void guardarTablaCreada(char* parametros);
void eliminarTablaCreada(char* parametros);
void enviarJournal(int socket);
void guardarDatos(seed* unaSeed);

int socketMemoriaSolicitada(consistencia criterio);
int obtenerIndiceDeConsistencia(consistencia unaConsistencia);
int strong_obtenerSocketAlQueSeEnvio(operacionLQL* opAux);
int eventual_obtenerSocketAlQueSeEnvio(operacionLQL* opAux);
int hash_obtenerSocketAlQueSeEnvio(operacionLQL* opAux);
int obtenerSocketAlQueSeEnvio(operacionLQL* opAux, int index);
int enviarOperacion(operacionLQL* opAux,int index);
int random_int(int min, int max);

tabla* encontrarTablaPorNombre(char* nombre);

memoria* encontrarMemoria(int numero);

consistencia encontrarConsistenciaDe(char* nombreTablaBuscada);

/******************************IMPLEMENTACIONES******************************************/
void kernel_gossiping(){ //TODO preguntar si puedo conectar con cualquier memoria o tiene que estar asignada a criterio
	int tamLista;
	memoria* mem;
	while(!destroy){
		pthread_mutex_lock(&mMemorias);
		tamLista = list_size(memorias);
		pthread_mutex_unlock(&mMemorias);
		int memoria = random_int(0,tamLista);
		pthread_mutex_lock(&mMemorias);
		mem = list_get(memorias,memoria);
		pthread_mutex_unlock(&mMemorias);
		int socket = crearSocketCliente(mem->ip,mem->puerto);
		if(socket==-1){
					continue;
				}
		operacionProtocolo protocoloGossip = TABLAGOSSIP;
		enviar(socket,(void*)&protocoloGossip, sizeof(operacionProtocolo));
		recibirYDeserializarTablaDeGossipRealizando(socket,guardarDatos);
		usleep(timedGossip*1000);
	}

}
void guardarDatos(seed* unaSeed){
	memoria* memAux = malloc(sizeof(memoria));
	memAux->numero = unaSeed->numero;
	memAux->puerto = string_duplicate(unaSeed->puerto);
	memAux->ip = string_duplicate(unaSeed->ip);
	agregarALista(memorias,memAux,mMemorias);
}
void actualizarListaMetadata(metadata* met){
	tabla* t = malloc(sizeof(tabla));
	bool tablaYaGuardada(tabla* t){
		return string_equals_ignore_case(t->nombreDeTabla,met->nombreTabla);
	}
	if(list_any_satisfy(tablas,(void*)tablaYaGuardada)){
		liberarMetadata(met);
		return;
	}
	t->nombreDeTabla = string_duplicate(met->nombreTabla);
	t->consistenciaDeTabla = met->tipoConsistencia;
	agregarALista(tablas,t,mTablas);
	liberarMetadata(met);
}
void describeTimeado(){
	operacionLQL* opAux = malloc(sizeof(operacionLQL));
	opAux ->operacion = malloc(sizeof("DESCRIBE"));
	opAux ->operacion = "DESCRIBE";
	opAux->parametros = "ALL";
	while(!destroy){
		int socket = obtenerSocketAlQueSeEnvio(opAux,EVENTUAL);
		if(socket != -1){
			recibirYDeserializarPaqueteDeMetadatasRealizando(socket, actualizarListaMetadata);
			pthread_mutex_lock(&mLog);
			log_info(kernel_configYLog->log, " RECIBIDO[TIMED]: Describe realizado");
			pthread_mutex_unlock(&mLog);
			cerrarConexion(socket);
		}
		usleep(metadataRefresh*1000);
	}
	liberarOperacionLQL(opAux);

}
int enviarOperacion(operacionLQL* opAux,int index){
	int socket = obtenerSocketAlQueSeEnvio(opAux,index);
	if(socket != -1){
		//serializarYEnviarOperacionLQL(socket, opAux);
		char* recibido = (char*) recibir(socket);
		if(recibido == NULL){
			return -1;
		}
		if(recibidoContiene(recibido, "ERROR")){
			loggearErrorYLiberarParametrosEXEC(recibido,opAux);
			cerrarConexion(socket);
			return -1;
		}
		else{
			while(recibidoContiene(recibido, "FULL")){
				enviarJournal(socket);
				serializarYEnviarOperacionLQL(socket, opAux);
				recibido = (char*) recibir(socket);
			}
			loggearInfoYLiberarParametrosEXEC(recibido,opAux);
			cerrarConexion(socket);
			return 1;
		}
	}
	else{
		pthread_mutex_lock(&mLog);
		log_error(kernel_configYLog->log, "ERROR: No hay memorias para enviar la request %s %s", opAux->operacion, opAux->parametros);
		pthread_mutex_unlock(&mLog);
		return -1;
	}
}
void freeMemoria(memoria* mem3){
	free(mem3->ip);
	free(mem3->puerto);
	free(mem3);
}

int strong_obtenerSocketAlQueSeEnvio(operacionLQL* opAux){
	int socket = -1;
	bool pudeConectarYEnviar(memoria* mem){
		if((socket = crearSocketCliente(mem->ip,mem->puerto))){
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

		 	list_remove_and_destroy_by_condition(criterios[STRONG].memorias,(void*)memoriaASacar, (void*)freeMemoria);
			return false;
		}
	}
	list_find(criterios[STRONG].memorias,(void*)pudeConectarYEnviar);
	return socket;
}
int hash_obtenerSocketAlQueSeEnvio(operacionLQL* opAux){
	int socket = -1;
	char** operacion = string_n_split(opAux->parametros,2," ");
	int tamLista;
	memoria* mem;
	pthread_mutex_lock(&mEventual);
	tamLista = list_size(criterios[HASH].memorias);
	pthread_mutex_unlock(&mEventual);
	int indice = atoi(*operacion) % tamLista;
	pthread_mutex_lock(&mEventual);
	mem = list_get(criterios[HASH].memorias,indice);
	pthread_mutex_unlock(&mEventual);

	if((socket = crearSocketCliente(mem->ip,mem->puerto))){
		serializarYEnviarOperacionLQL(socket, opAux);
		pthread_mutex_lock(&mLog);
		log_info(kernel_configYLog->log, " ENVIADO: %s %s", opAux->operacion, opAux->parametros);
		pthread_mutex_unlock(&mLog);
	}
	else{
		bool memoriaASacar(memoria* mem2){
			return mem2->numero == mem->numero;
		}
		list_remove_and_destroy_by_condition(criterios[STRONG].memorias,(void*)memoriaASacar, (void*)freeMemoria);
	}
	return socket;
}
int random_int(int min, int max)
{
   return min + rand() % (max - min);
}
int eventual_obtenerSocketAlQueSeEnvio(operacionLQL* opAux){ //todo liberar split
	int socket = -1;
	int tamLista;
	memoria* mem;
	pthread_mutex_lock(&mEventual);
	tamLista = list_size(criterios[EVENTUAL].memorias);
	pthread_mutex_unlock(&mEventual);
	while(socket==-1 && tamLista >0){
		int rand = random_int(0,tamLista);
		pthread_mutex_lock(&mEventual);
		mem = list_get(criterios[EVENTUAL].memorias, rand);
		pthread_mutex_unlock(&mEventual);
		if((socket = crearSocketCliente(mem->ip,mem->puerto))){
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

		 	list_remove_and_destroy_by_condition(criterios[EVENTUAL].memorias,(void*)memoriaASacar, (void*)freeMemoria);
			return false;
		}
		pthread_mutex_lock(&mEventual);
		tamLista = list_size(criterios[EVENTUAL].memorias);
		pthread_mutex_unlock(&mEventual);
	}
	return socket;
}
int obtenerSocketAlQueSeEnvio(operacionLQL* opAux, int index){
	if(index == EVENTUAL)
		return eventual_obtenerSocketAlQueSeEnvio(opAux);
	else if(index == STRONG)
		return strong_obtenerSocketAlQueSeEnvio(opAux);
	else if (index == HASH)
		return hash_obtenerSocketAlQueSeEnvio(opAux);
	return -1;
}
int obtenerIndiceDeConsistencia(consistencia unaConsistencia){
	if(unaConsistencia == SC){
		return STRONG;
	}
	else if(unaConsistencia == EC){
		return EVENTUAL;
	}
	else if(unaConsistencia == SH){
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
	loggearInfoYLiberarParametrosEXEC(recibido,opAux);
}
//------ INSTRUCCIONES DE PCB ---------
bool instruccion_no_ejecutada(instruccion* instruc){
	return instruc->ejecutado==0;
}
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
	//todo liberar opAux
}
void eliminarTablaCreada(char* parametros){
	//tablaAux = malloc(sizeof(tabla));
	bool tablaDeNombre(tabla* t){
			return t->nombreDeTabla == parametros;
		}
	tabla* tablaAux = list_remove_by_condition(tablas, (void*)tablaDeNombre);
	//free(tablaAux->nombreDeTabla);
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
	//memoria * memory = malloc(sizeof(memoria));
	memoria* memory = (memoria*) list_find(memorias, (void*)memoriaEsNumero);
	return memory;
}
//memoria* encontrarMemoriaStrong(){
//	return list_get(criterios[STRONG].memorias, 0);
//}
//------ CRITERIOS ---------
consistencia encontrarConsistenciaDe(char* nombreTablaBuscada){
	consistencia c = -1;
	bool encontrarTabla(tabla* t){
		return string_equals_ignore_case(t->nombreDeTabla, nombreTablaBuscada);
	}
	tabla* retorno =(tabla*) list_find(tablas,(void*)encontrarTabla);
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
void kernel_destroy(){
	destroy = 1;
}
void kernel_semFinalizar() {
	sem_post(&finalizar);
}
//----------------- LOGS -----------------------------
void loggearErrorYLiberarParametrosEXEC(char* recibido, operacionLQL *opAux){
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log, "@ RECIBIDO: %s", recibido); //todo cambiar
	pthread_mutex_unlock(&mLog);
	free(recibido);
	liberarOperacionLQL(opAux);
}
void loggearInfoYLiberarParametrosEXEC(char* recibido, operacionLQL *opAux){
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log, " RECIBIDO: %s", recibido);
	pthread_mutex_unlock(&mLog);
	free(recibido);
	liberarOperacionLQL(opAux);
}
//void thread_loggearErrorEXEC(char* estado, int threadProcesador, char* operacion){
//	pthread_mutex_lock(&mLog);
//	log_error(kernel_configYLog->log,"@ %s[%d]: %s",estado,threadProcesador, operacion);
//	pthread_mutex_unlock(&mLog);
//	//free(estado);
//}
void thread_loggearInfoEXEC(char* estado, int threadProcesador, char* operacion){
	pthread_mutex_lock(&mLog);
	log_info(kernel_configYLog->log," %s[%d]: %s",estado,threadProcesador, operacion);
	pthread_mutex_unlock(&mLog);
	//free(estado);
}
//------ LISTAS ---------
void agregarALista(t_list* lista, void* elemento, pthread_mutex_t semaphore){
	pthread_mutex_lock(&semaphore);
	list_add(lista,elemento);
	pthread_mutex_unlock(&semaphore);
}

#endif /* KER_AUXILIARES_H_ */
