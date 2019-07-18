#ifndef KER_STRUCTS_H_
#define KER_STRUCTS_H_

#include <commons/log.h>
#include <commons/config.h>
#include <commons/string.h>
#include <commons/collections/list.h>
#include <stdlib.h>
#include <semaphore.h>
#include <commonsPropias/serializacion.h>
#include <commonsPropias/conexiones.h>
#include <stdbool.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <unistd.h>
#include <sys/time.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
//#include <math.h>
#define HASH 2
#define STRONG 1
#define EVENTUAL 0
/******************************STRUCTS******************************************/
typedef struct{
	char* operacion;
	int ejecutado; //0 si no se ejecuto, 1 si se ejecuto
	t_list* instruccion;
}pcb;
typedef struct{
	char* operacion;
	int ejecutado; //0 si no se ejecuto, 1 si se ejecuto
}instruccion;
typedef struct{
	consistencia unCriterio;
	int cantidadSelects;
	int cantidadInserts;
	float tiempoSelects; //aca es el tiempo total, realizar division cuando loggee
	float tiempoInserts;
	t_list* memorias;
}criterio;
typedef struct{
	int numero;
	char* puerto;
	char* ip;
	int cantidadIns;
	int cantidadSel;
}memoria;
typedef struct{
	char* nombreDeTabla;
	consistencia consistenciaDeTabla;
}tabla;
typedef struct {
	t_config* config;
	t_log* log;
} configYLogs;
typedef struct {
	pthread_t* thread;
	int numero;
} t_thread;


t_log* logMetrics;
t_log* logResultados;
/******************************VARIABLES GLOBALES******************************************/
t_list* cola_proc_nuevos;
t_list* cola_proc_listos;
t_list* cola_proc_terminados;
t_list* memorias;
t_list* tablas;
t_list* rrThreads;
criterio criterios[3];

sem_t hayNew;
sem_t hayReady;
sem_t finalizar;
sem_t modificables;

pthread_mutex_t quantum;
pthread_mutex_t sleepExec;
pthread_mutex_t mMetadataRefresh;
pthread_mutex_t mMemorias;
pthread_mutex_t mTablas;
pthread_mutex_t mEventual;
pthread_mutex_t mStrong;
pthread_mutex_t mHash;
pthread_mutex_t colaListos;
pthread_mutex_t colaNuevos;
pthread_mutex_t colaTerminados;
pthread_mutex_t mLog;
pthread_mutex_t mThread;
pthread_mutex_t mConexion;
pthread_mutex_t mLogMetrics;
pthread_mutex_t mLogResultados;
//char * pathConfig ="/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/Kernel/KERNEL_CONFIG_EJEMPLO";

char* pathConfig;
char* ipMemoria;
char* puertoMemoria;

configYLogs *kernel_configYLog;

int destroy = 0;
int multiprocesamiento;
int timedGossip;
// -------------------- CAMBIAN EN TIEMPO DE EXEC ------------------------
int quantumMax;
int metadataRefresh;
int sleepEjecucion;
/******************************CONFIGURACIONES******************************************/
void kernel_inicializarSemaforos();
void kernel_crearListas();
void liberarConfigYLogs();
void kernel_inicializarVariablesYListas();
void kernel_finalizar();
int kernel_inicializarMemoria();
/******************************OPERACIONES******************************************/
void liberarMemoria(memoria* elemento);
void realizarJournal(memoria * mem);

bool kernel_create(char* operacion,int thread);
bool kernel_describe(char* operacion,int thread);
bool kernel_journal();
bool kernel_metrics(int consola);
bool kernel_api(char* operacionAParsear,int thread);
bool kernel_add(char* operacion);
bool kernel_drop(char* operacion,int thread);
bool kernel_select(char* operacion,int thread);
bool kernel_insert(char* operacion,int thread);

void journal_consistencia(int consistencia, pthread_mutex_t sem);
void kernel_almacenar_en_new(char*operacion);
void kernel_crearPCB(char* operacion);
void kernel_run(char* operacion);
void kernel_pasar_a_ready();
void kernel_consola();
void kernel_roundRobin(int threadProcesador);
void joinThreadRR();
void crearThreadRR(int numero);
void metrics();
/******************************AUXILIARES******************************************/
bool recibidoContiene(char* recibido, char* contiene);
bool instruccion_no_ejecutada(instruccion* instruc);

void describeTimeado();
void thread_loggearInfoYLiberarParametrosRECIBIDO(int thread,char* recibido, operacionLQL *opAux);
void thread_loggearInfo(char* estado, int threadProcesador, char* operacion);
void agregarALista(t_list* lista, void* elemento, pthread_mutex_t semaphore);
void guardarTablaCreada(char* parametros);
void eliminarTablaCreada(char* parametros);
void enviarJournal(int socket);
void guardarMemorias(seed* unaSeed);
void agregarTablaVerificandoSiLaTengo(tabla* t);
void agregarTablaVerificandoSiLaTengo(tabla* t);
void agregarMemoriaVerificandoSiLaTengo(memoria* memAux);
void liberarTabla(tabla* t);

int socketMemoriaSolicitada(consistencia criterio);
int obtenerIndiceDeConsistencia(consistencia unaConsistencia);
int strong_obtenerSocketAlQueSeEnvio(operacionLQL* opAux);
int eventual_obtenerSocketAlQueSeEnvio(operacionLQL* opAux);
int hash_obtenerSocketAlQueSeEnvio(operacionLQL* opAux);
int obtenerSocketAlQueSeEnvio(operacionLQL* opAux, int index);
int enviarOperacion(operacionLQL* opAux,int index,int thread);
int random_int(int min, int max);

tabla* encontrarTablaPorNombre(char* nombre);

memoria* encontrarMemoria(int numero);

consistencia encontrarConsistenciaDe(char* nombreTablaBuscada);
#endif /* KER_STRUCTS_H_ */
