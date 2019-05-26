/*
 * kernel_operaciones.h
 *
 *  Created on: 16 may. 2019
 *      Author: utnso
 */

#ifndef KERNEL_OPERACIONES_H_
#define KERNEL_OPERACIONES_H_
#include "structs-basicos.h"
#include <commons/string.h>
#include <commonsPropias/serializacion.h>
#include <stdbool.h>
#include <readline/readline.h>
#include <readline/history.h>

/******************************VARIABLES GLOBALES******************************************/
char * pathConfig ="/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/Kernel/KERNEL_CONFIG_EJEMPLO";
char* ipMemoria;
char* puertoMemoria;
configYLogs *kernel_configYLog;
int quantumMax; //sacar esto de archivo de config
t_list* cola_proc_nuevos;  //use esta en el caso del run
t_list* cola_proc_listos; //esto me da medio inncesario porque de new ->ready es como que no hay tanta diferencia, alias estructuras para crear
t_list* cola_proc_terminados;
t_list* cola_proc_ejecutando;
// t_list* cola_proc_bloqueados; es necesaria? al no haber tiempo io como que bloqueados no se en que casos llenarla

/******************************DECLARACIONES******************************************/
void kernel_almacenar_en_cola(char*,char*);
void kernel_agregar_cola_proc_nuevos(char*, char*);
void kernel_run(char*);
void kernel_obtener_configuraciones(char*);
void kernel_consola();
void kernel_api(char*);
void liberarConfigYLogs();
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
instruccion* obtener_ultima_instruccion(t_list* instruc){
	int size = list_size(instruc);
	instruccion *instrucAux = malloc(sizeof(instruccion));
	instrucAux = list_get(instruc,size);
	return instrucAux;
}

bool instruccion_no_ejecutada(instruccion* instruc){
	return instruc->ejecutado==0;
}
/*void kernel_planificador(){
	while(!list_is_empty(cola_proc_listos)){ //TODO agregar semaforo cuando para avisar que hay procesos en listo
		//TODO poner semaforo
		pcb* pcb_auxiliar = malloc(sizeof(pcb));
		pcb_auxiliar->instruccion =list_create();
		pcb_auxiliar = list_remove(cola_proc_listos,1);
		if( pcb_auxiliar->instruccion == NULL){
			if(pcb_auxiliar->ejecutado ==1){
				list_add(cola_proc_terminados,pcb_auxiliar);
				free(pcb_auxiliar);
			}
			else{
				pcb_auxiliar->ejecutado=1;
				kernel_api(pcb_auxiliar->operacion,pcb_auxiliar->argumentos);
				list_add(cola_proc_terminados,pcb_auxiliar);
				free(pcb_auxiliar);
			}
		}
		else if(pcb_auxiliar->instruccion !=NULL){
			for(int quantum=0;quantum++;quantum<quantumMax){
				if(pcb_auxiliar->ejecutado ==0){
					kernel_api(pcb_auxiliar->operacion, pcb_auxiliar->argumentos);
					pcb_auxiliar->ejecutado=1;
				}
				instruccion* instruc=malloc(sizeof(instruccion));
				instruc= list_find(pcb_auxiliar->instruccion,(void*)instruccion_no_ejecutada);
				instruc->ejecutado = 1;
				kernel_api(instruc->operacion,instruc->argumentos);
			}
			if(!instruccion_no_ejecutada(obtener_ultima_instruccion(pcb_auxiliar->instruccion)))
				list_add(cola_proc_terminados, pcb_auxiliar);
			else
				list_add(cola_proc_listos, pcb_auxiliar);
		}
		free(pcb_auxiliar);
	}
	//cola_proc_listos
}
*/
void kernel_agregar_cola_proc_nuevos(char*operacion, char*argumentos){
	pcb* pcb_auxiliar = malloc(sizeof(pcb));
	pcb_auxiliar->operacion= malloc(sizeof(*operacion));
	pcb_auxiliar->operacion = operacion;
	pcb_auxiliar->operacion= malloc(sizeof(*argumentos));
	pcb_auxiliar->argumentos = argumentos;
	pcb_auxiliar->ejecutado = 0;
	list_add(cola_proc_nuevos,pcb_auxiliar); //TODO agregar mutex
	free(pcb_auxiliar->operacion);
	free(pcb_auxiliar->argumentos);
	free(pcb_auxiliar);
}
void kernel_almacenar_en_cola(char*operacion,char* argumentos){
	string_to_upper(operacion);
	if (string_equals_ignore_case(operacion, "RUN")) {
		printf("Ha utilizado el comando RUN, su archivo comenzará a ser ejecutado\n"); //poner esto en log aclarando el path
		kernel_run(argumentos);
	}
	else if(string_contains("SELECTINSERTCREATEDESCRIBEDROPJOURNALMETRICSADD", operacion)){
		kernel_agregar_cola_proc_nuevos(operacion, argumentos);
	}
	else{
		//loggear error de instruccion incorrecta
	}

}
operacionLQL* splitear_operacion(char* operacion){
	operacionLQL* operacionAux=malloc(sizeof(operacionLQL));
	char** opSpliteada;
	if(string_starts_with(operacion,"JOURNAL") || (string_starts_with(operacion,"DESCRIBE") && !string_contains(operacion," "))){
		operacionAux->operacion=malloc(sizeof(operacion));
		operacionAux->operacion=operacion;
		operacionAux->parametros= NULL;
	}
	else{
		opSpliteada = string_n_split(operacion,2," ");
		operacionAux->operacion=malloc(sizeof(*(opSpliteada)));
		operacionAux->operacion=*opSpliteada;
		operacionAux->parametros=malloc(sizeof(*(opSpliteada+1)));
		operacionAux->parametros=*(opSpliteada+1);
		free(*(opSpliteada+1));
		free(*(opSpliteada));
		free(opSpliteada);
	}
	return operacionAux;
}
void kernel_insert(char* operacion){
	operacionLQL* opAux=splitear_operacion(operacion);
	void * aEnviar = serializarOperacionLQL(opAux);
	int socketClienteKernel = crearSocketCliente(ipMemoria,puertoMemoria);
	enviar(socketClienteKernel, aEnviar, sizeof(aEnviar));
	printf("\n\nEnviado\n\n");
	//enviar(socketClienteKernel, string, (strlen(string)+1)*sizeof(char));
	int* recibido = (int*)recibir(socketClienteKernel);
	printf("%d",*recibido);
	printf("\n\nRecibido\n\n");
	cerrarConexion(socketClienteKernel);
	free(opAux->operacion);
	free(opAux->parametros);
	free(opAux);
}
void kernel_consola(){
	printf("Por favor ingrese <OPERACION> seguido de los argumentos\n\n");
	char* linea= NULL;
	size_t largo = 0;
	getline(&linea, &largo, stdin);
	//linea = readline("");
	//char** opYArg;
	//opYArg = string_n_split(linea,2," ");
	kernel_api(linea);
	//kernel_api(*opYArg,*(opYArg+1));
	//printf("%s",opYArg);
	//printf("%s",opYArg+1);
	//kernel_almacenar_en_cola(*opYArg,*(opYArg+1));
	free(linea);
	//free(*(opYArg+1));
	//free(*(opYArg));
	//free(opYArg); //tener en cuenta esa liberacion de punteros

	//TODO crear proc nuevo, preguntar si run o no
}
void kernel_run(char* path){
	FILE *archivoALeer= fopen(path, "r");
	char *lineaLeida;
	size_t limite = 250;
	ssize_t leer;
	lineaLeida = (char *)malloc(limite * sizeof(char));
	if (archivoALeer == NULL){
		//log_error(kernel_configYLog->log,"No se pudo ejecutar el archivo solicitado, verifique su existencia\n");
		free(lineaLeida);
		exit(EXIT_FAILURE);
	}
	pcb* pcb_auxiliar = malloc(sizeof(pcb));
	pcb_auxiliar->operacion= malloc(sizeof("RUN"));
	pcb_auxiliar->operacion = "RUN";
	pcb_auxiliar->operacion= malloc(sizeof(*path));
	pcb_auxiliar->argumentos = path;
	pcb_auxiliar->ejecutado = 1;
	pcb_auxiliar->instruccion =list_create();

	while((leer = getline(&lineaLeida, &limite, archivoALeer)) != -1){
		char** operacion;
		operacion = string_n_split(lineaLeida,2," ");
		instruccion* instruccion_auxiliar = malloc(sizeof(instruccion));
		instruccion_auxiliar->ejecutado= 0;
		instruccion_auxiliar->operacion= malloc(sizeof(*operacion)); //necesario?
		instruccion_auxiliar->operacion= *operacion;
		instruccion_auxiliar->argumentos= malloc(sizeof(*(operacion+1)));
		instruccion_auxiliar->argumentos= *(operacion+1);


		list_add(pcb_auxiliar->instruccion,instruccion_auxiliar);
		free(instruccion_auxiliar->operacion);
		free(instruccion_auxiliar->argumentos);
		free(instruccion_auxiliar);

	}
	list_add(cola_proc_nuevos,pcb_auxiliar); //TODO agregar mutex
	free(lineaLeida);
	free(pcb_auxiliar->operacion);
	free(pcb_auxiliar->argumentos);
	free(pcb_auxiliar);
	fclose(archivoALeer);
}
void kernel_api(char* operacionAParsear) //cuando ya esta en el rr
{
	if(string_starts_with(operacionAParsear, "INSERT")) {
		//	printf("INSERT\n");
		kernel_insert(operacionAParsear);
//TODO
		/*
			*en otra funcion* -> kernel_insert();
			int socketClienteKernel = crearSocketCliente(IpMemoria,PuertoMemoria);
			enviar(socketClienteKernel, string, (strlen(string)+1)*sizeof(char));
			hacer recibir para tener la rta
			cerrarConexion(socketClienteKernel);
			*/
	}
	else if (string_starts_with(operacionAParsear, "SELECT")) {
			printf("SELECT\n");
//TODO			kernel_select(*(operacion+1));
			/*
			*en otra funcion* -> kernel_select();
			int socketClienteKernel = crearSocketCliente(IpMemoria,PuertoMemoria);
			enviar(socketClienteKernel, string, (strlen(string)+1)*sizeof(char));
			hacer recibir para tener la rta
			cerrarConexion(socketClienteKernel);
			*/
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
		printf("Mi no entender esa operacion");
		}
}
void kernel_obtener_configuraciones(char* path){ //TODO agregar quantum
	kernel_configYLog= malloc(sizeof(configYLogs));
	kernel_configYLog->config = config_create(path);
	kernel_configYLog->log = log_create("KERNEL.log", "KERNEL", 1, LOG_LEVEL_INFO);
	ipMemoria = config_get_string_value(kernel_configYLog->config ,"IP_MEMORIA");
	puertoMemoria = config_get_string_value(kernel_configYLog->config,"PUERTO_MEMORIA");
}
void liberarConfigYLogs() {
	free(kernel_configYLog->config);
	free(kernel_configYLog->log);
	log_destroy(kernel_configYLog->log);
	config_destroy(kernel_configYLog->config);
	free(ipMemoria);
	free(puertoMemoria);
	free(kernel_configYLog);
}
//void* kernel_cliente(void *archivo){
//	int socketClienteKernel = crearSocketCliente(IpMemoria,PuertoMemoria);
//	 //aca tengo dudas de si dejar ese enviar o si puedo salir de la funcion
//	 //enviar(socketClienteKernel, string, (strlen(string)+1)*sizeof(char));
//	cerrarConexion(socketClienteKernel); //Deberia de cerrar conexion? }

#endif /* KERNEL_OPERACIONES_H_ */
