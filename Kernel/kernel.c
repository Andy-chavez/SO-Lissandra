/*
 ============================================================================
 Name        : Kernel.c
 Author      : whyAreYouRunning?
 Version     :
 Copyright   : Your copyright notice
 Description :
 ============================================================================
 */
#include <stdio.h>
#include <stdlib.h>
#include <commonsPropias/conexiones.h>
#include <commons/config.h>
#include <pthread.h>
#include <string.h>
#include <readline/readline.h>
#include <readline/history.h>
#include "kernel_operaciones.h"

#define CANTIDADCRITERIOS 2 //0-2
#define STRONG 0
#define HASH 1
#define EVENTUAL 2

void kernel_api(char*,char*);
criterio *inicializarCriterios();
void kernel_consola();

void kernel_consola(){
	printf("Por favor ingrese <OPERACION> seguido de los argumentos\n");
	char* linea;
	linea = readline("");
	char** opYArg;
	opYArg = string_n_split(linea,2," ");
	kernel_api(*opYArg,*(opYArg+1));
}
void kernel_api(char* operacionAParsear, char* argumentos)
{
	if(string_equals_ignore_case(operacionAParsear, "INSERT")) {
			printf("INSERT\n");
//TODO			kernel_insert();
			/*
			*en otra funcion* -> kernel_insert();
			int socketClienteKernel = crearSocketCliente(IpMemoria,PuertoMemoria);
			enviar(socketClienteKernel, string, (strlen(string)+1)*sizeof(char));
			hacer recibir para tener la rta
			cerrarConexion(socketClienteKernel);
			*/
		}
		else if (string_equals_ignore_case(operacionAParsear, "SELECT")) {
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
		else if (string_equals_ignore_case(operacionAParsear, "DESCRIBE")) {
			printf("DESCRIBE\n");
//TODO			kernel_describe();
		}
		else if (string_equals_ignore_case(operacionAParsear, "CREATE")) {
			printf("CREATE\n");
//TODO			kernel_create();
		}
		else if (string_equals_ignore_case(operacionAParsear, "DROP")) {
			printf("DROP\n");
//TODO			kernel_drop();
		}
		else if (string_equals_ignore_case(operacionAParsear, "JOURNAL")) {
			printf("JOURNAL\n");
//TODO			kernel_journal();
		}
		else if (string_equals_ignore_case(operacionAParsear, "RUN")) {
			printf("Ha utilizado el comando RUN, su archivo comenzará a ser ejecutado\n");
//TODO			kernel_run(*argumentos);
		}
		else if (string_equals_ignore_case(operacionAParsear, "METRICS")) {
			printf("METRICS\n");
//TODO			kernel_metrics();
		}
		else if (string_equals_ignore_case(operacionAParsear, "ADD")) {
			printf("ADD\n");
//TODO			kernel_add();
		}
		else {
			printf("Mi no entender esa operacion");
		}
}

/*
void kernel_planificador(int quantum){

}
*/

criterio *inicializarCriterios(){
	criterio *datos = malloc(3*sizeof(criterio));
	datos[STRONG].unCriterio = SC; //Strong
	datos[HASH].unCriterio = SH; //Hash
	datos[EVENTUAL].unCriterio = EC; //Eventual
	for(int iter=0; iter <= CANTIDADCRITERIOS; iter++){
		datos[iter].memoriasAsociadas = malloc(sizeof(int));
		printf("Criterio: %d \n Memoria: %d \n",iter, *(datos[iter].memoriasAsociadas) );
	}
	return datos;
}

int main(int argc, char *argv[]){
//	criterio *criterios;
//	criterios = inicializarCriterios();
/*	pthread_t threadCliente;

	archivosDeConfigYLog->config = config_create("../KERNEL_CONFIG_EJEMPLO");//A modificar esto dependiendo del config que se quiera usar
	archivosDeConfigYLog->logger = log_create("KERNEL.log", "KERNEL", 1, LOG_LEVEL_INFO);
	pthread_create(&threadCliente, NULL,kernel_cliente, (void *)archivosDeConfigYLog);
	pthread_join(threadCliente, NULL);
*/
	cola_proc_nuevos = list_create();
	kernel_consola();
//	liberarConfigYLogs(archivosDeConfigYLog);
	return 0;
}
