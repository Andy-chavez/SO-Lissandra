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
//#include <commonsPropias/conexiones.h>
#include <commons/string.h>
#include <pthread.h>
#include <string.h>
#include "conexiones.h"
#include <readline/readline.h>
#include <readline/history.h>
#include "structs-basicos.h"

#define CANTIDADCRITERIOS 2 //0-2
#define STRONG 0
#define HASH 1
#define EVENTUAL 2

void kernel_api(char*,char*);
criterio *inicializarCriterios();
void kernel_consola();

void kernel_consola(){
	printf(" Welcome to the league of Kernel\n Por favor ingrese <OPERACION> seguido de los argumentos\n");
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
		}
		else if (string_equals_ignore_case(operacionAParsear, "SELECT")) {
			printf("SELECT\n");
		}
		else if (string_equals_ignore_case(operacionAParsear, "DESCRIBE")) {
			printf("DESCRIBE\n");
		}
		else if (string_equals_ignore_case(operacionAParsear, "CREATE")) {
			printf("CREATE\n");
		}
		else if (string_equals_ignore_case(operacionAParsear, "DROP")) {
			printf("DROP\n");
		}
		else if (string_equals_ignore_case(operacionAParsear, "JOURNAL")) {
				printf("JOURNAL\n");
		}
		else if (string_equals_ignore_case(operacionAParsear, "RUN")) {
				printf("Ha utilizado el comando RUN, su archivo comenzará a ser ejecutado\n");
			}
		else if (string_equals_ignore_case(operacionAParsear, "METRICS")) {
				printf("METRICS\n");
			}
		else if (string_equals_ignore_case(operacionAParsear, "ADD")) {
				printf("ADD\n");
			}
		else {
			printf("Mi no entender esa operacion");
		}
}

/*
void kernel_planificador(int quantum){

}
*/

//criterio *inicializarCriterios(){
//	criterio *datos = malloc(3*sizeof(criterio));
//	datos[STRONG].unCriterio = SC; //Strong
//	datos[HASH].unCriterio = SH; //Hash
//	datos[EVENTUAL].unCriterio = EC; //Eventual
//	for(int iter=0; iter <= CANTIDADCRITERIOS; iter++){
//		datos[iter].memoriasAsociadas = malloc(sizeof(int));
//		printf("Criterio: %d \n Memoria: %d \n",iter, *(datos[iter].memoriasAsociadas) );
//	}
//	return datos;
//}

void* kernel_cliente(void *archivo){
	configYLogs *kernel_configYLog = (configYLogs*) archivo;;
	char* IpMemoria = config_get_string_value(kernel_configYLog->config ,"IP_MEMORIA");
	char* PuertoMemoria = config_get_string_value(kernel_configYLog->config,"PUERTO_MEMORIA");
	int socketClienteKernel = crearSocketCliente(IpMemoria,PuertoMemoria);
	 //aca tengo dudas de si dejar ese enviar o si puedo salir de la funcion
	 //enviar(socketClienteKernel, string, (strlen(string)+1)*sizeof(char));
	cerrarConexion(socketClienteKernel); //Deberia de cerrar conexion?
 }
int main(int argc, char *argv[]){
//	criterio *criterios;
//	criterios = inicializarCriterios();
/*	pthread_t threadCliente;
	configYLogs *archivosDeConfigYLog = malloc(sizeof(configYLogs));
	archivosDeConfigYLog->config = config_create("../KERNEL_CONFIG_EJEMPLO");//A modificar esto dependiendo del config que se quiera usar
	archivosDeConfigYLog->logger = log_create("KERNEL.log", "KERNEL", 1, LOG_LEVEL_INFO);
	pthread_create(&threadCliente, NULL,kernel_cliente, (void *)archivosDeConfigYLog);
	pthread_join(threadCliente, NULL);
*/
	kernel_consola();
//	liberarConfigYLogs(archivosDeConfigYLog);
	return 0;
}
