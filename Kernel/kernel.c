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
#include "conexiones.h"
#include <commons/config.h>
#include <commons/string.h>
#include <pthread.h>
#include <string.h>
#include "structs-basicos.h"

#define CANTIDADCRITERIOS 2 //0-2
#define STRONG 0
#define HASH 1
#define EVENTUAL 2

void kernel_api(char*);
criterio *inicializarCriterios();

void kernel_api(char* operacionAParsear)
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
				printf("RUN\n");
			}
		else if (string_equals_ignore_case(operacionAParsear, "METRICS")) {
				printf("METRICS\n");
			}
		else if (string_equals_ignore_case(operacionAParsear, "ADD")) {
				printf("ADD\n");
			}
		else {
			printf("No pude entender la operacion");
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

void* kernel_cliente(){
	t_config *CONFIG_KERNEL;
	 CONFIG_KERNEL = config_create("../KERNEL_CONFIG_EJEMPLO");//A modificar esto dependiendo del config que se quiera usar
	 char* IpMemoria = config_get_string_value(CONFIG_KERNEL ,"IP_MEMORIA");
	 char* PuertoMemoria = config_get_string_value(CONFIG_KERNEL ,"PUERTO_MEMORIA");
	 int socketClienteKernel = crearSocketCliente(IpMemoria,PuertoMemoria);
	 //aca tengo dudas de si dejar ese enviar o si puedo salir de la funcion
	 //enviar(socketClienteKernel, string, (strlen(string)+1)*sizeof(char));
	 cerrarConexion(socketClienteKernel); //Deberia de cerrar conexion?
 }

int main(int argc, char *argv[]){
//	criterio *criterios;
//	criterios = inicializarCriterios();
	pthread_t threadCliente;
	pthread_create(&threadCliente, NULL,kernel_cliente, NULL);
	pthread_join(threadCliente, NULL);

	return 0;
}
