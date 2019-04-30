/*
 * structs-basicos.c
 *
 *  Created on: 25 abr. 2019
 *      Author: utnso
 */

#include "structs-basicos.h"


void liberarConfigYLogs(configYLogs *archivos) {
	log_destroy(archivos->logger);
	config_destroy(archivos->config);
	free(archivos);
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
