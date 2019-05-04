/*
 * parser.c
 *
 *  Created on: 13 abr. 2019
 *      Author: utnso
 */

void APIMemoria(char* operacionAParsear) {
	if(string_equals_ignore_case(operacionAParsear, "INSERT")) {
		printf("Recibi un INSERT\n");
	}
	else if (string_equals_ignore_case(operacionAParsear, "SELECT")) {
		printf("Recibi un SELECT\n");
	}
	else if (string_equals_ignore_case(operacionAParsear, "DESCRIBE")) {
		printf("Recibi un DESCRIBE\n");
	}
	else if (string_equals_ignore_case(operacionAParsear, "CREATE")) {
		printf("Recibi un CREATE\n");
	}
	else if (string_equals_ignore_case(operacionAParsear, "DROP")) {
		printf("Recibi un DROP\n");
	}
	else if (string_equals_ignore_case(operacionAParsear, "JOURNAL")) {
			printf("Recibi un JOURNAL\n");
		}
	else {
		printf("No pude entender la operacion");
	}
}
