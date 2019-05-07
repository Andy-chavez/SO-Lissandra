/*
 * parser.c
 *
 *  Created on: 13 abr. 2019
 *      Author: utnso
 */

#ifndef PARSER_H_
#define PARSER_H_

typedef enum {
	INSERT,
	CREATE,
	DESCRIBETABLE,
	DESCRIBEALL,
	DROP,
	JOURNAL,
	SELECT
} operacion;

//Ponemos la api y el parser todo en uno
void parserGeneral(char* operacionAParsear,char* argumentos) { //cambio parser para que ignore uppercase
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
	else {
		printf("no entendi xD");
	}
}

#endif
