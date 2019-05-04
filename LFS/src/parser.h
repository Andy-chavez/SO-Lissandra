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
/*
operacion parserGeneral(char* operacionAParsear) {
	if(string_starts_with(operacionAParsear, "INSERT")) {
		return INSERT;
	}
	else if (string_starts_with(operacionAParsear, "SELECT")) {
		return SELECT;
	}
	else if (string_starts_with(operacionAParsear, "DESCRIBE")) {
		return DESCRIBEALL;
	}
	else if (string_starts_with(operacionAParsear, "DESCRIBE ")) {
		return DESCRIBETABLE;
	}
	else if (string_starts_with(operacionAParsear, "CREATE")) {
		return CREATE;
	}
	else if (string_starts_with(operacionAParsear, "DROP")) {
		return DROP;
	}
	else {
		return -1;
	}
}
*/
//Ponemos la api y el parser todo en uno
void parserGeneral(char* operacionAParsear) {
	if(string_starts_with(operacionAParsear, "INSERT")) {
	//empezar operacion insert
		printf("empezar operacion insert: %s", operacionAParsear);
		char** parametros = string_split(operacionAParsear," ");
		printf("%s \n", *(parametros + 1));
		printf("%s \n", *(parametros + 2));
		printf("%s \n", *(parametros + 3));
	}
	else if (string_starts_with(operacionAParsear, "SELECT")) {
	//empezar operacion select
	}
	else if (string_starts_with(operacionAParsear, "DESCRIBE")) {
	printf("esto es un describe");
	//empezar operacion describe
	}
	else if (string_starts_with(operacionAParsear, "DESCRIBE  ")) {
	printf("esto es un describe all");
	//empezar operacion describeAll
	}
	else if (string_starts_with(operacionAParsear, "CREATE")) {
	//empezar operacion create
	}
	else if (string_starts_with(operacionAParsear, "DROP")) {
    //empezar operacion drop
	}
	else {
		printf("no entendi xD");
	}
}
