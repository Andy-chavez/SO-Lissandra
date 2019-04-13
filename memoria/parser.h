/*
 * parser.c
 *
 *  Created on: 13 abr. 2019
 *      Author: utnso
 */

typedef enum {
	INSERT,
	CREATE,
	DESCRIBETABLE,
	DESCRIBEALL,
	DROP,
	JOURNAL,
	SELECT
} operacion;

operacion parser(char* operacionAParsear) {
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
