/*
 ============================================================================
 Name        : lisandra_file_system.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description :
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include<readline/readline.h>

typedef enum{
	SELECT, //Select
	INSERT, //Insert
	CREATE, //Create
	DESCRIBEALL, //Describe All
	DESCRIBETABLE, //Describe Table
	DROP //Drop
}casos;

void ipa(casos caso;){
	//leerConsola();
	switch (caso){
		case SE:
			//Select
			break;
		case IN:
			//Insert
			break;
		case CR:
			//Create
			break;
		case DA:
			//Describe todas las tablas(All)
			break;
		case DT:
			//Describe una tabla
			break;
		case DR:
			//Drop table
			break;
		default:
			printf("Error del header");
			//agregar al archivo de log
	}
}

int main(int argc, char* argv[]) {
	
	return EXIT_SUCCESS;
}

