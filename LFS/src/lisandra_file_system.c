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
	SE, //Select
	IN, //Insert
	CR, //Create
	DA, //Describe All
	DT, //Describe Table
	DR //Drop
}casos;

int main(int argc, char* argv[]) {
	casos caso;

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
	return EXIT_SUCCESS;
}

void leerConsola(){
	char* linea = readline(">");
	while (strcmp(linea,'\0')==0){

	}
	//readline()
}
