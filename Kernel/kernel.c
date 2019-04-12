/*
 ============================================================================
 Name        : Kernel.c
 Author      : whyAreYouRunning?
 Version     :
 Copyright   : Your copyright notice
 Description :
 ============================================================================
 */

#include<stdio.h>

#define CANTIDAD_MEMORIAS_CONECTADAS // tambien esto

typedef enum {
	SC,
	SH,
	EC
}criterios;

typedef struct{
	int numeroDeMemoria;
	criterios *criterioAsociado;
}memoria;

typedef struct{
	criterios unCriterio;
	int *memoriaAsociadas; //malloc dps de saber cuantas memorias me devuelve el pool
}criterio;

typedef enum {
	INSERT,
	CREATE,
	DESCRIBETABLE,
	DESCRIBEALL,
	DROP,
	JOURNAL,
	SELECT,
	RUN,
	METRICS,
	ADD
}caso;

void interfaz();

int main(){
	return 0;
}


void interfaz(caso UN_CASO) //Cada aso deberia verse para luego implementarse
{
	switch(UN_CASO){
	case SELECT:
		printf("Caso a implementar INSERT");
		break;
	case INSERT:
		printf("Caso a implementar INSERT");
		break;
	case CREATE:
		printf("Caso a implementar CREATE");
		break;
	case DESCRIBETABLE:
		printf("Caso a implementar DESCRIBE TABLE");
		break;
	case DESCRIBEALL:
		printf("Caso a implementar DESCRIBE ALL");
		break;
	case DROP:
		printf("Caso a implementar DROP");
		break;
	case RUN:
		printf("Caso a implementar RUN");
		break;
	case JOURNAL:
		printf("Caso a implementar JOURNAL");
		break;
	case ADD:
		printf("Caso a implementar ADD");
		break;
	case METRICS:
		printf("Caso a implementar ADD");
		break;
	default:
		break;

	}
}
void roundRobinQuantumModificable(int quantum){

}
