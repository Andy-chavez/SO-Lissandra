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
#include<stdlib.h>

#define CANTIDADCRITERIOS 2 //0-2
#define STRONG 0
#define HASH 1
#define EVENTUAL 2

typedef enum {
	SC, // UNA
	SH, // MUCHAS
	EC  // MUCHAS
}criterios;

typedef struct{
	int numeroDeMemoria;
	criterios *criterioAsociado; //malloc dps de saber cuantos criterios me devuelve el pool
}memoria;

typedef struct{
	criterios unCriterio;
	int *memoriasAsociadas; //malloc dps de saber cuantas memorias me devuelve el pool
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

void interfaz(caso UN_CASO);
criterio *inicializarCriterios();

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

/*
void roundRobinQuantumModificable(int quantum){

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

int main(int argc, char *argv[]){
	criterio *criterios;
	printf("hello \n");
	criterios = inicializarCriterios();
	return 0;
}
