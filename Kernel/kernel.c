/*
 ============================================================================
 Name        : Kernel.c
 Author      :
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include<stdio.h>


typedef enum{
	IN,
	CR,
	DT,
	DA,
	DR,
	RU,
	JO,
	AD

}casos;

int main()
{
	casos CASO;
	switch(CASO){
	case IN:
		printf("Caso a implementar INSERT");
		break;
	case CR:
		printf("Caso a implementar CREATE");
		break;
	case DT:
		printf("Caso a implementar DESCRIBE TABLE");
		break;
	case DA:
		printf("Caso a implementar DESCRIBE ALL");
		break;
	case DR:
		printf("Caso a implementar DROP");
		break;
	case RU:
		printf("Caso a implementar RUN");
		break;
	case JO:
		printf("Caso a implementar JOURNAL");
		break;
	case AD:
		printf("Caso a implementar ADD");
		break;
	default:
		break;

	}
	return 0;
}
