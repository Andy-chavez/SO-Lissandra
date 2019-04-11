/*
 ============================================================================
 Name        : Api.c
 Author      :
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include<stdio.h>


typedef enum{
	INSERT,
	CREATE,
	DESCRIBETABLE,
	DESCRIBEALL,
	DROP,
	JOURNAL,
	SELECT
}casos;

int main()
{
	casos CASO;
	switch(CASO){
	case INSERT:
		break;
	case CREATE:
		break;
	case DESCRIBETABLE:
		break;
	case DESCRIBEALL:
		break;
	case DROP:
		break;
	case SELECT:
		break;
	case JOURNAL:
		break;
	default:
		break;
	}
	return 0;
}
