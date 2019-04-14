#include<stdio.h>
#include<stdlib.h>
#include<time.h>
#include<stdint.h>
#include<string.h>

typedef struct {
	time_t timestamp;
	u_int16_t key;
	char* value;
} registro;

void* serializarRegistro(registro* unRegistro,char* nombreTabla, int bytes) //~~~~~Como haces para saber eso? sizeof()?
{
	void *bufferRegistro= malloc(bytes);
	int desplazamiento = 0;
	int largoDeNombreTabla = strlen(nombreTabla);
	int largoDeValue = strlen(unRegistro->value);

	//Tamaño de nombre de tabla
	memcpy(bufferRegistro + desplazamiento, &largoDeNombreTabla, sizeof(int));
	desplazamiento+= sizeof(int);
	//Nombre de tabla
	memcpy(bufferRegistro + desplazamiento, &nombreTabla, sizeof(char)*largoDeNombreTabla);
	desplazamiento+= largoDeNombreTabla;
	//Tamaño de timestamp
	memcpy(bufferRegistro + desplazamiento, sizeof(time_t), sizeof(time_t)); //Tira warnings, si nos quieren ayudar, todo suyo
	desplazamiento+= sizeof(int);
	//Nombre de timestamp
	memcpy(bufferRegistro + desplazamiento, &(unRegistro->timestamp), sizeof(time_t));
	desplazamiento+= sizeof(time_t);
	//Tamaño de key
	memcpy(bufferRegistro + desplazamiento, sizeof(u_int16_t), sizeof(u_int16_t)); //Tira warnings, si nos quieren ayudar, todo suyo
	desplazamiento+= sizeof(int);
	//Nombre de key
	memcpy(bufferRegistro + desplazamiento, &(unRegistro->key), sizeof(u_int16_t));
	desplazamiento+= sizeof(u_int16_t);
	//Tamaño de value
	memcpy(bufferRegistro + desplazamiento, &largoDeValue, largoDeValue); //Tira warnings, si nos quieren ayudar, todo suyo
	desplazamiento+= sizeof(int);
	//Nombre de value
	memcpy(bufferRegistro + desplazamiento, &(unRegistro->value), sizeof(char)*largoDeValue);
	desplazamiento+= sizeof(char)*largoDeValue;
	return bufferRegistro;
}

void* serializarOperacion(int unaOperacion, char* stringDeValores, int bytes) //~~~~~Como haces para saber los bytes? sizeof()?
{
	void *bufferRegistro= malloc(bytes);
	int desplazamiento = 0;
	int largoDeStringValue = strlen(stringDeValores);

/*	//					[ OP (en este caso, 0) ] KHE
	memcpy(bufferRegistro + desplazamiento, &largoDeNombreTabla, sizeof(int));
	desplazamiento+= sizeof(int);
	//
	memcpy(bufferRegistro + desplazamiento, &(unRegistro->nombreTablaAsociada), largoDeNombreTabla);
	desplazamiento+= largoDeNombreTabla;
*/
	//Tamaño de operacion(enum)
	memcpy(bufferRegistro + desplazamiento, sizeof(int), sizeof(int)); //Tira warnings, si nos quieren ayudar, todo suyo
	desplazamiento+= sizeof(int);
	//enum de operacion
	memcpy(bufferRegistro + desplazamiento, &unaOperacion, sizeof(int));
	desplazamiento+= sizeof(int);
	//Tamaño de String de valores
	memcpy(bufferRegistro + desplazamiento, &largoDeStringValue, sizeof(int)); //Tira warnings, si nos quieren ayudar, todo suyo
	desplazamiento+= sizeof(int);
	//String de valores
	memcpy(bufferRegistro + desplazamiento, &stringDeValores, sizeof(char)*largoDeStringValue);
	desplazamiento+= sizeof(char)*largoDeStringValue;
	return bufferRegistro;
}

int main(){ return 0;}
