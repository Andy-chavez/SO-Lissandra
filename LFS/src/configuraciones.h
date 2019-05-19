/*
 * configuraciones.h
 *
 *  Created on: 19 may. 2019
 *      Author: utnso
 */

#ifndef SRC_CONFIGURACIONES_H_
#define SRC_CONFIGURACIONES_H_

#include <commons/config.h>
#include <commons/string.h>
#include <commons/bitarray.h>


int tamanioBloques;
int cantDeBloques;
char* magicNumber;
t_config* archivoMetadata;
//hasta aca todo se saca del metadata

char* ipLisandra;
char* puertoLisandra;
char* puntoMontaje;
//int tiempoDump y int Retardo por ahora no, pueden ir cambiando
int tamanioValue;
t_config* archivoDeConfig;
//hasta aca del archivo de config

void leerConfig(char* ruta){
	archivoDeConfig = config_create(ruta);
	ipLisandra = config_get_string_value(archivoDeConfig,"IP_LISANDRA");
	puertoLisandra = config_get_string_value(archivoDeConfig,"PUERTO_ESCUCHA");
	puntoMontaje = config_get_string_value(archivoDeConfig,"PUNTO_MONTAJE");
	tamanioValue = config_get_int_value(archivoDeConfig,"TAMAÑO_VALUE");

}

void leerMetadataFS (){
	char* rutaMetadata = string_new();
	string_append(&rutaMetadata,puntoMontaje);
	string_append(&rutaMetadata,"Metadata/Metadata.bin");
	archivoMetadata= config_create(rutaMetadata);
	tamanioBloques = config_get_int_value(archivoMetadata,"BLOCK_SIZE");
	cantDeBloques = config_get_int_value(archivoMetadata,"BLOCKS");
	magicNumber = config_get_string_value(archivoMetadata,"MAGIC_NUMBER");
}

//inicializarBitMap(){
//	FILE *archBitMap;
//	char* rutaBitMap = string_new(); //recordar inicializar siempre primero el leerConfig para el punto de montaje
//	string_append(&rutaBitMap,puntoMontaje);
//	string_append(&rutaBitMap,"Metadata/Bitmap.bin");
//	archBitMap = fopen(rutaBitMap);
//
//
//	fclose(archBitMap);
//	free(rutaBitMap);
//}


#endif /* SRC_CONFIGURACIONES_H_ */
