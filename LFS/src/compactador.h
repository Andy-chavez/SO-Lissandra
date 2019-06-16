/*
 * compactador.h
 *
 *  Created on: 31 may. 2019
 *      Author: utnso
 */

#include "dump.h"
#include <stdio.h>


void cargarInfoDeBloquesParaCompactacion(char** buffer, char**arrayDeBloques, int sizeParticion){
	int i = 0;
		while(*(arrayDeBloques+i)!= NULL){
							char* informacion = infoEnBloque(*(arrayDeBloques+i),sizeParticion);
							string_append(buffer, informacion);
							i++;
						}
}


//hay que implementarlo con hilos
//pasar el buffer con & , y recibir como un char*
void insertarInfoEnBloquesOriginales(char* rutaTabla, t_list* listaRegistrosTemporales){

	//esto estaba en la de obtener metadata pero no se si se puede ser delegativo con esto
	t_list* listaRegistrosOriginales = list_create();
	//liberar la lista

	char* bufferNuevo = string_new();
	char* rutaMetadata = string_new();

	string_append(&rutaMetadata,rutaTabla);
	string_append(&rutaMetadata,"/Metadata");

	t_config* configMetadata = config_create(rutaMetadata);

	int cantParticiones = config_get_int_value(configMetadata, "PARTITIONS");


	for (int i = 0; i< cantParticiones; i++){

		string_append(&rutaTabla,"/part"); //vamos a usar la convension PartN.bin
		string_append(&rutaTabla, i);
		string_append(&rutaTabla,".bin");
		t_config* tabla = config_create(rutaTabla);
		struct stat sb;

		char** arrayDeBloques = config_get_array_value(rutaTabla,"BLOCKS");

		int sizeParticion=config_get_int_value(tabla,"SIZE");
		int j = 0;
				while(*(arrayDeBloques+j)!= NULL){
					char* informacion = infoEnBloque(*(arrayDeBloques+j),sizeParticion);
					string_append(&bufferNuevo, informacion);
					j++;
				}


	char** separarRegistro = string_split(bufferNuevo,"\n");
		int k =0;
				for(k=0;*(separarRegistro+j)!=NULL;j++){
						char **aCargar =string_split(*(separarRegistro+j),";");
						agregarALista(*(aCargar+0),*(aCargar+1),*(aCargar+2),listaRegistrosOriginales);
						free(*(aCargar+0));
						free(*(aCargar+1));
						free(*(aCargar+2));
							}



		int archivo = open(rutaTabla,O_RDWR);
		fstat(archivo,&sb);
			if (sb.st_size == 0){
		//		cargarInformacionNueva(rutaTabla);
				break;
			}


	config_destroy(tabla);
	}


}

void compactar(char* nombreTabla){

	int i;
	char* rutaTabla = string_new();
	char* rutaTmpOriginal = string_new();
	char* rutaTmpCompactar= string_new();
	char* bufferTemporales = string_new();

	t_config* archivoTmp;
	t_list* listaRegistrosTemporales = list_create();
	//liberar la lista

	int numeroTmp = obtenerCantTemporales(nombreTabla);

	for (i = 0; i< numeroTmp; i++){

		string_append(&rutaTabla, puntoMontaje);
		string_append(&rutaTabla, "Tables/");
		string_append(&rutaTabla, nombreTabla);
		string_append(&rutaTabla, "/");


		string_append(&rutaTmpOriginal, rutaTabla);
		string_append(&rutaTmpOriginal, string_itoa(i));
		string_append(&rutaTmpOriginal,".tmp");
		string_append(&rutaTmpCompactar, rutaTabla);
		string_append(&rutaTmpCompactar, string_itoa(i));
		string_append(&rutaTmpCompactar,".tmpc");
		//con esto despues se puede verificar que no se pueda hacer esto
		int cambiarNombre = rename(rutaTmpOriginal, rutaTmpCompactar);
		archivoTmp = config_create(rutaTmpCompactar);

		//leer bloques

		char** arrayDeBloques = config_get_array_value(archivoTmp,"BLOCKS");
		int sizeParticion=config_get_int_value(archivoTmp,"SIZE");

		int j = 0;

		cargarInfoDeBloquesParaCompactacion(&bufferTemporales, arrayDeBloques, sizeParticion);

		/*
		while(*(arrayDeBloques+j)!= NULL){
			char* informacion = infoEnBloque(*(arrayDeBloques+j),sizeParticion);
			string_append(&bufferTemporales, informacion);
			j++;
		}
		*/

		//para cerrar este checkpoint guardar eso en los bloques originales y chau

		//cargar en lista de temporales //REUTILIZA CODIGO, HACER DELEGACION

		char** separarRegistro = string_split(bufferTemporales,"\n");
				int k =0;
				for(k=0;*(separarRegistro+j)!=NULL;j++){
					char **aCargar =string_split(*(separarRegistro+j),";");
					agregarALista(*(aCargar+0),*(aCargar+1),*(aCargar+2),listaRegistrosTemporales);
					free(*(aCargar+0));
					free(*(aCargar+1));
					free(*(aCargar+2));
				}

		free(rutaTabla);
		free(rutaTmpOriginal);
		free(rutaTmpCompactar);
		config_destroy(archivoTmp);
	}

	insertarInfoEnBloquesOriginales(rutaTabla, listaRegistrosTemporales);

//	string_append(bufferRegistrosTemporales, infoDeTmps);

}
