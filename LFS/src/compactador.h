/*
 * compactador.h
 *
 *  Created on: 31 may. 2019
 *      Author: utnso
 */

#include "dump.h"
#include <stdio.h>


bool cargarInfoDeBloquesParaCompactacion(char** buffer, char**arrayDeBloques, int sizeParticion){
	int i = 0;
		while(*(arrayDeBloques+i)!= NULL){
			//el archivo esta vacio
			if(infoEnBloque(*(arrayDeBloques+i),sizeParticion) == NULL){
					return false;
			}else{
				char* informacion = infoEnBloque(*(arrayDeBloques+i),sizeParticion);
				string_append(buffer, informacion);
			i++;
			}
		}
		return true;
}


void crearArchivoConRegistrosACompactar(char*ruta){
	char* rutaRegistros = string_new();
	string_append(&rutaRegistros, ruta);
	string_append(&rutaRegistros, "c");
	//crear el archivo

	FILE *archivoPartc = fopen(rutaRegistros,"w");

	fclose(archivoPartc);
	free(rutaRegistros);
}

void actualizarBloquesEnArchivo(char** arrayDeBloques, char* rutaParticion,int cantidadTotalNecesaria,int size){
//aca entras cuando ya necesitas mas bloques de los que ya tenes
	char* infoAGuardar = string_new();


				int largoArrayDeBloques = calcularLargoArrayDeBloques(arrayDeBloques);
				char* tamanioTotal = string_itoa(size);
				string_append(&infoAGuardar, "SIZE=");

				string_append(&infoAGuardar,tamanioTotal);
				string_append(&infoAGuardar, "\n");
				string_append(&infoAGuardar, "BLOCKS=[");
				free(tamanioTotal);
				for(int i=0;i<largoArrayDeBloques;i++){
					string_append(&infoAGuardar, *(arrayDeBloques + i));
					cantidadTotalNecesaria--;
					string_append(&infoAGuardar,",");
			}
				for(int j=0;j<cantidadTotalNecesaria;j++){
						char* bloqueLibre = devolverBloqueLibre();
						string_append(&infoAGuardar,bloqueLibre);
						cantidadTotalNecesaria--;
						free(bloqueLibre);
						if(cantidadTotalNecesaria>0) string_append(&infoAGuardar,","); //si es el ultimo no quiero que me ponga una ,
				}
				string_append(&infoAGuardar, "]");

remove(rutaParticion);
guardarInfoEnArchivo(rutaParticion, infoAGuardar);
free(infoAGuardar);
}

int calcularLargoArrayDeBloques(char** arrayDeBloques){
	int cantidad=0;
	while(*(arrayDeBloques+cantidad)!=NULL){
		cantidad++;
	}
	return cantidad;
}

void ingresarNuevaInfo(char* rutaParticion,char** arrayDeBloques, int sizeParticion, char** bufferTemporales){
	int tamanioDelBuffer = strlen(*(bufferTemporales));
	int cantBloquesNecesarios =  ceil((float) (tamanioDelBuffer/ (float) tamanioBloques));
	int i;
	int largoDeArrayDeBloques =calcularLargoArrayDeBloques(arrayDeBloques);
	//si solo se necesita un bloque se guarda en el unico bloque que se le asigno a la particion al cargar la tabla
	if (cantBloquesNecesarios == largoDeArrayDeBloques){
		guardarRegistrosEnBloques(tamanioDelBuffer, cantBloquesNecesarios, arrayDeBloques, *bufferTemporales);
	//Si no, hay que darle mas bloques libres y cargarlos al array

	}else{

		actualizarBloquesEnArchivo(arrayDeBloques, rutaParticion,cantBloquesNecesarios,tamanioDelBuffer); //aca se crea la nueva particion

		guardarRegistrosEnBloques(tamanioDelBuffer, cantBloquesNecesarios, arrayDeBloques, *bufferTemporales);

		//ver si funca esto, dejo asi comentado porque hay que ver el tema del drop si hay algo que se use para borrar archivos

	//	guardarInfoEnArchivo(rutaParticion, infoAGuardar);


//		liberarDoblePuntero(separarRegistro);
	}

}

bool seEncuentraElRegistroYTieneLaMismaKey(registro* registroOriginal, registro* registroTemporal){
	return registroOriginal->key == registroTemporal->key;
}

t_list* agregadoYReemplazoDeRegistros(t_list* listaRegistrosTemporalesDeParticionActual, t_list* listaRegistrosOriginalesDeParticionActual){

	registro* registroACorroborar;

	int i = 0;

	void agregarOReemplazar(registro* registroTemporal){

		bool estaElRegistro(registro* registroOriginal){

			if(seEncuentraElRegistroYTieneLaMismaKey(registroOriginal, registroTemporal)){
				//registro que queda !!!!!!
				registro* registroAReemplazar = devolverMayor(registroOriginal, registroTemporal);
				list_replace(listaRegistrosOriginalesDeParticionActual, i, registroAReemplazar);
				return true;
			}else{
				i++;
				return false;
		}
		}
	i=0;
	if(!(registroACorroborar = list_find(listaRegistrosOriginalesDeParticionActual, estaElRegistro))){
		list_add(listaRegistrosOriginalesDeParticionActual, registroTemporal);
	}
	}

	list_iterate(listaRegistrosTemporalesDeParticionActual,(void *) agregarOReemplazar);

	return listaRegistrosOriginalesDeParticionActual;
}

void cargarListaEnBuffer(t_list* listaDeRegistros, char** buffer){


	void cargarRegistro(registro* unRegistro){

		char* time = string_itoa(unRegistro->timestamp);
		char* key = string_itoa(unRegistro->key);

		string_append(buffer,time);
		string_append(buffer,";");
		string_append(buffer,key);
		string_append(buffer,";");
		string_append(buffer,unRegistro->value);
		string_append(buffer,"\n");

		free(time);
		free(key);

	}

	list_iterate(listaDeRegistros,(void*)cargarRegistro);

}

//hay que implementarlo con hilos
//pasar el buffer con & , y recibir como un char*
void insertarInfoEnBloquesOriginales(char* rutaTabla, t_list* listaRegistrosTemporales){

	//esto estaba en la de obtener metadata pero no se si se puede ser delegativo con esto
	t_list* listaRegistrosOriginalesDeParticionActual = list_create();
	//liberar la lista

	char* rutaMetadata = string_new();

	string_append(&rutaMetadata,rutaTabla);
	string_append(&rutaMetadata,"/Metadata");

	t_config* configMetadata = config_create(rutaMetadata);

	int cantParticiones = config_get_int_value(configMetadata, "PARTITIONS");

	for (int i = 0; i< cantParticiones; i++){

		char* bufferBloques = string_new();
		char* rutaParticion = string_new();
		char* bufferParticion = string_new();
		char* bufferFinal = string_new();
		char* bufferTemporales = string_new();

		bool tieneLaKey(registro* unRegistro){
			return (calcularParticion(unRegistro->key, cantParticiones) == i);
		}

		//no se si se puede hacer delegacion de una funcion de orden superior
		//se cargan los registros de cada particion
		/*
		void cargarRegistro(registro* unRegistro){

				char* time = string_itoa(unRegistro->timestamp);
				char* key = string_itoa(unRegistro->key);

				string_append(&bufferParticion,time);
				string_append(&bufferParticion,";");
				string_append(&bufferParticion,key);
				string_append(&bufferParticion,";");
				string_append(&bufferParticion,unRegistro->value);
				string_append(&bufferParticion,"\n");

				free(time);
				free(key);

			}
*/
		void guardarEnBuffer(registro* unRegistro){
			char* time = string_itoa(unRegistro->timestamp);
			char* key = string_itoa(unRegistro->key);

			string_append(&bufferFinal,time);
			string_append(&bufferFinal,";");
			string_append(&bufferFinal,key);
			string_append(&bufferFinal,";");
			string_append(&bufferFinal,unRegistro->value);
			string_append(&bufferFinal,"\n");
			free(time);
			free(key);
			}

		t_list* listaRegistrosTemporalesDeParticionActual = list_filter(listaRegistrosTemporales, tieneLaKey);
		char* numeroDeParticion =string_itoa(i);
		string_append(&rutaParticion, rutaTabla);
		string_append(&rutaParticion,"part"); //vamos a usar la convension PartN.bin
		string_append(&rutaParticion, numeroDeParticion);
		string_append(&rutaParticion,".bin");
		t_config* tabla = config_create(rutaParticion);
		struct stat sb;

		char** arrayDeBloques = config_get_array_value(tabla,"BLOCKS");

		int sizeParticion=config_get_int_value(tabla,"SIZE");

		if (!cargarInfoDeBloquesParaCompactacion(&bufferBloques, arrayDeBloques, sizeParticion)){
			if (listaRegistrosTemporalesDeParticionActual->elements_count == 0){
				puts("ACA SE VA TODO A LA VERRRRRRRRRRRRRRGAAAAAA");
			}
			//meter una lista de registros temporales en un buffer
			cargarListaEnBuffer(listaRegistrosTemporalesDeParticionActual, &bufferTemporales);
			ingresarNuevaInfo(rutaParticion, arrayDeBloques, sizeParticion, &bufferTemporales);
			//break porque no puede haber una particion que tenga data si la anterior no tenia data
			//puede ser q sea continue
			continue;
		}else{

			char** separarRegistro = separarRegistrosDeBuffer(bufferBloques, listaRegistrosOriginalesDeParticionActual);
			t_list* estoNoFuncaNiAPalo = agregadoYReemplazoDeRegistros(listaRegistrosTemporalesDeParticionActual, listaRegistrosOriginalesDeParticionActual);
			list_iterate(estoNoFuncaNiAPalo, (void *)guardarEnBuffer);

			ingresarNuevaInfo(rutaParticion, arrayDeBloques, sizeParticion, &bufferFinal);



			//eliminar archivos originales
			//nueva info.....
		//crearArchivoConRegistrosACompactar(rutaTabla);
			liberarDoblePuntero(arrayDeBloques);
			liberarDoblePuntero(separarRegistro);
		}
	list_destroy_and_destroy_elements(listaRegistrosTemporalesDeParticionActual,(void*)liberarRegistros);
	config_destroy(tabla);
	free(numeroDeParticion);
	free(rutaParticion);
	free(bufferBloques);
	free(bufferParticion);
	free(bufferFinal);
	free(bufferTemporales);
	}

	config_destroy(configMetadata);
	free(rutaMetadata);
	list_destroy_and_destroy_elements(listaRegistrosOriginalesDeParticionActual,(void*)liberarRegistros);
}

void compactar(char* nombreTabla){

	int i;
	char* bufferTemporales = string_new();
	char* rutaTabla = string_new();

//	t_config* archivoTmp;
	t_list* listaRegistrosTemporales = list_create();
	//liberar la lista

	int numeroTmp = obtenerCantTemporales(nombreTabla);

	string_append(&rutaTabla, puntoMontaje);
	string_append(&rutaTabla, "Tables/");
	string_append(&rutaTabla, nombreTabla);
	string_append(&rutaTabla, "/");


	for (i = 0; i< numeroTmp; i++){


		char* rutaTmpOriginal = string_new();
		char* rutaTmpCompactar= string_new();
		char* numeroDeTmp = string_itoa(i);
		string_append(&rutaTmpOriginal, rutaTabla);
		string_append(&rutaTmpOriginal, numeroDeTmp);

		string_append(&rutaTmpOriginal,".tmp");
		string_append(&rutaTmpCompactar, rutaTabla);
		string_append(&rutaTmpCompactar, numeroDeTmp);
		string_append(&rutaTmpCompactar,".tmpc");


		//con esto despues se puede verificar que no se pueda hacer esto
		int cambiarNombre = rename(rutaTmpOriginal, rutaTmpCompactar);


		t_config* archivoTmp = config_create(rutaTmpCompactar);



		if(cambiarNombre){
			enviarYLogearMensajeError(-1,"No se pudo renombrar el archivo tmp");
			continue;
		}

		//leer bloques

		char** arrayDeBloques = config_get_array_value(archivoTmp,"BLOCKS");
		int sizeParticion=config_get_int_value(archivoTmp,"SIZE");
		config_destroy(archivoTmp);

		cargarInfoDeBloquesParaCompactacion(&bufferTemporales, arrayDeBloques, sizeParticion);

		remove(rutaTmpCompactar);
		free(rutaTmpOriginal);
		free(rutaTmpCompactar);
		free(numeroDeTmp);

		liberarDoblePuntero(arrayDeBloques);
	}

	char** separarRegistro = separarRegistrosDeBuffer(bufferTemporales, listaRegistrosTemporales);
	insertarInfoEnBloquesOriginales(rutaTabla, listaRegistrosTemporales);

	list_destroy_and_destroy_elements(listaRegistrosTemporales,(void*)liberarRegistros);
	free(rutaTabla);
	free(bufferTemporales);

//	string_append(bufferRegistrosTemporales, infoDeTmps);

}
