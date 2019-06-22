/*
 * compactador.h
 *
 *  Created on: 31 may. 2019
 *      Author: utnso
 */

#include "utils.h"
#include <stdio.h>


bool cargarInfoDeBloquesParaCompactacion(char** buffer, char**arrayDeBloques){
	int i = 0;
		while(*(arrayDeBloques+i)!= NULL){
			//el archivo esta vacio
			if(infoEnBloque(*(arrayDeBloques+i)) == NULL){
					return false;
			}else{
				char* informacion = infoEnBloque(*(arrayDeBloques+i));
				string_append(buffer, informacion);
//				free(informacion);
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

int calcularLargoArrayDeBloques(char** arrayDeBloques){
	int cantidad=0;
	while(*(arrayDeBloques+cantidad)!=NULL){
		cantidad++;
	}
	return cantidad;
}

void actualizarBloquesEnArchivo(char* rutaParticion,int cantidadTotalNecesaria,int size){
//aca entras cuando ya necesitas mas bloques de los que ya tenes
	char* infoAGuardar = string_new();

				char* tamanioTotal = string_itoa(size);
				string_append(&infoAGuardar, "SIZE=");

				string_append(&infoAGuardar,tamanioTotal);
				string_append(&infoAGuardar, "\n");
				string_append(&infoAGuardar, "BLOCKS=[");
				free(tamanioTotal);

				for(int j=0;j=cantidadTotalNecesaria;j++){
						char* bloqueLibre = devolverBloqueLibre();
						string_append(&infoAGuardar,bloqueLibre);
						cantidadTotalNecesaria--;
						free(bloqueLibre);
						if(cantidadTotalNecesaria>0) string_append(&infoAGuardar,","); //si es el ultimo no quiero que me ponga una ,
				}
				string_append(&infoAGuardar, "]");

//remove(rutaParticion);
guardarInfoEnArchivo(rutaParticion, infoAGuardar);
free(infoAGuardar);
}


void ingresarNuevaInfo(char* rutaParticion, char* buffer, char** arrayDeBloques){
	int tamanioDelBuffer = strlen(buffer);
	char* size = string_itoa(tamanioDelBuffer);
	//free del array de bloques
	int cantBloquesNecesarios =  ceil((float) (tamanioDelBuffer/ (float) tamanioBloques));
	//int largoDeArrayDeBloques =calcularLargoArrayDeBloques(arrayDeBloques);

	//si solo se necesita un bloque se guarda en el unico bloque que se le asigno a la particion al cargar la tabla
	//aca no

		actualizarBloquesEnArchivo(rutaParticion,cantBloquesNecesarios,tamanioDelBuffer);
		t_config* particion = config_create(rutaParticion);

		char** arrayDeBloquesFinal = config_get_array_value(particion, "BLOCKS");
		guardarRegistrosEnBloques(tamanioDelBuffer, cantBloquesNecesarios, arrayDeBloquesFinal, buffer);
		config_set_value(particion, "SIZE", size);


		config_destroy(particion);
	liberarDoblePuntero(arrayDeBloquesFinal);
	free(size);
}

bool seEncuentraElRegistroYTieneLaMismaKey(registro* registroOriginal, registro* registroTemporal){
	return registroOriginal->key == registroTemporal->key;
}

void agregadoYReemplazoDeRegistros(t_list* listaRegistrosTemporalesDeParticionActual, t_list* listaRegistrosOriginalesDeParticionActual){

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

//	return listaRegistrosOriginalesDeParticionActual;
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
bool perteneceAParticion(int suParticion,int particionActual){
	return (suParticion == particionActual);
}
//bloques de particion. reemplazar todo lo que dice original por particion.. actualizar particion
void insertarInfoEnBloquesOriginales(char* rutaTabla, t_list* listaRegistrosTemporales){

	char* rutaMetadata = string_new();

	string_append(&rutaMetadata,rutaTabla);
	string_append(&rutaMetadata,"/Metadata");

	t_config* configMetadata = config_create(rutaMetadata);

	int cantParticiones = config_get_int_value(configMetadata, "PARTITIONS");

	for (int i = 0; i< cantParticiones; i++){

	//	t_list* listaDeRegistrosTemporales = list_create();
	//	listaDeRegistrosTemporales = listaRegistrosTemporales;

		char* bufferParticion = string_new();
		char* rutaParticion = string_new();
		char* bufferFinal = string_new();
		char* bufferTemporales = string_new();
		char* numeroDeParticion =string_itoa(i);

		string_append(&rutaParticion, rutaTabla);
		string_append(&rutaParticion,"part"); //vamos a usar la convension PartN.bin
		string_append(&rutaParticion, numeroDeParticion);
		string_append(&rutaParticion,".bin");

		t_config* tabla = config_create(rutaParticion);
		char** arrayDeBloques = config_get_array_value(tabla,"BLOCKS");

		bool tieneLaKey(registro* unRegistro){
			int suParticion = calcularParticion(unRegistro->key,cantParticiones);
			return perteneceAParticion(suParticion,i);
		}

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

		if (listaRegistrosTemporalesDeParticionActual->elements_count == 0){
								list_destroy(listaRegistrosTemporalesDeParticionActual);
								config_destroy(tabla);
								free(numeroDeParticion);
								free(rutaParticion);
								free(bufferParticion);
								free(bufferFinal);
								free(bufferTemporales);
								liberarDoblePuntero(arrayDeBloques);
					continue;
		}

//se liberan los bloques del array de bloques
		marcarBloquesComoLibre(arrayDeBloques);



		//el array de bloques no se pasa, hay que liberar los viejos
		if (!cargarInfoDeBloquesParaCompactacion(&bufferParticion, arrayDeBloques)){
			//particion vacía y hay temporales
			//meter una lista de registros temporales en un buffer
			cargarListaEnBuffer(listaRegistrosTemporalesDeParticionActual, &bufferTemporales);
			ingresarNuevaInfo(rutaParticion, bufferTemporales, arrayDeBloques );

			//break porque no puede haber una particion que tenga data si la anterior no tenia data
			//puede ser q sea continue

			list_destroy(listaRegistrosTemporalesDeParticionActual);
			//list_destroy_and_destroy_elements(listaRegistrosTemporalesDeParticionActual,(void*)liberarRegistros);
			config_destroy(tabla);
			free(numeroDeParticion);
			free(rutaParticion);
			free(bufferParticion);
			free(bufferFinal);
			free(bufferTemporales);
			liberarDoblePuntero(arrayDeBloques);
			continue;

		}else{
			//la particion tiene registros y hay registros temporales para actualizar o agregar

			//Tenemos que hacer esto porque si no se eliminan los nodos temporales que se van a usar despues

			void liberarRegistrosNoTemporales(registro* unRegistro){

				bool seEncuentraKey(registro* registroTemporal){
					return (registroTemporal->key == unRegistro->key);
				}


				list_remove_and_destroy_by_condition(listaRegistrosTemporales, seEncuentraKey, liberarRegistros);
			/*	if(list_remove_by_condition(listaRegistrosTemporales, seEncuentraKey)){
					liberarRegistros(unRegistro);
				}
			*/
			}

			t_list* listaRegistrosOriginalesDeParticionActual = list_create();
			separarRegistrosYCargarALista(bufferParticion, listaRegistrosOriginalesDeParticionActual);
	//		t_list* listaRegistrosFinal = list_create();

			agregadoYReemplazoDeRegistros(listaRegistrosTemporalesDeParticionActual, listaRegistrosOriginalesDeParticionActual);
			list_iterate(listaRegistrosOriginalesDeParticionActual, (void *)guardarEnBuffer);

			/*
			t_list* listaRegistrosFinal = agregadoYReemplazoDeRegistros(listaRegistrosTemporalesDeParticionActual, listaRegistrosOriginalesDeParticionActual);
			list_iterate(listaRegistrosFinal, (void *)guardarEnBuffer);
*/
			ingresarNuevaInfo(rutaParticion, bufferFinal, arrayDeBloques);

			list_destroy_and_destroy_elements(listaRegistrosOriginalesDeParticionActual,(void*)liberarRegistrosNoTemporales);
//			list_destroy(listaRegistrosFinal);
		}
	list_destroy(listaRegistrosTemporalesDeParticionActual);
	//list_destroy_and_destroy_elements(listaRegistrosTemporalesDeParticionActual,(void*)liberarRegistros);

	config_destroy(tabla);
	free(numeroDeParticion);
	free(rutaParticion);
	free(bufferParticion);
	free(bufferFinal);
	free(bufferTemporales);
	liberarDoblePuntero(arrayDeBloques);
	}

	config_destroy(configMetadata);
	free(rutaMetadata);
}

void compactar(char* nombreTabla){

	//aca habria que poner un while 1
	int i;
	int numeroTmp = obtenerCantTemporales(nombreTabla);
	/*
	if(numeroTmp == 0){
		continue;
	}
	*/
	char* bufferTemporales = string_new();
	char* rutaTabla = string_new();

//	t_config* archivoTmp;
	t_list* listaRegistrosTemporales = list_create();
	//liberar la lista

	string_append(&rutaTabla, puntoMontaje);
	string_append(&rutaTabla, "Tables/");
	string_append(&rutaTabla, nombreTabla);
	string_append(&rutaTabla, "/");

	for (i = 0; i< numeroTmp; i++){


		char* rutaTmpOriginal = string_new();
		char* rutaTmpCompactar= string_new();
		char* nombreDelTmpc = string_new();

		char* numeroDeTmp = string_itoa(i);
		string_append(&rutaTmpOriginal, rutaTabla);
		string_append(&rutaTmpOriginal, numeroDeTmp);

		string_append(&rutaTmpOriginal,".tmp");
		string_append(&rutaTmpCompactar, rutaTabla);
		string_append(&rutaTmpCompactar, numeroDeTmp);
		string_append(&rutaTmpCompactar,".tmpc");

		string_append(&nombreDelTmpc, numeroDeTmp);
		string_append(&nombreDelTmpc,".tmpc");


		//con esto despues se puede verificar que no se pueda hacer esto
		int cambiarNombre = rename(rutaTmpOriginal, rutaTmpCompactar);


		t_config* archivoTmp = config_create(rutaTmpCompactar);

		if(cambiarNombre){
			enviarYLogearMensajeError(-1,"No se pudo renombrar el archivo tmp");
			free(rutaTmpOriginal);
			free(rutaTmpCompactar);
			free(numeroDeTmp);
			free(nombreDelTmpc);
			config_destroy(archivoTmp);
			continue;
		}

		//leer bloques
		char** arrayDeBloques = config_get_array_value(archivoTmp,"BLOCKS");
		config_destroy(archivoTmp);

		cargarInfoDeBloquesParaCompactacion(&bufferTemporales, arrayDeBloques);

		liberarBloquesDeTmpYPart(nombreDelTmpc, rutaTabla);

		remove(rutaTmpCompactar);
		free(rutaTmpOriginal);
		free(rutaTmpCompactar);
		free(numeroDeTmp);
		free(nombreDelTmpc);

		liberarDoblePuntero(arrayDeBloques);
	}

	separarRegistrosYCargarALista(bufferTemporales, listaRegistrosTemporales);
	insertarInfoEnBloquesOriginales(rutaTabla, listaRegistrosTemporales);

	list_destroy_and_destroy_elements(listaRegistrosTemporales,(void*)liberarRegistros);
	free(rutaTabla);
	free(bufferTemporales);

//	string_append(bufferRegistrosTemporales, infoDeTmps);

}
