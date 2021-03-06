/*
 * compactador.h
 *
 *  Created on: 31 may. 2019
 *      Author: utnso
 */

#include "utils.h"
#include <stdio.h>
//hacer logs para compactacion


// ------------------------------------------------------------------------ //
// 1) FUNCIONES USADAS DURANTE LA COMPACTACION//

bool cargarInfoDeBloquesParaCompactacion(char** buffer, char**arrayDeBloques){
	if(infoEnBloque(*arrayDeBloques) == NULL){
			return false;
	}

	int i = 0;

	while(*(arrayDeBloques+i)!= NULL){
		//el archivo esta vacio
		char* informacion = infoEnBloque(*(arrayDeBloques+i));
		if(informacion != NULL) {
			string_append(buffer, informacion);
		};
		munmap((void*) informacion, tamanioBloques);
		i++;
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
	config_destroy(particion);

	liberarDoblePuntero(arrayDeBloquesFinal);
	free(size);
}

bool seEncuentraElRegistroYTieneLaMismaKey(registro* registroOriginal, registro* registroTemporal){
	return registroOriginal->key == registroTemporal->key;
}


void agregadoYReemplazoDeRegistros(t_list* listaAComparar, t_list* listaAActualizar, char* nombreTabla){

	registro* registroACorroborar;

	int i = 0;

	int cantidadRegistrosReemplazados = 0;

	void agregarOReemplazar(registro* registroTemporal){

		bool estaElRegistro(registro* registroOriginal){

			if(seEncuentraElRegistroYTieneLaMismaKey(registroOriginal, registroTemporal)){
				//registro que queda !!!!!!
				cantidadRegistrosReemplazados++;
				registro* registroAReemplazar = devolverMayor(registroOriginal, registroTemporal);
				list_replace(listaAActualizar, i, registroAReemplazar);
				return true;
			}else{
				i++;
				return false;
			}
		}
		i=0;

		if(!(registroACorroborar = list_find(listaAActualizar, estaElRegistro))){
			list_add(listaAActualizar, registroTemporal);
		}

	}

	list_iterate(listaAComparar,(void *) agregarOReemplazar);
	soloLoggear(-1, "Se aplanaron %d registros en la tabla %s.", cantidadRegistrosReemplazados, nombreTabla);
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


// ------------------------------------------------------------------------ //
// 2) REALIZACION DE LA COMPACTACION//


void insertarInfoEnBloquesDeTabla(char* rutaTabla, t_list* listaRegistrosTemporales, char* nombreTabla){

	char* rutaMetadata = string_new();

	string_append(&rutaMetadata,rutaTabla);
	string_append(&rutaMetadata,"/Metadata");

	t_config* configMetadata = config_create(rutaMetadata);

	int cantParticiones = config_get_int_value(configMetadata, "PARTITIONS");

	enviarOMostrarYLogearInfo(-1, "Se leeran los bloques de las particiones de %s", nombreTabla);

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

		//DESTRUIR CABEZA DE LISTAREGTEMPORALESDEPARTICIONACTUAL
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



		if (!cargarInfoDeBloquesParaCompactacion(&bufferParticion, arrayDeBloques)){

			enviarOMostrarYLogearInfo(-1, "La particion esta vacia, se ingresara la nueva informacion");
			//particion vacía y hay temporales
			//meter una lista de registros temporales en un buffer

			cargarListaEnBuffer(listaRegistrosTemporalesDeParticionActual, &bufferTemporales);
			ingresarNuevaInfo(rutaParticion, bufferTemporales, arrayDeBloques );

			enviarOMostrarYLogearInfo(-1, "Se ha ingresado la informacion");


			list_destroy(listaRegistrosTemporalesDeParticionActual);
			//list_destroy_and_destroy_elements(listaRegistrosTemporalesDeParticionActual,(void*)liberarRegistros);
			config_destroy(tabla);
			free(numeroDeParticion);
			free(rutaParticion);
			free(bufferParticion);
			free(bufferFinal);
			free(bufferTemporales);
			//se liberan los bloques del array de bloques
			marcarBloquesComoLibre(arrayDeBloques);
			liberarDoblePuntero(arrayDeBloques);
			continue;

		}else{

			//la particion tiene registros y hay registros temporales para actualizar o agregar

			//Tenemos que hacer esto porque si no se eliminan los nodos temporales que se van a usar despues

			enviarOMostrarYLogearInfo(-1, "La particion tiene informacion, se actualizaran los registros");

			t_list* listaRegistrosDeTablaDeParticionActual = list_create();
			separarRegistrosYCargarALista(bufferParticion, listaRegistrosDeTablaDeParticionActual);
			t_list* listaRegistrosFinal = list_duplicate(listaRegistrosDeTablaDeParticionActual);

			agregadoYReemplazoDeRegistros(listaRegistrosTemporalesDeParticionActual, listaRegistrosFinal, nombreTabla);
			list_iterate(listaRegistrosFinal, (void *)guardarEnBuffer);

			ingresarNuevaInfo(rutaParticion, bufferFinal, arrayDeBloques);

			enviarOMostrarYLogearInfo(-1, "Se ha actualizado la informacion");

			//liberar todos los registros
			list_destroy_and_destroy_elements(listaRegistrosDeTablaDeParticionActual,(void*)liberarRegistros);
			list_destroy(listaRegistrosFinal);

		}
		//se liberan los bloques del array de bloques
		marcarBloquesComoLibre(arrayDeBloques);
		list_destroy(listaRegistrosTemporalesDeParticionActual);

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

void compactar(metadataConSemaforo* metadataDeTabla){

	sem_t *semaforoDeTabla = devolverSemaforoDeTablaFS(metadataDeTabla->nombreTabla);

	while(1){

	//	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
		usleep((metadataDeTabla->tiempoCompactacion)*1000);
	//	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);
		int i;
		sem_wait(semaforoDeTabla);
		int numeroTmp = obtenerCantTemporales(metadataDeTabla->nombreTabla);
		if(numeroTmp == 0){
			sem_post(semaforoDeTabla);
			continue;
		}
		enviarOMostrarYLogearInfo(-1,"Comenzando compactacion de la tabla: %s\n",metadataDeTabla->nombreTabla);

		char* bufferTemporales = string_new();
		char* rutaTabla = string_new();

		//t_config* archivoTmp;
		t_list* listaRegistrosTemporales = list_create();
		//liberar la lista

		string_append(&rutaTabla, puntoMontaje);
		string_append(&rutaTabla, "Tables/");
		string_append(&rutaTabla, metadataDeTabla->nombreTabla);
		string_append(&rutaTabla, "/");

		enviarOMostrarYLogearInfo(-1, "Se leeran los bloques de los archivos temporales de %s\n", metadataDeTabla->nombreTabla);


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
				//config_destroy(archivoTmp);
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
			//pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
		}

		separarRegistrosYCargarALista(bufferTemporales, listaRegistrosTemporales);
		soloLoggear(-1, "Se encontraron %d registros para compactar en la tabla %s\n", listaRegistrosTemporales->elements_count, metadataDeTabla->nombreTabla);
		soloLoggear(-1, "Se insertara la informacion en los bloques de las particiones de la tabla %s", metadataDeTabla->nombreTabla);

		t_list* listaRegistrosTemporalesSinKeyRepetidas = list_create();

		agregadoYReemplazoDeRegistros(listaRegistrosTemporales, listaRegistrosTemporalesSinKeyRepetidas, metadataDeTabla->nombreTabla);

		//Bloques originales -- de tabla

		insertarInfoEnBloquesDeTabla(rutaTabla, listaRegistrosTemporalesSinKeyRepetidas, metadataDeTabla->nombreTabla);

		sem_post(semaforoDeTabla);
			// LIBERAR Y DESTRUIR ELEMENTOS DE LISTAREGISTROSTEMPORALES
		list_destroy(listaRegistrosTemporalesSinKeyRepetidas);
		list_destroy_and_destroy_elements(listaRegistrosTemporales,(void*)liberarRegistros);
		free(rutaTabla);
		free(bufferTemporales);

		soloLoggear(-1,"Compactacion finalizada de la tabla: %s", metadataDeTabla->nombreTabla);
	}
}
