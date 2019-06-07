/*
 * dump.h
 *
 *  Created on: 31 may. 2019
 *      Author: utnso
 */
#include "funcionesLFS.h"
#include <semaphore.h>

char* crearTemporal(int size ,int cantidadDeBloques,char* nombreTabla) {

	char* rutaTmp = string_new();
	int numeroTmp = obtenerCantTemporales(nombreTabla);

	string_append(&rutaTmp, puntoMontaje);
	string_append(&rutaTmp, "Tables/");
	string_append(&rutaTmp, nombreTabla);
	string_append(&rutaTmp, "/");
	string_append(&rutaTmp, string_itoa(numeroTmp));
	string_append(&rutaTmp,".tmp");

	char* info = string_new();
	string_append(&info,"SIZE=");
	string_append(&info,string_itoa(size));
	string_append(&info,"\n");
	string_append(&info,"BLOCKS=[");

	while(cantidadDeBloques>0){
		char* bloqueLibre = devolverBloqueLibre();
		string_append(&info,bloqueLibre);
		cantidadDeBloques--;
		if(cantidadDeBloques>0) string_append(&info,","); //si es el ultimo no quiero que me ponga una ,
	}
	string_append(&info,"]");
	guardarInfoEnArchivo(rutaTmp, info);

	free(info);
	return rutaTmp;
}


void dump(){
	while(1){
	sleep(tiempoDump);
	pthread_mutex_lock(&mutexLogger);
	int tamanioTotalADumpear =0;
	char* buffer;
	void cargarRegistro(registro* unRegistro){


		char* time = string_itoa(unRegistro->timestamp);
		char* key = string_itoa(unRegistro->key);

		string_append(&buffer,time);
		string_append(&buffer,";");
		string_append(&buffer,key);
		string_append(&buffer,";");
		string_append(&buffer,unRegistro->value);
		string_append(&buffer,"\n");

		free(time);
		free(key);

	}

	void dumpearTabla(tablaMem* unaTabla){
	//puts("hola");
	buffer = string_new();
	list_iterate(unaTabla->listaRegistros,(void*)cargarRegistro); //while el bloque no este lleno, cantOcupada += lo que dumpeaste
	puts(buffer);

		tamanioTotalADumpear = strlen(buffer);
		pthread_mutex_lock(&mutexLogger);
		log_info(logger,"Creando tmp");
		pthread_mutex_unlock(&mutexLogger);

		int cantBloquesNecesarios =  ceil((float) (tamanioTotalADumpear/ (float) tamanioBloques));
		char* rutaTmp = crearTemporal(tamanioTotalADumpear,cantBloquesNecesarios,unaTabla->nombre);
		pthread_mutex_lock(&mutexLogger);
		log_info(logger,"Se creo el tmp en la ruta: %s",rutaTmp);
		pthread_mutex_unlock(&mutexLogger);


		t_config* temporal =config_create(rutaTmp);
		char** bloquesAsignados= config_get_array_value(temporal,"BLOCKS");

		int desplazamiento=0;
		int restante = tamanioTotalADumpear;
		int i;
		int j=0; //esto es para no desperdiciar espacio
		for(i=0; i<cantBloquesNecesarios;i++){
			j= i+1; //osea el proximo

			if(*(bloquesAsignados+j) == NULL){ //osea si es el ultimo bloque

				char* rutaBloque = string_new();
				string_append(&rutaBloque,puntoMontaje);
				string_append(&rutaBloque,"Bloques/");
				string_append(&rutaBloque,*(bloquesAsignados+i));
				string_append(&rutaBloque,".bin");
				FILE* fd = fopen(rutaBloque,"w");
				fwrite(buffer+desplazamiento,1,restante,fd);
				fclose(fd);
				break;
			}

			char* rutaBloque = string_new();
			string_append(&rutaBloque,puntoMontaje);
			string_append(&rutaBloque,"Bloques/");
			string_append(&rutaBloque,*(bloquesAsignados+i)); //este es el numero de bloque donde escribo
			string_append(&rutaBloque,".bin");
			FILE* fd = fopen(rutaBloque,"w");
			fwrite(buffer+desplazamiento,1,tamanioBloques,fd);
			desplazamiento+= tamanioBloques;
			restante-=tamanioBloques;
			fclose(fd);
			free(rutaBloque);
		}
		config_destroy(temporal);
		free(buffer);
		liberarDoblePuntero(bloquesAsignados);
	}

	pthread_mutex_lock(&mutexMemtable);
	list_iterate(memtable,(void*)dumpearTabla);
	pthread_mutex_unlock(&mutexMemtable);

	funcionSelect("PELICULAS 10");
	liberarMemtable();
	pthread_mutex_unlock(&mutexMemtable);

	pthread_mutex_unlock(&mutexDump);
	}

}

