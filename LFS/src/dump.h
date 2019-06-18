/*
 * dump.h
 *
 *  Created on: 31 may. 2019
 *      Author: utnso
 */
#include "funcionesLFS.h"

char* crearTemporal(int size ,int cantidadDeBloques,char* nombreTabla) {

	char* rutaTmp = string_new();
	char* numeroTmp = string_itoa(obtenerCantTemporales(nombreTabla));

	string_append(&rutaTmp, puntoMontaje);
	string_append(&rutaTmp, "Tables/");
	string_append(&rutaTmp, nombreTabla);
	string_append(&rutaTmp, "/");
	string_append(&rutaTmp, numeroTmp);
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
		free(bloqueLibre);
		if(cantidadDeBloques>0) string_append(&info,","); //si es el ultimo no quiero que me ponga una ,
	}
	string_append(&info,"]");
	guardarInfoEnArchivo(rutaTmp,info);
	free(numeroTmp);
	free(info);
	return rutaTmp;
}

void  guardarRegistrosEnBloques(int tamanioTotalADumpear, int cantBloquesNecesarios, char** bloquesAsignados, char* buffer) {

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
	liberarDoblePuntero(bloquesAsignados);
}

void dump(){

	/*while(1){
		pthread_mutex_lock(&mutexTiempoDump);
		usleep(tiempoDump*1000);
		pthread_mutex_unlock(&mutexTiempoDump);
		pthread_mutex_lock(&mutexMemtable);
		if(memtable->elements_count==0){
			pthread_mutex_unlock(&mutexMemtable);
			continue;
		}*/

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
		buffer = string_new();
		list_iterate(unaTabla->listaRegistros,(void*)cargarRegistro); //while el bloque no este lleno, cantOcupada += lo que dumpeaste

			tamanioTotalADumpear = strlen(buffer);
			enviarOMostrarYLogearInfo(-1,"Creando tmp");

			int cantBloquesNecesarios =  ceil((float) (tamanioTotalADumpear/ (float) tamanioBloques));
			char* rutaTmp = crearTemporal(tamanioTotalADumpear,cantBloquesNecesarios,unaTabla->nombre);
			pthread_mutex_lock(&mutexLoggerConsola);
			log_info(loggerConsola,"Se creo el tmp en la ruta: %s",rutaTmp);
			pthread_mutex_unlock(&mutexLoggerConsola);


			t_config* temporal =config_create(rutaTmp);
			char** bloquesAsignados= config_get_array_value(temporal,"BLOCKS");

			guardarRegistrosEnBloques(tamanioTotalADumpear, cantBloquesNecesarios, bloquesAsignados, buffer);
			liberarDoblePuntero(bloquesAsignados);
			free(buffer);
			config_destroy(temporal);
		}
		list_iterate(memtable,(void*)dumpearTabla);
		liberarMemtable();

		pthread_mutex_unlock(&mutexMemtable);

	}
//}
