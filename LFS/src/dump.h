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
	char* tamanio = string_itoa(size);
	string_append(&info,"SIZE=");
	string_append(&info,tamanio);
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
	free(tamanio);
	free(numeroTmp);
	free(info);
	return rutaTmp;
}


void dump(){


	while(1){
		int tiempoActual=0;


		pthread_mutex_lock(&mutexTiempoDump);
		tiempoActual = tiempoDump;
		pthread_mutex_unlock(&mutexTiempoDump);
		//pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
		usleep(tiempoActual*1000);
		puts("DUMP: HOLAAAA");

//deadlock en mutex memtable
	//	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);
		pthread_mutex_lock(&mutexMemtable);
		int cantElementos =memtable->elements_count;
		pthread_mutex_unlock(&mutexMemtable);
		if(cantElementos==0){
			puts("DUMP: MEMTABLE VACIA");
//			log_info(loggerResultadosConsola,"ELEMENTOS=0");
			continue;
		}

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

		log_info(loggerResultadosConsola,"DUMP: EMPEZANDO DUMP");


		pthread_mutex_t semaforoDeTablaFS = devolverSemaforoDeTablaFS(unaTabla->nombre);
		pthread_mutex_t semaforoDeTablaMemtable = devolverSemaforoDeTablaMemtable(unaTabla->nombre);
		pthread_mutex_lock(&semaforoDeTablaMemtable);
		list_iterate(unaTabla->listaRegistros,(void*)cargarRegistro); //while el bloque no este lleno, cantOcupada += lo que dumpeaste

			soloLoggear(-1,"Dumpeando tabla: %s", unaTabla->nombre);

			tamanioTotalADumpear = strlen(buffer);
			soloLoggear(-1,"Creando tmp");

			int cantBloquesNecesarios =  ceil((float) (tamanioTotalADumpear/ (float) tamanioBloques));
			pthread_mutex_lock(&semaforoDeTablaFS);
			char* rutaTmp = crearTemporal(tamanioTotalADumpear,cantBloquesNecesarios,unaTabla->nombre);

			soloLoggear(-1,"Se creo el tmp en la ruta: %s",rutaTmp);

			t_config* temporal =config_create(rutaTmp);
			char** bloquesAsignados= config_get_array_value(temporal,"BLOCKS");
			config_destroy(temporal);
			free(rutaTmp);

			guardarRegistrosEnBloques(tamanioTotalADumpear, cantBloquesNecesarios, bloquesAsignados, buffer);

			puts("DUMP: GUARDIOLA LA DATA");

			soloLoggear(-1,"Finalizado dumpeo de: %s", unaTabla->nombre);
			log_info(loggerResultadosConsola,"SE DUMPEO");


			pthread_mutex_unlock(&semaforoDeTablaFS);

			free(buffer);
			liberarDoblePuntero(bloquesAsignados);

			bool tablaActual(tablaMem* unaTablita){
				return (string_equals_ignore_case(unaTablita->nombre,unaTabla->nombre));
			}

			pthread_mutex_lock(&mutexMemtable);
			list_remove_and_destroy_by_condition(memtable, tablaActual, liberarTablaMem);
			pthread_mutex_unlock(&mutexMemtable);

			pthread_mutex_unlock(&semaforoDeTablaMemtable);


		}

		list_iterate(memtable,(void*)dumpearTabla);

		//liberarPorTablas

//		liberarMemtable();



	}
}
