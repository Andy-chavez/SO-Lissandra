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


		sem_wait(&mutexTiempoDump);
		tiempoActual = tiempoDump;
		sem_post(&mutexTiempoDump);
		//pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
		usleep(tiempoActual*1000);

//deadlock en mutex memtable
	//	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);
		sem_wait(&mutexMemtable);
		int cantElementos =memtable->elements_count;
		sem_post(&mutexMemtable);
		if(cantElementos==0){
//			log_info(loggerResultadosConsola,"ELEMENTOS=0");
			continue;
		}

		int tamanioTotalADumpear =0;



		void dumpearTabla(tablaMem* unaTabla){
			char* buffer = string_new();
			int cantidadRegistrosDumpeados = 0;
			void cargarRegistro(registro* unRegistro){

				char* time = string_itoa(unRegistro->timestamp);
				char* key = string_itoa(unRegistro->key);

				soloLoggear(-1, "Se agrega al buffer de dump el registro %d %s %d", unRegistro->key, unRegistro->value, unRegistro->timestamp);

				string_append(&buffer,time);
				string_append(&buffer,";");
				string_append(&buffer,key);
				string_append(&buffer,";");
				string_append(&buffer,unRegistro->value);
				string_append(&buffer,"\n");

				cantidadRegistrosDumpeados++;
				free(time);
				free(key);

			}
			//sem_post(&mutexMemtable);
			bool tablaActual(tablaMem* unaTablita){
				return (string_equals_ignore_case(unaTablita->nombre,unaTabla->nombre));
			}
			soloLoggearResultados(-1,0,"DUMP: EMPEZANDO DUMP");
			/*if(!verificarExistenciaDirectorioTabla(unaTabla->nombre,-1)){
				soloLoggearError(-1,"No se pudo dumpear los registros porque la tabla %s fue dropeado. Se procedera a eliminar los registros de la memtable",unaTabla->nombre);
				sem_wait(&mutexMemtable);
				list_remove_and_destroy_by_condition(memtable, tablaActual, liberarTablaMem);
				return;
			}*/
			sem_t *semaforoDeTablaFS = devolverSemaforoDeTablaFS(unaTabla->nombre);
			sem_t *semaforoDeTablaMemtable = devolverSemaforoDeTablaMemtable(unaTabla->nombre);
			sem_wait(semaforoDeTablaMemtable);
			list_iterate(unaTabla->listaRegistros,(void*)cargarRegistro); //while el bloque no este lleno, cantOcupada += lo que dumpeaste

			soloLoggear(-1,"Dumpeando tabla: %s", unaTabla->nombre);
			soloLoggear(-1, "Se dumpearan %d registros.", cantidadRegistrosDumpeados);

			tamanioTotalADumpear = strlen(buffer);
			soloLoggear(-1,"Creando tmp");

			int cantBloquesNecesarios =  ceil((float) (tamanioTotalADumpear/ (float) tamanioBloques));
			sem_wait(semaforoDeTablaFS);
			char* rutaTmp = crearTemporal(tamanioTotalADumpear,cantBloquesNecesarios,unaTabla->nombre);

			soloLoggear(-1,"Se creo el tmp en la ruta: %s",rutaTmp);

			t_config* temporal =config_create(rutaTmp);
			char** bloquesAsignados= config_get_array_value(temporal,"BLOCKS");
			config_destroy(temporal);
			free(rutaTmp);

			guardarRegistrosEnBloques(tamanioTotalADumpear, cantBloquesNecesarios, bloquesAsignados, buffer);

			soloLoggear(-1,"Finalizado dumpeo de: %s", unaTabla->nombre);

			sem_post(semaforoDeTablaFS);
			free(buffer);

			liberarDoblePuntero(bloquesAsignados);

			//sem_wait(&mutexMemtable);
			list_remove_and_destroy_by_condition(memtable, tablaActual, liberarTablaMem);
			sem_post(semaforoDeTablaMemtable);

		}
		//sem_wait(&mutexMemtable);
		list_iterate(memtable,(void*)dumpearTabla);
		//sem_post(&mutexMemtable);
		//liberarPorTablas

//		liberarMemtable();



	}
}
