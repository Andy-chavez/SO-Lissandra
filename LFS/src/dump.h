/*
 * dump.h
 *
 *  Created on: 31 may. 2019
 *      Author: utnso
 */
#include "funcionesLFS.h"


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
	int tamanioTotalADumpear =0;
	//int cantBytesDumpeados = 0;
	char* buffer = string_new();
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
		list_iterate(unaTabla->listaRegistros,(void*)cargarRegistro); //while el bloque no este lleno, cantOcupada += lo que dumpeaste
		tamanioTotalADumpear = strlen(buffer);
		log_info(logger,"Creando tmp");

		int cantBloquesNecesarios = 2;
		//int cantBloquesNecesarios = (int) round(tamanioTotalADumpear/tamanioBloques); //hacer que funque, anda mal el round
		char* rutaTmp = crearTemporal(tamanioTotalADumpear,cantBloquesNecesarios,unaTabla->nombre);
		log_info(logger,"Se creo el tmp numero en la ruta: ",rutaTmp);

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
			}
			char* rutaBloque = string_new();
			string_append(&rutaBloque,puntoMontaje);
			string_append(&rutaBloque,"Bloques/");
			string_append(&rutaBloque,*(bloquesAsignados+i)); //este es el numero de bloque donde escribo
			string_append(&rutaBloque,".bin");
			FILE* fd = fopen(rutaBloque,"w");
			fwrite(buffer+desplazamiento,1,tamanioBloques,fd);
			desplazamiento+= tamanioBloques;
			restante-=tamanioBloques
			fclose(fd);
			free(rutaBloque);
		}
		/*if(tamanioBuffer<=tamanioBloque){
			FILE* fd = fopen(rutaBloque,"w");
			fwrite(buffer,1,tamanioBuffer,fd);
			fclose(fd);
			//terminar tmp
		}
		//130 bytes
		//bloques 64
		//entra la primera vez
		//desplazamiento = 64


		else{
			FILE* fd = fopen(rutaBloque,"w");
			cantBytesDumpeados += tamanioBloques;
			int desplazamiento =0;
			while(cantBytesDumpeados<tamanioBuffer){
				if()
				fwrite(buffer+desplazamiento,tamanioBloques,)
				cantBytesDumpeados += tamanioBloques;
			}
		}*/
		config_destroy(temporal);
		free(buffer);
		liberarDoblePuntero(bloquesAsignados);
	}

	list_iterate(memtable,(void*)dumpearTabla);
	liberarMemtable();
}

