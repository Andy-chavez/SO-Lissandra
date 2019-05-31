/*
 * dump.h
 *
 *  Created on: 31 may. 2019
 *      Author: utnso
 */

#ifndef SRC_DUMP_H_
#define SRC_DUMP_H_
#include "funcionesLFS.h"


void crearTemporal(int size ,int cantidadDeBloques,char* nombreTabla) {

	char* bloqueLibre;
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
	free(rutaTmp);
}

void dump(){
	int tamanioTotalADumpear =0;
	int cantBytesDumpeados = 0;
	char* buffer = string_new();
	void cargarRegistros(registro* unRegistro){
		char* time = atoi(unRegistro->timestamp);
		char* key = atoi(unRegistro->key);
		string_append(&buffer,time);
		string_append(&buffer,";");
		string_append(&buffer,key);
		string_append(&buffer,";");
		string_append(&buffer,unRegistro->value);
		string_append(&buffer,"\n");

	}

	void dumpearTabla(tablaMem* unaTabla){
		tamanioTotalADumpear = tamanioRegistros(unaTabla->nombre); //56 y los bloques 30
		list_iterate(unaTabla->listaRegistros,(void*)cargarRegistros); //while el bloque no este lleno, cantOcupada += lo que dumpeaste
		char* rutaBloque = string_new();
		log_info(logger,"Creando tmp");
		//tamanioTotalADumpear = 120
		//tamanioBloques = 64
		//cantBloques=2



		int cantBloquesNecesarios = (int) round(tamanioTotalADumpear/tamanioBloques);
		crearTemporal(tamanioTotalADumpear,cantBloquesNecesarios,unaTabla->nombre);


		string_append(&rutaBloque,puntoMontaje);
		string_append(&rutaBloque,"Bloques/");
		char* numeroBloque = devolverBloqueLibre();
		string_append(&rutaBloque,numeroBloque);
		string_append(&rutaBloque,".bin"); //el primer bloque siempre se asigna
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
		free(rutaBloque);
		free(buffer);
	}

	list_iterate(memtable,(void*)dumpearTabla);
	liberarMemtable();
}



#endif /* SRC_DUMP_H_ */
