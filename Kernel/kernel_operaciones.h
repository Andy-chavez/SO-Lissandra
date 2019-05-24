/*
 * kernel_operaciones.h
 *
 *  Created on: 16 may. 2019
 *      Author: utnso
 */

#ifndef KERNEL_OPERACIONES_H_
#define KERNEL_OPERACIONES_H_
#include "structs-basicos.h"
#include <commons/string.h>

/************************************************************************/
void kernel_almacenar_en_cola(char*,char*);
void kernel_agregar_cola_proc_nuevos(char*, char*);
void kernel_run(char* path);
void kernel_obtener_configuraciones(char* path);
void kernel_consola();
void kernel_api(char*,char*);
/* TODO implementaciones
 * los primeros 5 pasarlos a la memoria elegida por el criterio de la tabla
 * run -> ya esta hecho
 * metrics -> variables globales con semaforos
 * journal -> pasarselo a memoria
 * add -> agregar memoria a un criterio @MEMORIA tengo que avisarles?
 *
 */
void kernel_planificador();
void kernel_insert();
void kernel_select();
void kernel_describe();
void kernel_create();
void kernel_drop();
void kernel_journal();
void kernel_metrics();
void kernel_add();

void* kernel_cliente(void *archivo);
#endif /* KERNEL_OPERACIONES_H_ */
