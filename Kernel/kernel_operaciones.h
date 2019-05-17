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
void kernel_insert();
void kernel_select();
void kernel_describe();
void kernel_create();
void kernel_drop();
void kernel_journal();
void kernel_run(char* path);
void kernel_metrics();
void kernel_add();

void kernel_obtener_configuraciones(char* path);
void* kernel_cliente(void *archivo);
#endif /* KERNEL_OPERACIONES_H_ */
