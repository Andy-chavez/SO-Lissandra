/*
 * serializacion.h
 *
 *  Created on: 15 abr. 2019
 *      Author: utnso
 */

#ifndef SERIALIZACION_H_
#define SERIALIZACION_H_

void* serializarRegistro(registro* unRegistro,char* nombreTabla);
void* serializarOperacion(int unaOperacion, char* stringDeValores);
void* serializarMetadata(metadata* unaMetadata);

#endif /* SERIALIZACION_H_ */
