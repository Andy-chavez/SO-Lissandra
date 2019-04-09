/*
 * conexiones.h
 *
 *  Created on: 9 abr. 2019
 *      Author: utnso
 */

#ifndef CONEXIONES_H_
#define CONEXIONES_H_

int crearSocketCliente(char *ip,char *puerto);
int crearSocketServidor(char *ip, char *puerto);
int cerrarConexion(int unSocket);

#endif /* CONEXIONES_H_ */
