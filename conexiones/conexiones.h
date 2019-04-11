/*
 * conexiones.h
 *
 *  Created on: 9 abr. 2019
 *      Author: utnso
 */

#ifndef CONEXIONES_H_
#define CONEXIONES_H_
//de prueba
#define IP "127.0.0.1"
#define PUERTO "4444"

int crearSocketCliente(char *ip,char *puerto);
int crearSocketServidor(char *ip, char *puerto);
int cerrarConexion(int unSocket);

#endif /* CONEXIONES_H_ */
