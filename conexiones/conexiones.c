/*
 * conexiones.c
 *
 *  Created on: 9 abr. 2019
 *      Author: utnso
 */
#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <string.h>
#include <netdb.h>
#include "conexiones.h"

// crea un socket para la comunicacion con un servidor (Dado IP y puerto).

int crearSocketCliente(char *ip, char *puerto) {
	int conexionSocket, intentarConexion;
	struct addrinfo hints, *infoDireccion, *iterLista;
	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	if(getaddrinfo(ip, puerto, &hints, &infoDireccion)){
		printf("Hubo un problema con la obtencion de datos del servidor.");
		return -1;
	}

	// Iterar por toda la lista de posibles direcciones a las cuales me puedo conectar
	for(iterLista = infoDireccion; iterLista != NULL; iterLista->ai_next) {
		if((conexionSocket = socket(iterLista->ai_family, iterLista->ai_socktype, iterLista->ai_protocol)) == -1) continue;
		if((intentarConexion = connect(conexionSocket, iterLista->ai_addr, iterLista->ai_addrlen)) == -1) continue;
		break;
	}

	//Chequear errores
	if(conexionSocket == -1) {
		printf("Hubo un error en la creacion del socket");
		freeaddrinfo(infoDireccion);
		return -1;
	} else if(intentarConexion == -1) {
		printf("Hubo un error en la conexion del socket");
		freeaddrinfo(infoDireccion);
		return -1;
	}

	freeaddrinfo(infoDireccion);
	return conexionSocket;
}

//crea un servidor que se comunicara con los clientes que se conecten a el (dado IP y puerto)
int crearSocketServidor(char *ip, char *puerto) {

	int socketServidor;
	struct addrinfo hints, *infoDireccionServidor, *lista;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	getaddrinfo(ip, puerto, &hints, &infoDireccionServidor);

			    for (lista=infoDireccionServidor; lista != NULL; lista = lista->ai_next)
			       {
			    	//errores de conexion
			           if ((socketServidor = socket(lista->ai_family, lista->ai_socktype, lista->ai_protocol)) == -1)
			               continue;
			        //
			           if (bind(socketServidor, lista->ai_addr, lista->ai_addrlen) == -1) {
			               cerrarConexion(socketServidor);
			               continue;
			           }
			           break;
			       }

	listen(socketServidor, SOMAXCONN);
	freeaddrinfo(infoDireccionServidor);
	printf("El servidor esta listo para escuchar al cliente");
	return socketServidor;

	//return 2;
}

int cerrarConexion(int unSocket) {
	close(unSocket);
}

//hice un define con una direccion y un puerto cualquiera en el .h para probar
int main(void)
{
	//printf("El servidor esta listo para escuchar al cliente");

	int servidor = crearSocketServidor(IP, PUERTO);
	int cliente = crearSocketCliente(IP, PUERTO);

	return 0;
}
