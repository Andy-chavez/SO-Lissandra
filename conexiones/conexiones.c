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
#include <unistd.h>

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

	int socketServidor, intentarBindeo;
	struct addrinfo hints, *infoDireccionServidor, *lista;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	getaddrinfo(ip, puerto, &hints, &infoDireccionServidor);

		for (lista=infoDireccionServidor; lista != NULL; lista = lista->ai_next) {
			//errores de conexion
			if ((socketServidor = socket(lista->ai_family, lista->ai_socktype, lista->ai_protocol)) == -1) continue;
			if ((intentarBindeo = bind(socketServidor, lista->ai_addr, lista->ai_addrlen)) == -1) continue; // no estuy seguro, pero no se necesita cerrar la conexion de algo que no conecto :p
			break;
		}

	if(socketServidor == -1) {
		printf("Hubo un error en la creacion del socket");
		freeaddrinfo(infoDireccionServidor);
		return -1;
	} else if(intentarBindeo == -1) {
		printf("Hubo un error en el bindeo del socket");
		freeaddrinfo(infoDireccionServidor);
		return -1;
	}

	listen(socketServidor, SOMAXCONN);
	freeaddrinfo(infoDireccionServidor);
	printf("El servidor esta listo para escuchar al cliente");
	return socketServidor;
}

int aceptarCliente(int unSocketDeServidor) {
	struct sockaddr_in direccionCliente;
	int tamanioDireccion = sizeof(struct sockaddr_in);

	int socketDelCliente = accept(unSocketDeServidor, (void*) &direccionCliente, &tamanioDireccion);

	printf("Se Conecto cliente");

	return socketDelCliente;
}

int cerrarConexion(int unSocket) {
	return close(unSocket);
}

int main(void)
{

	int servidor = crearSocketServidor(IP, PUERTO);
	int cliente = crearSocketCliente(IP, PUERTO);
	aceptarCliente(servidor);
	return 0;
}
