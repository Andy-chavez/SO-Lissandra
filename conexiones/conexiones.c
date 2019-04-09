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

int crearSocketServidor(char *ip, char *puerto) {
	return 2;
}

int cerrarConexion(int unSocket) {
	close(unSocket);
}
