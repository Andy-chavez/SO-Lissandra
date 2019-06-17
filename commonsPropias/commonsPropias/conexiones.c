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
#include <stdlib.h>
#include <commons/log.h>

// crea un socket para la comunicacion con un servidor (Dado IP y puerto).
int crearSocketCliente(char *ip, char *puerto) {
	t_log* logger = log_create("conexiones.log", "CONEXIONES", 0, LOG_LEVEL_ERROR);

	int conexionSocket, intentarConexion;
	struct addrinfo hints, *infoDireccion, *iterLista;

	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	if(getaddrinfo(ip, puerto, &hints, &infoDireccion)){
		log_error(logger, "Hubo un problema con la obtencion de datos del servidor.");
		return -1;
	}

	// Iterar por toda la lista de posibles direcciones a las cuales me puedo conectar
	for(iterLista = infoDireccion; iterLista != NULL; iterLista = iterLista->ai_next) {
		if((conexionSocket = socket(iterLista->ai_family, iterLista->ai_socktype, iterLista->ai_protocol)) == -1) continue;
		if((intentarConexion = connect(conexionSocket, iterLista->ai_addr, iterLista->ai_addrlen)) == -1) continue;
		break;
	}

	//Chequear errores
	if(conexionSocket == -1) {
		log_error(logger, "Hubo un error en la creacion del socket");
		freeaddrinfo(infoDireccion);
		log_destroy(logger);
		return -1;
	} else if(intentarConexion == -1) {
		log_error(logger, "Hubo un error en la conexion del socket");
		freeaddrinfo(infoDireccion);
		log_destroy(logger);
		return -1;
	}

	log_destroy(logger);
	freeaddrinfo(infoDireccion);
	return conexionSocket;
}

//crea un servidor que se comunicara con los clientes que se conecten a el (puerto)
int crearSocketServidor(char *ip, char *puerto) {
	t_log* logger = log_create("conexiones.log", "CONEXIONES", 0, LOG_LEVEL_ERROR);

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
		log_error(logger, "Hubo un error en la creacion del socket");
		freeaddrinfo(infoDireccionServidor);
		log_destroy(logger);
		return -1;
	} else if(intentarBindeo == -1) {
		log_error(logger, "Hubo un error en el bindeo del socket");
		freeaddrinfo(infoDireccionServidor);
		log_destroy(logger);
		return -1;
	}

	listen(socketServidor, SOMAXCONN);
	log_info(logger, "El servidor esta listo para escuchar al cliente");

	log_destroy(logger);
	freeaddrinfo(infoDireccionServidor);
	return socketServidor;
}

// acepta un cliente
int aceptarCliente(int unSocketDeServidor) {
	t_log* logger = log_create("conexiones.log", "CONEXIONES", 0, LOG_LEVEL_ERROR);
	struct sockaddr_in direccionCliente;
	int tamanioDireccion = sizeof(struct sockaddr_in);

	int socketDelCliente = accept(unSocketDeServidor, (void*) &direccionCliente, &tamanioDireccion);

	log_info(logger, "Se acepto la conexion de un cliente");

	log_destroy(logger);
	return socketDelCliente;
}

// cierra una conexion
int cerrarConexion(int unSocket) {
	return close(unSocket);
}

// Envia algo de tipo void*, dado un socket y un tamanio a enviar.
void enviar(int unSocket, void* algoAEnviar, int tamanioAEnviar) {
	void *buffer = malloc(sizeof(int) + tamanioAEnviar); // alojar memoria para el mensaje
	void *aux = buffer; // auxiliar
	*((int*)aux) = tamanioAEnviar; // guardar el tamanio
	aux += sizeof(int); // desplazar auxiliar
	memcpy(aux, algoAEnviar, tamanioAEnviar); // copiar el mensaje en el buffer

	send(unSocket, buffer, sizeof(int) + tamanioAEnviar, NULL); // enviarlo
	free(buffer); // liberar mensaje
}

// Recibe algo de tipo void*, dado un socket. si no recibe nada, devuelve un NULL.
void *recibir(int unSocket) {
	void *recibido = malloc(sizeof(int));
	int bytesRecibidos = recv(unSocket, recibido, sizeof(int), MSG_WAITALL); // OJO, el flag dice que esto es bloqueante!

	if(!bytesRecibidos || bytesRecibidos == -1)  {
		free(recibido);
		return NULL;
	}

	int tamanioBufferARecibir = *((int*) recibido);
	free(recibido);
	recibido = malloc(tamanioBufferARecibir);

	int bytesRecibidosTotales = 0;

	while(bytesRecibidosTotales < tamanioBufferARecibir && bytesRecibidos){
		bytesRecibidos = recv(unSocket, (recibido + bytesRecibidosTotales), (tamanioBufferARecibir - bytesRecibidosTotales), MSG_WAITALL); // lo mismo, es bloqueante
		bytesRecibidosTotales += bytesRecibidos;
	}

	if(!bytesRecibidos || bytesRecibidos == -1) {
		free(recibido);
		return NULL;
	}

	return recibido;
}
