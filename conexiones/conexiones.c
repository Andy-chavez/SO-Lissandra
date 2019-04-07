#include "servidor.h"
#include <commons/collections/list.h>

int esperarCliente(int socketServidor)
{
	struct sockaddr_in direccionCliente;
	int tamanioDireccion = sizeof(struct sockaddr_in);

	int socketCliente = accept(socketServidor, (void*) &direccionCliente, &tamanioDireccion);

	log_info(logger, "Se conecto un cliente!");

	return socketCliente;
}

int recibirOperacion(int socketCliente)
{
	int codigoOperaciones;
	if(recv(socketCliente, &codigoOperacion, sizeof(int), MSG_WAITALL) != 0)
		return codigoOperacion;
	else
	{
		close(socketCliente);
		return -1;
	}
}

void* recibirBuffer(int* size, int socketCliente)
{
	void * buffer;

	recv(socketCliente, size, sizeof(int), MSG_WAITALL);
	buffer = malloc(*size);
	recv(socketCliente, buffer, *size, MSG_WAITALL);

	return buffer;
}

// Recibe funcion por orden superior
void recibirMensajeYHacer(int socketCliente,void (*hacerAlgo)(char*))
{
	int size;
	char* buffer = recibirBuffer(&size, socketCliente);
	hacerAlgo(buffer);
	free(buffer);
}

//podemos usar la lista de valores para poder hablar del for y de como recorrer la lista
void recibirPaqueteYHacer(int socket_cliente, void(*hacerAlgo)(t_list*))
{
	int size;
	int desplazamiento = 0;
	void * buffer;
	t_list* valores = list_create();
	int tamanio;

	buffer = recibirBuffer(&size, socket_cliente);
	while(desplazamiento < size)
	{
		memcpy(&tamanio, buffer + desplazamiento, sizeof(int));
		desplazamiento+=sizeof(int);
		char* valor = malloc(tamanio);
		memcpy(valor, buffer+desplazamiento, tamanio);
		desplazamiento+=tamanio;
		list_add(valores, valor);
	}
	free(buffer);
	hacerAlgo(valores); // OJO, verificar que los valores no sea NULL
}
