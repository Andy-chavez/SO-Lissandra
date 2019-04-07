/*
 * servidor.c
 *
 *  Created on: 3 mar. 2019
 *      Author: utnso
 */

#include "servidor.h"

int main(void)
{
	// esto para que es??
	void iterator(char* value)
	{
		printf("%s\n", value);
	}
	logger = log_create("log.log", "Servidor", 1, LOG_LEVEL_DEBUG);

	int servidor = crearConexion();
	log_info(logger, "El servidor esta listo para recibir al cliente");
	int cliente = esperarCliente(servidor);

	t_list* lista;
	while(1)
	{
		int codigoOperacion = recibir_operacion(cliente);
		switch(codigoOperacion)
		{
		case MENSAJE:
			recibir_mensaje(cliente);
			break;
		case PAQUETE:
			lista = recibirPaquete(cliente);
			printf("Llegaron los siguientes valores:\n");
			list_iterate(lista, (void*) iterator);
			break;
		case -1:
			log_error(logger, "Finaliza servidor. Se desconecto el cliente");
			return EXIT_FAILURE;
		default:
			log_warning(logger, "Operacion desconocida");
			break;
		}
	}
	return EXIT_SUCCESS;
}
