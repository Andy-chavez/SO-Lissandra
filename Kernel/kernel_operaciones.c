
#include "kernel_operaciones.h"

char * path ="../KERNEL_CONFIG_EJEMPLO";
configYLogs *kernel_configYLog;
char* IpMemoria;
char* PuertoMemoria;

void kernel_run(char* path){
	FILE *archivoALeer;
	archivoALeer = fopen(path, "r");
	char *lineaLeida;
	size_t limite = 250;
	ssize_t leer;
	lineaLeida = (char *)malloc(limite * sizeof(char));
	if (archivoALeer == NULL){
		log_error(kernel_configYLog->log,"No se pudo ejecutar el archivo solicitado, verifique su existencia\n");
		exit(EXIT_FAILURE);
	}
		pcb* pcb_auxiliar = malloc(sizeof(pcb));
		pcb_auxiliar->operacion = "RUN";
		pcb_auxiliar->argumentos = path;
		pcb_auxiliar->ejecutado = 1;
		pcb_auxiliar->instrruccion_siguiente =list_create();
		list_add(cola_proc_nuevos,pcb_auxiliar);

	while((leer = getline(&lineaLeida, &limite, archivoALeer)) != -1){
		char** operacion;
		operacion = string_n_split(lineaLeida,2," ");
		//TODO
		//list_add()
	}

	free(pcb_auxiliar);
	fclose(archivoALeer);
}

void kernel_obtener_configuraciones(char* path){
	kernel_configYLog->config = config_create(path);
	kernel_configYLog= malloc(sizeof(configYLogs));
	IpMemoria = config_get_string_value(kernel_configYLog->config ,"IP_MEMORIA");
	PuertoMemoria = config_get_string_value(kernel_configYLog->config,"PUERTO_MEMORIA");
}
//void* kernel_cliente(void *archivo){
//	int socketClienteKernel = crearSocketCliente(IpMemoria,PuertoMemoria);
//	 //aca tengo dudas de si dejar ese enviar o si puedo salir de la funcion
//	 //enviar(socketClienteKernel, string, (strlen(string)+1)*sizeof(char));
//	cerrarConexion(socketClienteKernel); //Deberia de cerrar conexion? }
