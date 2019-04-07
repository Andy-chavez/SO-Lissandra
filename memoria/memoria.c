/*
 * memoria.c
 *
 *  Created on: 7 abr. 2019
 *      Author: whyAreYouRunning?
 */


int main() {
	char* instruccion;
	instruccion = leerMensaje();

	ejecutarInstruccion(instruccion);
	free(instruccion);

	return 0;
}


