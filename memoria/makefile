OBJS	:= memoria.o
SOURCE	:= memoria.c
HEADER	:= operacionesMemoria.h structsYVariablesGlobales.h
OUT	:= laMemoria.o
CC	 := gcc
FLAGS	 := -g -c -Wall
LFLAGS	 := -lpthread -lcommonsPropias -lcommons -lreadline

all: $(OBJS)
	$(CC) -g $(OBJS) -o $(OUT) $(LFLAGS)

memoria.o: memoria.c
	$(CC) $(FLAGS) memoria.c 


clean:
	rm -f $(OBJS) $(OUT)
