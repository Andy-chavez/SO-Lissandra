OBJS	:= kernel.o
SOURCE	:= kernel.c
HEADER	:= ker_configuraciones.h ker_operaciones.h ker_structs.h ker_auxiliares.h
OUT	:= elKernel.o
CC	 := gcc
FLAGS	 := -g -c -Wall
LFLAGS	 := -lpthread -lcommonsPropias -lcommons -lreadline

all: $(OBJS)
	$(CC) -g $(OBJS) -o $(OUT) $(LFLAGS)

kernel.o: kernel.c
	$(CC) $(FLAGS) kernel.c 


clean:
	rm -f $(OBJS) $(OUT)