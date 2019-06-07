OBJS	:= kernel.o
SOURCE	:= kernel.c
HEADER	:= kernel_configuraciones.h kernel_operaciones.h kernel_structs-basicos.h
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