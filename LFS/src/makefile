OBJS	:= lisandra_file_system.o
SOURCE	:= lisandra_file_system.c
HEADER	:= compactador.h configuraciones.h dump.h funcionesLFS.h
OUT	:= elLFS.o
CC	 := gcc
FLAGS	 := -g -c -Wall
LFLAGS	 := -lpthread -lcommonsPropias -lcommons -lreadline

all: $(OBJS)
	$(CC) -g $(OBJS) -o $(OUT) $(LFLAGS)

lisandra_file_system.o: lisandra_file_system.c
	$(CC) $(FLAGS) lisandra_file_system.c 

clean:
	rm -f $(OBJS) $(OUT)