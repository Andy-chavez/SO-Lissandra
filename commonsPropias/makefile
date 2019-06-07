# Makefile template for shared library, Ya veremos de hacer uno nuestro JAJAJAJA

CC = gcc # C compiler
CFLAGS = -fPIC -Wall -Wextra -O2 -g # C flags
LDFLAGS = -shared  # linking flags
RM = rm -f  # rm command
TARGET_LIB = libcommonsPropias.so # target lib

SRCS = commonsPropias/conexiones.c commonsPropias/serializacion.c # source files
HSRCS = $(SRCS:.c=.h)
OBJS = $(SRCS:.c=.o)

.PHONY: all
all: ${TARGET_LIB}

$(TARGET_LIB): $(OBJS)
	$(CC) ${LDFLAGS} -o $@ $^

$(SRCS:.c=.d):%.d:%.c
	$(CC) $(CFLAGS) -MM $< >$@

include $(SRCS:.c=.d)

.PHONY: clean
clean:
	-${RM} ${TARGET_LIB} ${OBJS} $(SRCS:.c=.d)

install: all
	cp -u ${TARGET_LIB} /usr/lib
	cp --parents -u ${HSRCS} /usr/include
	
uninstall:
	rm -f /usr/lib/libcommonsPropias.so
	rm -rf /usr/include/commonsPropias