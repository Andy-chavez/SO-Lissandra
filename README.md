# tp-2019-1c-Why-are-you-running-
Recordar ejecutar el script antes para la creacion de las carpetas

## mensajes de serializacion que se necesita para cada flujo

### INSERT

de Kernel a Memoria -> OperacionLQL

de Memoria a LFS -> OperacionLQL

de LFS a Memoria -> registroConNombreTabla (veremos si en memoria podemos hacer algo para no tener que mandar un registro error)

de Memoria a Kernel -> String

### SELECT

de Kernel a Memoria -> OperacionLQL

de Memoria a LFS -> OperacionLQL

de LFS a Memoria -> registroConNombreTabla (veremos si en memoria podemos hacer algo para no tener que mandar un registro error)

de Memoria a Kernel -> String

### CREATE

de Kernel a Memoria -> OperacionLQL

de Memoria a LFS -> OperacionLQL

de LFS a Memoria -> String

de Memoria a Kernel -> String

### DESCRIBE (Una tabla)

de Kernel a Memoria -> OperacionLQL

de Memoria a LFS -> OperacionLQL

de LFS a Memoria -> metadata

de Memoria a Kernel -> metadata

### DESCRIBE ALL 

de Kernel a Memoria -> OperacionLQL

de Memoria a LFS -> OperacionLQL

de LFS a Memoria -> paquete de metadata

de Memoria a Kernel -> paquete de metadata

### DROP
de Kernel a Memoria -> OperacionLQL

de Memoria a LFS -> OperacionLQL

de LFS a Memoria -> String

de Memoria a Kernel -> String


### JOURNAL
de Kernel a Memoria -> OperacionLQL

de Memoria a LFS -> paquete de operacionLQL (INSERTS)

de LFS a Memoria -> paquete de Strings (Información de cada insert que se realizó)

de Memoria a Kernel -> String

## HANDSHAKES

### LFS <-> MEMORIA
de Memoria a LFS -> hello how are you (enviar cualquier cosa, es para decir hola nomas)

de LFS a Memoria -> tamaño Value

### KERNEL <-> MEMORIA
de Kernel a Memoria -> hello how are you (enviar cualquier cosa, es para decir hola nomas)

de Memoria a Kernel -> TABLA GOSSIP (Serialización in progress)
