################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/compactador.c \
../src/conexiones.c \
../src/fileSystem.c \
../src/lisandra_file_system.c 

OBJS += \
./src/compactador.o \
./src/conexiones.o \
./src/fileSystem.o \
./src/lisandra_file_system.o 

C_DEPS += \
./src/compactador.d \
./src/conexiones.d \
./src/fileSystem.d \
./src/lisandra_file_system.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/commonsPropias" -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


