################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../commonsPropias/conexiones.c \
../commonsPropias/serializacion.c 

OBJS += \
./commonsPropias/conexiones.o \
./commonsPropias/serializacion.o 

C_DEPS += \
./commonsPropias/conexiones.d \
./commonsPropias/serializacion.d 


# Each subdirectory must supply rules for building sources it contributes
commonsPropias/%.o: ../commonsPropias/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


