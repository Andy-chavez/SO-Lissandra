################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../commonsPropias/conexiones/conexiones.c 

OBJS += \
./commonsPropias/conexiones/conexiones.o 

C_DEPS += \
./commonsPropias/conexiones/conexiones.d 


# Each subdirectory must supply rules for building sources it contributes
commonsPropias/conexiones/%.o: ../commonsPropias/conexiones/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


