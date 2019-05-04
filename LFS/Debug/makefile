################################################################################
# Automatically-generated file. Do not edit!
################################################################################

-include ../makefile.init

RM := rm -rf

# All of the sources participating in the build are defined here
-include sources.mk
-include src/subdir.mk
-include subdir.mk
-include objects.mk

ifneq ($(MAKECMDGOALS),clean)
ifneq ($(strip $(C_DEPS)),)
-include $(C_DEPS)
endif
endif

-include ../makefile.defs

# Add inputs and outputs from these tool invocations to the build variables 

# All Target
all: lisandra_file_system

dependents:
	-cd /home/utnso/workspace/tp-2019-1c-Why-are-you-running-/commonsPropias/Debug && $(MAKE) all

# Tool invocations
lisandra_file_system: $(OBJS) $(USER_OBJS) /home/utnso/workspace/tp-2019-1c-Why-are-you-running-/commonsPropias/Debug/libcommonsPropias.so
	@echo 'Building target: $@'
	@echo 'Invoking: GCC C Linker'
	gcc -L"/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/commonsPropias/Debug" -o "lisandra_file_system" $(OBJS) $(USER_OBJS) $(LIBS)
	@echo 'Finished building target: $@'
	@echo ' '

# Other Targets
clean:
	-$(RM) $(EXECUTABLES)$(OBJS)$(C_DEPS) lisandra_file_system
	-@echo ' '

.PHONY: all clean dependents
/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/commonsPropias/Debug/libcommonsPropias.so:

-include ../makefile.targets
