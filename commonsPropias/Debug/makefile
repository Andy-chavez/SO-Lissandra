################################################################################
# Automatically-generated file. Do not edit!
################################################################################

-include ../makefile.init

RM := rm -rf

# All of the sources participating in the build are defined here
-include sources.mk
-include commonsPropias/subdir.mk
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
all: libcommonsPropias.so

dependents:
	-cd /home/utnso/workspace/tp-2019-1c-Why-are-you-running-/commonsPropias/Release && $(MAKE) clean all

# Tool invocations
libcommonsPropias.so: $(OBJS) $(USER_OBJS) /home/utnso/workspace/tp-2019-1c-Why-are-you-running-/commonsPropias/Release/libcommonsPropias.so
	@echo 'Building target: $@'
	@echo 'Invoking: GCC C Linker'
	gcc -shared -o "libcommonsPropias.so" $(OBJS) $(USER_OBJS) $(LIBS)
	@echo 'Finished building target: $@'
	@echo ' '

# Other Targets
clean:
	-$(RM) $(LIBRARIES)$(OBJS)$(C_DEPS) libcommonsPropias.so
	-@echo ' '

.PHONY: all clean dependents
/home/utnso/workspace/tp-2019-1c-Why-are-you-running-/commonsPropias/Release/libcommonsPropias.so:

-include ../makefile.targets
