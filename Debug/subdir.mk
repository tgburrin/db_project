################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../app.c \
../index_tools.c \
../table_tools.c \
../utils.c 

OBJS += \
./app.o \
./index_tools.o \
./table_tools.o \
./utils.o 

C_DEPS += \
./app.d \
./index_tools.d \
./table_tools.d \
./utils.d 


# Each subdirectory must supply rules for building sources it contributes
%.o: ../%.c
	@echo 'Building file: $<'
	@echo 'Invoking: Cross GCC Compiler'
	gcc -O3 -g3 -Wall -c -fmessage-length=0 -fms-extensions -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


