################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../app.c \
../data_dictionary.c \
../db_interface.c \
../fruit_index_test.c \
../index_tools.c \
../journal_test.c \
../journal_tools.c \
../server_tools.c \
../table_tools.c \
../utils.c 

OBJS += \
./app.o \
./data_dictionary.o \
./db_interface.o \
./fruit_index_test.o \
./index_tools.o \
./journal_test.o \
./journal_tools.o \
./server_tools.o \
./table_tools.o \
./utils.o 

C_DEPS += \
./app.d \
./data_dictionary.d \
./db_interface.d \
./fruit_index_test.d \
./index_tools.d \
./journal_test.d \
./journal_tools.d \
./server_tools.d \
./table_tools.d \
./utils.d 


# Each subdirectory must supply rules for building sources it contributes
%.o: ../%.c
	@echo 'Building file: $<'
	@echo 'Invoking: Cross GCC Compiler'
	gcc -I/usr/local/include -O3 -g3 -Wall -c -fmessage-length=0 -fms-extensions -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


