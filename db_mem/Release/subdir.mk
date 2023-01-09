################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../data_dictionary.c \
../db_interface.c \
../index_tools.c \
../journal_tools.c \
../server_tools.c \
../table_tools.c \
../utils.c 

C_DEPS += \
./data_dictionary.d \
./db_interface.d \
./index_tools.d \
./journal_tools.d \
./server_tools.d \
./table_tools.d \
./utils.d 

OBJS += \
./data_dictionary.o \
./db_interface.o \
./index_tools.o \
./journal_tools.o \
./server_tools.o \
./table_tools.o \
./utils.o 


# Each subdirectory must supply rules for building sources it contributes
%.o: ../%.c subdir.mk
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O3 -pedantic -Wall -c -fmessage-length=0 -v -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$@" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


clean: clean--2e-

clean--2e-:
	-$(RM) ./data_dictionary.d ./data_dictionary.o ./db_interface.d ./db_interface.o ./index_tools.d ./index_tools.o ./journal_tools.d ./journal_tools.o ./server_tools.d ./server_tools.o ./table_tools.d ./table_tools.o ./utils.d ./utils.o

.PHONY: clean--2e-

