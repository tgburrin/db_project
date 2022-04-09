/*
 * table_tools.h
 *
 *  Created on: Mar 28, 2022
 *      Author: tgburrin
 */

#ifndef TABLE_TOOLS_H_
#define TABLE_TOOLS_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>

#include "utils.h"

#define DEFAULT_BASE "/data/tables"
#define DEFAULT_SHM "/dev/shm"

typedef struct TableBase table_t;

typedef void *(*add_record_f)(table_t *, void *);
typedef void *(*find_record_f)(table_t *, void *);
typedef void *(*delete_record_f)(table_t *, void *);

typedef struct TableBase {
	char table_name[64];
	int filedes;
	size_t filesize;
	uint8_t record_size;
	uint64_t total_record_count;
	uint64_t free_record_slot;

	add_record_f add_record;
	delete_record_f delete_record;

	uint64_t *used_slots;
	uint64_t *free_slots;
	void *data;
} table_t;

int open_table(table_t *tablemeta, table_t **mapped_table);

#endif /* TABLE_TOOLS_H_ */
