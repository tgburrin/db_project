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

typedef struct TableBase table_t;

typedef uint64_t (*add_record_f)(table_t *, void *);
typedef void *(*read_record_f)(table_t *, uint64_t);
typedef bool (*delete_record_f)(table_t *, uint64_t, void *);

typedef struct TableBase {
	char table_name[64];
	uint16_t header_size;
	uint16_t record_size;
	uint64_t total_record_count;
	uint64_t free_record_slot;

	add_record_f add_record;
	delete_record_f delete_record;
	read_record_f read_record;

	int filedes;
	size_t filesize;

	uint64_t *used_slots;
	uint64_t *free_slots;
	void *data;
} table_t;

int open_table(table_t *tablemeta, table_t **mapped_table);
int close_table(table_t *mapped_table);

#endif /* TABLE_TOOLS_H_ */
