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
#include "data_dictionary.h"

typedef struct TableBase table_t;

typedef uint64_t (*add_record_f)(table_t *, char *);
typedef char *(*read_record_f)(table_t *, uint64_t);
typedef bool (*delete_record_f)(table_t *, uint64_t, char *);

typedef struct TableBase {
	char table_name[DB_OBJECT_NAME_SZ];
	uint16_t header_size;
	uint16_t record_size;
	struct timespec closedtm;
	uint64_t total_record_count;
	uint64_t free_record_slot;

	add_record_f add_record;
	delete_record_f delete_record;
	read_record_f read_record;

	int filedes;
	size_t filesize;

	uint64_t *used_slots;
	uint64_t *free_slots;
	char *data;
} table_t;

int open_table(table_t *tablemeta, table_t **mapped_table);
int close_table(table_t *mapped_table);

int open_dd_table(db_table_t *tablemeta, db_table_t **mapped_table);
int close_dd_table(db_table_t *mapped_table);

uint64_t add_db_table_record(db_table_t *, char *);
bool delete_db_table_record(db_table_t *, uint64_t, char *);
char * read_db_table_record(db_table_t *, uint64_t);

#endif /* TABLE_TOOLS_H_ */
