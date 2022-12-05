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

bool open_dd_table(db_table_t *tablemeta);
bool close_dd_table(db_table_t *tablemeta);

uint64_t add_db_table_record(db_table_t *, char *);
bool delete_db_table_record(db_table_t *, uint64_t, char *);
char * read_db_table_record(db_table_t *, uint64_t);
char * new_db_table_record(dd_table_schema_t *);
void reset_db_table_record(dd_table_schema_t *, char *);
void release_table_record(dd_table_schema_t *, char *);
bool set_db_table_record_field(dd_table_schema_t *, char *, char *, char *);
bool set_db_table_record_field_num(dd_table_schema_t *, uint8_t, char *, char *);

void db_table_record_print(dd_table_schema_t *, char *);
void db_table_record_str(dd_table_schema_t *, char *, char *, size_t);

#endif /* TABLE_TOOLS_H_ */
