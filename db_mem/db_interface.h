/*
 * db_interface.h
 *
 *  Created on: May 17, 2022
 *      Author: tgburrin
 */

#ifndef DB_INTERFACE_H_
#define DB_INTERFACE_H_

#include "table_tools.h"
#include "index_tools.h"
#include "journal_tools.h"
#include "server_tools.h"
#include "data_dictionary.h"

#include "utils.h"

void read_index_from_record_numbers(table_t *tbl, index_t *idx);
void write_record_numbers_from_index(index_t *idx);

bool load_database (data_dictionary_t *);

bool load_all_dd_tables(data_dictionary_t *);
bool close_all_dd_tables(data_dictionary_t *);

uint64_t add_db_record(db_table_t *, char *);
bool delete_db_record(db_table_t *, char *, char *);
char * read_db_record(db_table_t *, uint64_t);

void add_key_from_record(db_table_t *, uint64_t, db_index_t *);
void set_key_from_record(db_table_t *, db_table_t *, uint64_t, db_index_schema_t *, db_indexkey_t*);

bool set_table_field_value(dd_table_schema_t *, char *, char *);
bool set_table_field_value_str(dd_table_schema_t *, char *, char *);
bool set_table_field_value_timestamp(dd_table_schema_t *, char *, struct timespec *);
bool set_table_field_value_bool(dd_table_schema_t *, char *, bool);
bool set_table_field_value_uint(dd_table_schema_t *, char *, uint64_t);
bool set_table_field_value_int(dd_table_schema_t *, char *, int64_t);

/*
void print_db_record(db_table_t *, uint64_t, char **);
char * find_db_record(db_table_t *, char *, char *);
*/
#endif /* DB_INTERFACE_H_ */
