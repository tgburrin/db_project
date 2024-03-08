/*
 * db_interface.h
 *
 *  Created on: May 17, 2022
 *      Author: tgburrin
 */

#ifndef DB_INTERFACE_H_
#define DB_INTERFACE_H_

#include "journal_tools.h"
#include "server_tools.h"
#include "data_dictionary.h"

#include "utils.h"

bool load_database (data_dictionary_t *);

bool load_all_dd_tables(data_dictionary_t *);
bool close_all_dd_tables(data_dictionary_t *);

bool load_all_dd_disk_tables(data_dictionary_t *);
bool close_all_dd_disk_tables(data_dictionary_t *);

bool open_all_dd_shm_tables(data_dictionary_t *);
bool close_all_dd_shm_tables(data_dictionary_t *);

bool open_shm_table_name(data_dictionary_t *, char *, db_table_t **);
bool close_shm_table_name(data_dictionary_t *, char *, db_table_t **);

bool open_shm_table(db_table_t *);
bool close_shm_table(db_table_t *);

bool load_dd_index_from_table(db_table_t *);
bool load_dd_index_from_file(db_table_t *);

bool load_dd_indexes(db_table_t *);

/* layers on top of the table functions that also maintain indexes */
bool insert_db_record(db_table_t *, char *, record_num_t *);
bool add_db_record(db_table_t *, char *, record_num_t *);
bool delete_db_record(db_table_t *, record_num_t, char **);
bool read_db_record(db_table_t *, record_num_t, char **);

// TODO fill in the following
record_num_t find_records(db_table_t *, db_index_t *, char *, uint8_t, char **); // table, record data, index, num fields (0 means all), rv list of records

void set_key_from_record_slot(db_table_t *, record_num_t, db_index_schema_t *, db_indexkey_t*);
void set_key_from_record_data(dd_table_schema_t *, db_index_schema_t *, char *, db_indexkey_t *);
db_indexkey_t *create_key_from_record_data(dd_table_schema_t *, db_index_schema_t *, char *);

bool set_table_field_value(dd_table_schema_t *, char *, char *);

bool set_table_field_value_str(dd_table_schema_t *, char *, char *);
bool set_table_field_value_timestamp(dd_table_schema_t *, char *, struct timespec *);
bool set_table_field_value_bool(dd_table_schema_t *, char *, bool);
bool set_table_field_value_uint(dd_table_schema_t *, char *, uint64_t);
bool set_table_field_value_int(dd_table_schema_t *, char *, int64_t);

bool init_index_block(db_table_t *tbl, db_index_t *idx);
bool read_index_file_records(db_table_t *tbl, db_index_t *idx);
bool read_index_table_records(db_table_t *tbl, db_index_t *idx);

/*
void print_db_record(db_table_t *, uint64_t, char **);
char * find_db_record(db_table_t *, char *, char *);
*/
#endif /* DB_INTERFACE_H_ */
