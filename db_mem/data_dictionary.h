/*
 * data_dictionary.h
 *
 *  Created on: May 19, 2022
 *      Author: tgburrin
 */

#ifndef DATA_DICTIONARY_H_
#define DATA_DICTIONARY_H_

#include <pthread.h>

#include <cjson/cJSON.h>
#include "utils.h"

/* empty typedef definitions for early use, these will be redefined later */
typedef struct DbIndex db_index_t;

typedef enum { STR, TIMESTAMP, BOOL, I8, UI8, I16, UI16, I32, UI32, I64, UI64, UUID } datatype_t;

typedef int (*compare_value_f)(char *, char *);

typedef struct DDDataField {
	char field_name[DB_OBJECT_NAME_SZ];
	datatype_t fieldtype;
	uint8_t fieldsz;  /* in bytes */
} dd_datafield_t;

typedef struct DDSchemaTable {
	char schema_name[DB_OBJECT_NAME_SZ];
	uint8_t field_count;
	uint16_t record_size;
	uint8_t fields_sz;  /* this is the size of the array below */
	dd_datafield_t *fields;
} dd_table_schema_t;

typedef struct DbTable {
	char table_name[DB_OBJECT_NAME_SZ];
	uint16_t header_size;
	struct timespec closedtm;
	uint64_t total_record_count;
	uint64_t free_record_slot;

	int filedes;
	size_t filesize;

	pthread_mutex_t read_lock;
	uint32_t reader_count;
	pthread_mutex_t write_lock;
	uint32_t writer_session;

	dd_table_schema_t *schema;
	db_index_t *indexes;
	uint64_t *used_slots;
	uint64_t *free_slots;
	char *data;
} db_table_t;

typedef struct DbIndex {
	char index_name[DB_OBJECT_NAME_SZ];
	db_table_t *table;
	uint8_t field_count;
	uint8_t fields_sz;  /* this is the size of the array below */
	dd_datafield_t *fields;
} db_index_t;

typedef struct DataDictionary {
	uint32_t num_alloc_fields;
	uint32_t num_alloc_schemas;
	uint32_t num_alloc_tables;

	uint32_t num_fields;
	uint32_t num_schemas;
	uint32_t num_tables;
	dd_datafield_t *fields;
	dd_table_schema_t *schemas;
	db_table_t *tables;
} data_dictionary_t;

data_dictionary_t **init_data_dictionary(uint32_t, uint32_t, uint32_t);
data_dictionary_t **build_dd_from_json(char *);
void release_data_dictionary(data_dictionary_t **);
char *read_dd_json_file(char *);
db_table_t *init_dd_table(char *, dd_table_schema_t *, uint64_t);
dd_table_schema_t *init_dd_schema(char *, uint8_t);
dd_datafield_t *init_dd_field_type(char *, datatype_t, uint8_t);
dd_datafield_t *init_dd_field_str(char *, char *, uint8_t);

const char *map_enum_to_name(datatype_t);

int add_dd_table(data_dictionary_t **, db_table_t *);
int add_dd_schema(data_dictionary_t **, dd_table_schema_t *);
int add_dd_field(data_dictionary_t **, dd_datafield_t *);

uint8_t get_dd_field_size(datatype_t, uint8_t);
int add_dd_schema_field(dd_table_schema_t *, dd_datafield_t *);

int str_compare (char *, char *);
int i8_compare (char *, char *);
int ui8_compare (char *, char *);
int i16_compare (char *, char *);
int ui16_compare (char *, char *);
int i32_compare (char *, char *);
int ui32_compare (char *, char *);
int i64_compare (char *, char *);
int ui64_compare (char *, char *);
int ts_compare (char *, char *);

#endif /* DATA_DICTIONARY_H_ */
