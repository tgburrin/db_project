/*
 * data_dictionary.h
 *
 *  Created on: May 19, 2022
 *      Author: tgburrin
 */

#ifndef DATA_DICTIONARY_H_
#define DATA_DICTIONARY_H_

#include "utils.h"

typedef enum { STR, TIMESTAMP, BOOL, I8, UI8, I16, UI16, I32, UI32, I64, UI64 } datatype_t;

typedef int (*compare_value_f)(void *, void *);

typedef struct DDDataField {
	char fieldname[DB_OBJECT_NAME_SZ+1];
	datatype_t fieldtype;
	uint8_t fieldsz; // in bytes
	compare_value_f fieldcompare;
} dd_datafield_t;

typedef struct DDSchema {
	char schema_name[DB_OBJECT_NAME_SZ];
	uint8_t field_count;
	uint16_t record_size;
	dd_datafield_t *fields;
} dd_schema_t;

typedef struct DDTable {
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

	dd_schema_t schema;

	uint64_t *used_slots;
	uint64_t *free_slots;
	void *data;
} dd_table_t;

int str_compare (void *, void *);
int i8_compare (void *, void *);
int ui8_compare (void *, void *);
int i16_compare (void *, void *);
int ui16_compare (void *, void *);
int i32_compare (void *, void *);
int ui32_compare (void *, void *);
int i64_compare (void *, void *);
int ui64_compare (void *, void *);
int ts_compare (void *, void *);

dd_schema_t *init_dd_schema(char *);
dd_datafield_t *init_dd_field_type(char *, datatype_t, uint8_t);
dd_datafield_t *init_dd_field_str(char *, char *, uint8_t);
uint8_t get_dd_field_size(datatype_t, uint8_t);
dd_schema_t *add_dd_schema_field(dd_schema_t *, dd_datafield_t *);

#endif /* DATA_DICTIONARY_H_ */
