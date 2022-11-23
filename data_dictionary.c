/*
 * data_dictionary.c
 *
 *  Created on: May 19, 2022
 *      Author: tgburrin
 */

#include <string.h>

#include "data_dictionary.h"

const char const * datatype_names[] = {"STR", "TIMESTAMP", "BOOL", "I8", "UI8", "I16", "UI16", "I32", "UI32", "I64", "UI64", "UUID"};

data_dictionary_t **init_data_dictionary() {
	data_dictionary_t *dd = (data_dictionary_t *)malloc(sizeof(data_dictionary_t));
	// on a realloc, the addr of *dd can change, so we have to point to a pointer
	data_dictionary_t **rv = malloc(sizeof(data_dictionary_t *));
	*rv = dd;

	dd->num_fields = 0;
	dd->num_schemas = 0;
	dd->num_tables = 0;
	dd->fields = NULL;
	dd->schemas = NULL;
	dd->tables = NULL;
	return rv;
}

dd_table_t *init_dd_table(char *table_name, dd_schema_t *schema, uint64_t size) {
	if( strlen(table_name) >= DB_OBJECT_NAME_SZ )
		return NULL;

	dd_table_t *t = malloc(sizeof(dd_table_t));
	memset(t, 0, sizeof(dd_table_t));
	strcpy(t->table_name, table_name);
	t->header_size = sizeof(dd_table_t);
	t->total_record_count = size;
	t->schema = schema;
	return t;
}

dd_schema_t *init_dd_schema(char *schema_name) {
	if( strlen(schema_name) >= DB_OBJECT_NAME_SZ )
		return NULL;

	dd_schema_t *s = malloc(sizeof(dd_schema_t));
	memset(s, 0, sizeof(dd_schema_t));
	strcpy(s->schema_name, schema_name);
	s->field_count = 0;
	s->record_size = 0;
	s->fields = NULL;
	return s;
}

dd_datafield_t *init_dd_field_type(char *field_name, datatype_t type, uint8_t size) {
	if( strlen(field_name) >= DB_OBJECT_NAME_SZ )
		return NULL;

	dd_datafield_t *f = malloc(sizeof(dd_datafield_t));
	memset(f, 0, sizeof(dd_datafield_t));
	strcpy(f->field_name, field_name);
	f->fieldsz = get_dd_field_size(type, size);
	f->fieldtype = type;

	return f;
}

dd_datafield_t *init_dd_field_str(char *field_name, char *type, uint8_t size) {
	if( strlen(field_name) >= DB_OBJECT_NAME_SZ )
		return NULL;

	datatype_t field_type;
	if (strcmp(type, "STR") == 0)
		field_type = STR;
	else if (strcmp(type, "TIMESTAMP") == 0)
		field_type = TIMESTAMP;
	else if (strcmp(type, "BOOL") == 0)
		field_type = BOOL;
	else if (strcmp(type, "UUID") == 0)
		field_type = UUID;
	else if (type[0] == 'I')
		if (strcmp(type, "I64") == 0)
			field_type = I64;
		else if (strcmp(type, "I32") == 0)
			field_type = I32;
		else if (strcmp(type, "I16") == 0)
			field_type = I16;
		else if (strcmp(type, "I8") == 0)
			field_type = I8;
		else
			return NULL;
	else if (type[0] == 'U')
		if (strcmp(type, "UI64") == 0)
			field_type = UI64;
		else if (strcmp(type, "UI32") == 0)
			field_type = UI32;
		else if (strcmp(type, "UI16") == 0)
			field_type = UI16;
		else if (strcmp(type, "UI8") == 0)
			field_type = UI8;
		else
			return NULL;
	else
		return NULL;

	return init_dd_field_type(field_name, field_type, size);
}

char *map_enum_to_name(datatype_t position) {
	return datatype_names[position];
}

int add_dd_table(data_dictionary_t **dd, dd_table_t *table) {
	for(int i = 0; i < (*dd)->num_tables; i++)
		if(strcmp(((*dd)->tables + i)->table_name, table->table_name) == 0) // prevents a duplicate entry
			return 0;

	(*dd)->tables = realloc((*dd)->tables, sizeof(dd_table_t) * ((*dd)->num_tables + 1));
	memcpy((*dd)->tables + (*dd)->num_tables, table, sizeof(dd_table_t));
	(*dd)->num_tables++;
	return 1;
}

int add_dd_field(data_dictionary_t **dd, dd_datafield_t *field) {
	for(int i = 0; i < (*dd)->num_fields; i++)
		if(strcmp(((*dd)->fields + i)->field_name, field->field_name) == 0) // prevents a duplicate entry
			return 0;
	(*dd)->fields = realloc((*dd)->fields, sizeof(dd_datafield_t) * ((*dd)->num_fields + 1));
	memcpy((*dd)->fields + (*dd)->num_fields, field, sizeof(dd_datafield_t));
	(*dd)->num_fields++;
	return 1;
}

int add_dd_schema(data_dictionary_t **dd, dd_schema_t *schema) {
	for(int i = 0; i < (*dd)->num_schemas; i++)
		if(strcmp(((*dd)->schemas + i)->schema_name, schema->schema_name) == 0) // prevents a duplicate entry
			return 0;
	(*dd)->schemas = realloc((*dd)->schemas, sizeof(dd_schema_t) * ((*dd)->num_schemas + 1));
	memcpy((*dd)->schemas + (*dd)->num_schemas, schema, sizeof(dd_schema_t));
	(*dd)->num_schemas++;
	return 1;
}

uint8_t get_dd_field_size(datatype_t type, uint8_t size) {
	switch (type) {
	case TIMESTAMP:
		size = sizeof(struct timespec);
		break;

	case UUID:
		size = sizeof(uuid_t);
		break;

	case UI64:
		size = sizeof(uint64_t);
		break;
	case I64:
		size = sizeof(int64_t);
		break;

	case UI32:
		size = sizeof(uint32_t);
		break;
	case I32:
		size = sizeof(int32_t);
		break;

	case UI16:
		size = sizeof(uint16_t);
		break;
	case I16:
		size = sizeof(int16_t);
		break;

	case UI8:
		size = sizeof(uint8_t);
		break;
	case I8:
		size = sizeof(int8_t);
		break;
	default:
		break;
	}
	return size;
}

int add_dd_schema_field(dd_schema_t *s, dd_datafield_t *f) {
	s->fields = realloc(s->fields, sizeof(dd_datafield_t *) * (s->field_count + 1));
	s->fields[s->field_count] = f;

	(s->field_count)++;
	s->record_size += f->fieldsz;
	return 1;
}

int i8_compare(void *a, void *b) {
	return *(int8_t *)a == *(int8_t *)b ? 0 : *(int8_t *)a > *(int8_t *)b ? 1 : -1;
}
