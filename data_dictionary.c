/*
 * data_dictionary.c
 *
 *  Created on: May 19, 2022
 *      Author: tgburrin
 */

#include <string.h>

#include "data_dictionary.h"


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

uint8_t get_dd_field_size(datatype_t type, uint8_t size) {
	switch (type) {
	case TIMESTAMP:
		size = sizeof(struct timespec);
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

dd_datafield_t *init_dd_field_type(char *field_name, datatype_t type, uint8_t size) {
	if( strlen(field_name) >= DB_OBJECT_NAME_SZ )
		return NULL;

	dd_datafield_t *f = malloc(sizeof(dd_datafield_t));
	memset(f, 0, sizeof(dd_datafield_t));
	strcpy(f->fieldname, field_name);
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
	else if (type[0] == 'I')
		if (strcmp(type, "I64") == 0)
			field_type = I64;
		else if (strcmp(type, "I32") == 0)
			field_type = I32;
		else if (strcmp(type, "I8") == 0)
			field_type = I8;
		else
			return NULL;
	else if (type[0] == 'U')
		if (strcmp(type, "UI64") == 0)
			field_type = UI64;
		else if (strcmp(type, "UI32") == 0)
			field_type = UI32;
		else if (strcmp(type, "UI8") == 0)
			field_type = UI8;
		else
			return NULL;
	else
		return NULL;

	return init_dd_field_type(field_name, field_type, size);
}

dd_schema_t *add_dd_schema_field(dd_schema_t *s, dd_datafield_t *f) {
	s->fields = realloc(s->fields, sizeof(dd_datafield_t) * (s->field_count + 1));
	dd_datafield_t *dst = ((dd_datafield_t *)s->fields) + s->field_count;

	memcpy(dst, f, sizeof(dd_datafield_t));

	(s->field_count)++;
	s->record_size += f->fieldsz;
	return s;
}

int i8_compare(void *a, void *b) {
	return *(int8_t *)a == *(int8_t *)b ? 0 : *(int8_t *)a > *(int8_t *)b ? 1 : -1;
}
