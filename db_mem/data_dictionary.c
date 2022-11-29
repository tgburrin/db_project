/*
 * data_dictionary.c
 *
 *  Created on: May 19, 2022
 *      Author: tgburrin
 */

#include <string.h>

#include "table_tools.h"
#include "index_tools.h"
#include "data_dictionary.h"

char const * datatype_names[] = {"STR", "TIMESTAMP", "BOOL", "I8", "UI8", "I16", "UI16", "I32", "UI32", "I64", "UI64", "UUID"};

data_dictionary_t **init_data_dictionary(uint32_t num_fields, uint32_t num_schemas, uint32_t num_tables) {
	data_dictionary_t *dd = (data_dictionary_t *)malloc(sizeof(data_dictionary_t));
	/* on a realloc, the addr of *dd can change, so we have to point to a pointer */
	data_dictionary_t **rv = malloc(sizeof(data_dictionary_t *));
	*rv = dd;

	dd->num_alloc_fields = num_fields;
	dd->num_alloc_schemas = num_schemas;
	dd->num_alloc_tables = num_tables;
	dd->num_fields = 0;
	dd->num_schemas = 0;
	dd->num_tables = 0;
	dd->fields = calloc(num_fields, sizeof(dd_datafield_t));
	dd->schemas = calloc(num_schemas, sizeof(dd_table_schema_t));
	dd->tables = calloc(num_tables, sizeof(db_table_t));
	return rv;
}

char *read_dd_json_file(char *filename) {
	char *rv = NULL;
	FILE *fdf = NULL;

	if ( filename == NULL )
		return rv;

	if ( strcmp(filename, "-") == 0 ) {
		fdf = stdin;
		printf("Opened stdin\n");
	} else 	if ( (fdf = fopen(filename, "r")) == NULL ) {
		fprintf(stderr, "Unable open file pointer for %s: %s\n", filename, strerror(errno));
		return rv;
	} else
		printf("Opened file '%s'\n", filename);

	size_t i = 0, buffsz = 4096;
	uint32_t rb = 0, memsz = buffsz;
	char *buff = malloc(buffsz);
	bzero(buff, buffsz);

	for (;;) {
		//printf("Reading up to %ld bytes from file\n", buffsz);
		if ( (i = fread(buff +  rb, buffsz, 1, fdf)) == 0 && !feof(fdf)) {
			fprintf(stderr, "Unable to read from file\n");
			free(buff);
			fclose(fdf);
			return rv;

		} else if ( feof(fdf) ) {
			// this was the last read
			rv = buff;
			break;

		} else {
			rb += i * buffsz;
			//printf("Read %ld bytes...allocating more space\n", i * buffsz);
			memsz += buffsz;
			if ( (buff = realloc(buff, memsz)) == NULL ) {
				fprintf(stderr, "Unable to allocate more memory while reading file\n");
				free(buff);
				fclose(fdf);
				return rv;
			}
			bzero(buff+rb, buffsz);
		}
	}

	fclose(fdf);
	//printf("File of %ld bytes loaded\n", strlen(rv));

	return rv;
}

data_dictionary_t **build_dd_from_json(char *filename) {
	char *filedata = NULL;
	if ( (filedata = read_dd_json_file(filename)) == NULL ) {
		fprintf(stderr, "Error reading from %s\n", filename);
		return NULL;
	}

	cJSON *doc = NULL;
	if ( (doc = cJSON_Parse(filedata)) == NULL ) {
		const char *err = NULL;
		if ( (err = cJSON_GetErrorPtr()) != NULL )
			fprintf(stderr, "Error parsing json: %s\n", err);
		return NULL;
	} else
		free(filedata);

	cJSON *fields = cJSON_GetObjectItemCaseSensitive(doc, "fields");
	cJSON *tables = cJSON_GetObjectItemCaseSensitive(doc, "tables");

	uint32_t num_fields = 0, num_tables = 0;

	cJSON *attr = NULL;
	for( attr = fields->child; attr != NULL; attr = attr->next )
		num_fields++;
	for( attr = tables->child; attr != NULL; attr = attr->next )
		num_tables++;

	data_dictionary_t **dd = init_data_dictionary(num_fields, num_tables, num_tables);

	attr = fields->child;
	while(attr != NULL) {
		char *type = NULL;
		uint8_t size = 0;
		cJSON *c = NULL;
		if ( (c = cJSON_GetObjectItemCaseSensitive(attr, "type")) == NULL || !cJSON_IsString(c)) {
			fprintf(stderr, "type not provided for field: %s\n", attr->string);
			attr = attr->next;
			continue;
		} else {
			type = cJSON_GetStringValue(c);
		}

		if ( (c = cJSON_GetObjectItemCaseSensitive(attr, "size")) != NULL && cJSON_IsNumber(c) ) {
			double sv = cJSON_GetNumberValue(c);
			if ( sv >= UINT8_MAX || sv <= 0) {
				fprintf(stderr, "size %lf must be between 1 and %d\n", sv, UINT8_MAX - 1);
				attr = attr->next;
				continue;
			} else {
				size = (uint8_t)sv;
			}
		}

		dd_datafield_t *field = NULL;
		if ( (field = init_dd_field_str(attr->string, type, size)) == NULL ) {
			fprintf(stderr, "could not create dd field %s (%s)\n", attr->string, type);
			attr = attr->next;
			continue;
		}
		if ( !add_dd_field(dd, field)) {
			fprintf(stderr, "could add dd field %s to dictionary\n", field->field_name);
			attr = attr->next;
			continue;
		} else {
			//printf("Added field %s to dictionary\n", field->field_name);
			free(field);
		}

		attr = attr->next;
	}

	attr = tables->child;
	while(attr != NULL) {
		//printf("Creating schema and table for %s\n", attr->string);
		uint8_t num_fields = cJSON_GetArraySize(cJSON_GetObjectItemCaseSensitive(attr, "fields"));
		dd_table_schema_t *schema = init_dd_schema(attr->string, num_fields);
		dd_table_schema_t *created_schema = NULL;
		uint64_t table_size = 0;
		cJSON *c = NULL;

		if ( (c = cJSON_GetObjectItemCaseSensitive(attr, "size")) != NULL && cJSON_IsNumber(c) ) {
			double sv = cJSON_GetNumberValue(c);
			if ( sv >= UINT64_MAX || sv < 1) {
				fprintf(stderr, "size %lf must be between 1 and %ld\n", sv, UINT64_MAX - 1);
				attr = attr->next;
				continue;
			} else
				table_size = (uint64_t)sv;
		}

		c = cJSON_GetObjectItemCaseSensitive(attr, "fields");
		if ( c == NULL || cJSON_GetArraySize(c) == 0 ) {
			fprintf(stderr, "could not find field list for table %s\n", attr->string);
			attr = attr->next;
			continue;
		} else {
			for( int i = 0; i < cJSON_GetArraySize(c); i++ ) {
				cJSON *el = cJSON_GetArrayItem(c, i);
				char *table_field = NULL;
				if ( !cJSON_IsString(el) ) {
					fprintf(stderr, "table %s has invalid field name in index %d\n", attr->string, i);
					continue;
				} else
					table_field = cJSON_GetStringValue(el);

				dd_datafield_t *field = NULL;
				for(uint32_t i = 0; i<(*dd)->num_fields; i++) {
					dd_datafield_t *cf = &(*dd)->fields[i];
					if ( strcmp(cf->field_name, table_field) == 0 ) {
						field = cf;
						break;
					}
				}
				if ( field == NULL ) {
					fprintf(stderr, "field %s is defined on table %s, but does not have its own definition\n", table_field, attr->string);
					continue;
				}
				add_dd_table_schema_field(schema, field);
			}
		}
		/*
		 * right now schemas and tables are the same, but there are use cases for using the same schema for multiple tables
		 * e.g. a top level and history tables which would be the same except for an additional member of a unique index
		 * top level:
		 *   order id
		 *   created datetime
		 *   ...
		 *   unique index = order id
		 * history table:
		 * 	 order id
		 * 	 created datetime
		 * 	 ...
		 * 	 unique index = order id, created datetime
		 */
		add_dd_schema(dd, schema, &created_schema);
		free(schema);
		schema = NULL;

		db_table_t *tbl = init_db_table(attr->string, created_schema, table_size);
		cJSON *indexes = cJSON_GetObjectItemCaseSensitive(attr, "indexes");
		uint8_t num_indexes = 0;
		cJSON *index = indexes->child;
		while ( index != NULL ) {
			num_indexes++;
			index = index->next;
		}
		tbl->num_indexes = num_indexes;
		tbl->indexes = calloc(num_indexes, sizeof(db_index_t *));
		index = indexes->child;
		for(uint8_t i = 0; i < num_indexes && index != NULL; i++) {
			uint8_t num_fields = cJSON_GetArraySize(cJSON_GetObjectItemCaseSensitive(index, "fields"));
			if ( num_fields == 0 ) {
				fprintf(stderr, "No fields found for index %s\n", index->string);
				free(tbl);
				release_data_dictionary(dd);
				return NULL;
			}
			tbl->indexes[i] = init_db_idx(index->string, num_fields);
			tbl->indexes[i]->table = tbl;

			cJSON *idxattr = cJSON_GetObjectItemCaseSensitive(index, "order");
			if ( idxattr != NULL && cJSON_IsNumber(idxattr) ) {
				double ord = cJSON_GetNumberValue(idxattr);
				if ( ord > 0 && ord < INDEX_ORDER_MAX )
					tbl->indexes[i]->idx_schema->index_order = (uint8_t)ord;
			}
			idxattr = cJSON_GetObjectItemCaseSensitive(index, "unique");
			if ( idxattr != NULL && cJSON_IsBool(idxattr) && cJSON_IsTrue(idxattr))
				tbl->indexes[i]->idx_schema->is_unique = true;

			idxattr = cJSON_GetObjectItemCaseSensitive(index, "fields");
			dd_datafield_t *idxfield = NULL;
			for(uint8_t idxfieldpos = 0; idxfieldpos < num_fields; idxfieldpos++) {
				char *idxfieldname = cJSON_GetStringValue(cJSON_GetArrayItem(idxattr, idxfieldpos));
				printf("Locating field for index member %s position %d\n", idxfieldname, idxfieldpos);
				if ( (idxfield = find_dd_field(dd, idxfieldname)) != NULL ) {
					tbl->indexes[i]->idx_schema->fields[idxfieldpos] = idxfield;
					tbl->indexes[i]->idx_schema->record_size += idxfield->fieldsz;
				} else {
					fprintf(stderr, "Error locating index field member %s on table %s\n", idxfieldname, tbl->table_name);
					free(tbl);
					release_data_dictionary(dd);
					return NULL;
				}
			}

			tbl->indexes[i]->root_node = dbidx_init_root_node(tbl->indexes[i]->idx_schema);

			index = index->next;
		}
		add_dd_table(dd, tbl);
		free(tbl);

		attr = attr->next;
	}
	cJSON_Delete(doc);
	return dd;
}

void release_data_dictionary(data_dictionary_t **dd) {
	for(uint32_t tblcnt = 0; tblcnt < (*dd)->num_tables; tblcnt++) {
		db_table_t *t = &(*dd)->tables[tblcnt];
		dd_table_schema_t *s = t->schema;
		if ( t->indexes != NULL ) {
			for(uint8_t idxcnt = 0; idxcnt < t->num_indexes; idxcnt++) {
				dbidx_release_tree(t->indexes[idxcnt], NULL);
				free(t->indexes[idxcnt]->idx_schema->fields);
				free(t->indexes[idxcnt]);
			}
			free(t->indexes);
		}
		if ( s->fields != NULL )
			free(s->fields);
	}
	free((*dd)->tables);
	(*dd)->num_tables = 0;
	(*dd)->num_alloc_tables = 0;

	free((*dd)->schemas);
	(*dd)->num_schemas = 0;
	(*dd)->num_alloc_schemas = 0;

	free((*dd)->fields);
	(*dd)->num_fields= 0;
	(*dd)->num_alloc_fields = 0;
	free(*dd);
	free(dd);
}

db_table_t *init_db_table(char *table_name, dd_table_schema_t *schema, uint64_t size) {
	if( strlen(table_name) >= DB_OBJECT_NAME_SZ )
		return NULL;

	db_table_t *t = malloc(sizeof(db_table_t));
	memset(t, 0, sizeof(db_table_t));
	strcpy(t->table_name, table_name);
	t->header_size = sizeof(db_table_t);
	t->total_record_count = size;
	t->free_record_slot = size - 1;
	t->schema = schema;
	return t;
}

dd_table_schema_t *init_dd_schema(char *schema_name, uint8_t num_fields) {
	if( strlen(schema_name) >= DB_OBJECT_NAME_SZ )
		return NULL;

	dd_table_schema_t *s = malloc(sizeof(dd_table_schema_t));
	memset(s, 0, sizeof(dd_table_schema_t));
	strcpy(s->schema_name, schema_name);
	s->field_count = 0;
	s->record_size = 0;
	s->fields_sz = num_fields;
	s->fields = NULL;
	if ( num_fields > 0 )
		s->fields = calloc(sizeof(dd_datafield_t *), num_fields);
	return s;
}

db_index_t *init_db_idx(char *index_name, uint8_t num_fields) {
	db_index_t *idx = NULL;
	db_index_schema_t *idxschema = NULL;

	size_t memsz = sizeof(db_index_t) + sizeof(db_index_schema_t) + sizeof(dd_datafield_t *) * num_fields;

	idx = (db_index_t *)malloc(memsz);
	bzero(idx, memsz);

	idxschema = (db_index_schema_t *)((char *)idx + sizeof(db_index_t));
	idxschema->fields = (dd_datafield_t **)((char *)idxschema + sizeof(db_index_schema_t));

	strcpy(idx->index_name, index_name);
	idxschema->index_order = 5;
	idxschema->is_unique = false;
	idxschema->fields_sz = num_fields;
	idxschema->fields = calloc(num_fields, sizeof(dd_datafield_t *));
	idx->idx_schema = idxschema;
	idx->root_node = NULL;
	return idx;
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

const char *map_enum_to_name(datatype_t position) {
	return datatype_names[position];
}

void idx_key_to_str(db_index_schema_t *idx, db_indexkey_t *key, char *buff) {
	bzero(buff, sizeof(*buff));

	char *p = buff;
	size_t offset = 0;
	char timestampstr[31];

	for( uint8_t i = 0; i < idx->fields_sz; i++ ) {
		switch (idx->fields[i]->fieldtype) {
		case STR:
			if ( i > 0) {
				sprintf(p, ", ");
				p = buff + strlen(buff);
			}
			snprintf(p, idx->fields[i]->fieldsz, "%s", (*key->data + offset));
			offset += idx->fields[i]->fieldsz;
			break;
		case TIMESTAMP:
			bzero(&timestampstr, sizeof(timestampstr));
			format_timestamp((struct timespec *)(*key->data + offset), timestampstr);
			sprintf(p, "%s%s", i > 0 ? ", " : "", timestampstr);
			offset += idx->fields[i]->fieldsz;
			break;
		case UUID:
			break;
		case UI64:
			break;
		case I64:
			break;
		case UI32:
			break;
		case I32:
			break;
		case UI16:
			break;
		case I16:
			break;
		case UI8:
			break;
		case I8:
			break;
		default:
			break;
		}
		p = buff + strlen(buff);
	}
}

int add_dd_table(data_dictionary_t **dd, db_table_t *table) {
	for(uint32_t i = 0; i < (*dd)->num_tables; i++)
		if(strcmp((*dd)->tables[i].table_name, table->table_name) == 0) // prevents a duplicate entry
			return 0;

	if ( (*dd)->num_tables >= (*dd)->num_alloc_tables )
		return 0;

	memcpy(((*dd)->tables + (*dd)->num_tables), table, sizeof(db_table_t));
	(*dd)->num_tables++;
	return 1;
}

int add_dd_field(data_dictionary_t **dd, dd_datafield_t *field) {
	for(uint32_t i = 0; i < (*dd)->num_fields; i++)
		if(strcmp((*dd)->fields[i].field_name, field->field_name) == 0) // prevents a duplicate entry
			return 0;

	if ( (*dd)->num_fields >= (*dd)->num_alloc_fields )
		return 0;

	memcpy((*dd)->fields + (*dd)->num_fields, field, sizeof(dd_datafield_t));
	(*dd)->num_fields++;

	return 1;
}

int add_dd_schema(data_dictionary_t **dd, dd_table_schema_t *schema, dd_table_schema_t **created_schema) {
	for(uint32_t i = 0; i < (*dd)->num_schemas; i++)
		if(strcmp((*dd)->schemas[i].schema_name, schema->schema_name) == 0) // prevents a duplicate entry
			return 0;

	if ( (*dd)->num_schemas >= (*dd)->num_alloc_schemas )
		return 0;

	memcpy((*dd)->schemas + (*dd)->num_schemas, schema, sizeof(dd_table_schema_t));

	if ( created_schema != NULL )
		*created_schema = ((*dd)->schemas + (*dd)->num_schemas);
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

int add_dd_table_schema_field(dd_table_schema_t *s, dd_datafield_t *f) {
	if ( s->field_count + 1 > s->fields_sz ) {
		s->fields_sz++;
		s->fields = realloc(s->fields, sizeof(dd_datafield_t *) * s->fields_sz);
	}

	s->fields[s->field_count] =f;
	(s->field_count)++;
	s->record_size += f->fieldsz;
	return 1;
}

db_table_t *find_db_table(data_dictionary_t **dd, const char *tbl_name) {
	for(uint32_t i = 0; i<(*dd)->num_tables; i++)
		if ( strcmp((*dd)->tables[i].table_name, tbl_name) == 0 )
			return &(*dd)->tables[i];
	return NULL;
}

db_index_t *find_db_index(db_table_t *tbl, const char *idx_name) {
	for(uint32_t i = 0; i<tbl->num_indexes; i++)
		if ( strcmp(tbl->indexes[i]->index_name, idx_name) == 0 )
			return tbl->indexes[i];
	return NULL;
}

dd_table_schema_t *find_dd_schema(data_dictionary_t **dd, const char *schema_name) {
	for(uint32_t i = 0; i<(*dd)->num_schemas; i++)
		if ( strcmp((*dd)->schemas[i].schema_name, schema_name) == 0 )
			return &(*dd)->schemas[i];
	return NULL;
}

db_index_schema_t *find_dd_idx_schema(db_table_t *tbl, const char *idx_name) {
	for(uint32_t i = 0; i < tbl->num_indexes; i++)
		if ( strcmp(tbl->indexes[i]->index_name, idx_name) == 0 )
			return tbl->indexes[i]->idx_schema;
	return NULL;
}

dd_datafield_t *find_dd_field(data_dictionary_t **dd, const char *field_name) {
	for(uint32_t i = 0; i<(*dd)->num_fields; i++)
		if ( strcmp((*dd)->fields[i].field_name, field_name) == 0 )
			return &(*dd)->fields[i];
	return NULL;
}

signed char str_compare_sz (const char *s1, const char *s2, size_t n) {
	int i = strncmp(s1, s2, n);
	return i == 0 ? i : i > 0 ? 1 : -1; // the int value could overflow a signec char
}

signed char str_compare (const char *s1, const char *s2) {
	int i = strcmp(s1, s2);
	return i == 0 ? i : i > 0 ? 1 : -1; // the int value could overflow a signec char
}

signed char i64_compare(int64_t *a, int64_t *b) {
	return *a == *b ? 0 : *a > *b ? 1 : -1;
}
signed char ui64_compare(uint64_t *a, uint64_t *b) {
	return *a == *b ? 0 : *a > *b ? 1 : -1;
}
signed char i32_compare(int32_t *a, int32_t *b) {
	return *a == *b ? 0 : *a > *b ? 1 : -1;
}
signed char ui32_compare(uint32_t *a, uint32_t *b) {
	return *a == *b ? 0 : *a > *b ? 1 : -1;
}
signed char i16_compare(int16_t *a, int16_t *b) {
	return *a == *b ? 0 : *a > *b ? 1 : -1;
}
signed char ui16_compare(uint16_t *a, uint16_t *b) {
	return *a == *b ? 0 : *a > *b ? 1 : -1;
}
signed char i8_compare(int8_t *a, int8_t *b) {
	return *a == *b ? 0 : *a > *b ? 1 : -1;
}
signed char ui8_compare(uint8_t *a, uint8_t *b) {
	return *a == *b ? 0 : *a > *b ? 1 : -1;
}

signed char ts_compare (struct timespec *ts1, struct timespec *ts2) {
	if ( ts1->tv_sec > ts2->tv_sec )
		return 1;
	else if ( ts1->tv_sec < ts2->tv_sec )
		return -1;
	else if ( ts1->tv_nsec > ts2->tv_nsec )
		return 1;
	else if ( ts1->tv_nsec < ts2->tv_nsec )
		return -1;
	return 0;
}
