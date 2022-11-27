/*
 * data_dictionary.c
 *
 *  Created on: May 19, 2022
 *      Author: tgburrin
 */

#include <string.h>

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

		db_table_t *tbl = init_dd_table(attr->string, created_schema, table_size);
		cJSON *indexes = cJSON_GetObjectItemCaseSensitive(attr, "indexes");
		uint8_t num_indexes = 0;
		cJSON *index = indexes->child;
		while ( index != NULL ) {
			num_indexes++;
			index = index->next;
		}
		tbl->num_indexes = num_indexes;
		tbl->indexes = calloc(num_indexes, sizeof(db_index_t));
		index = indexes->child;
		for(uint8_t i = 0; i < num_indexes && index != NULL; i++) {
			uint8_t num_fields = cJSON_GetArraySize(cJSON_GetObjectItemCaseSensitive(index, "fields"));
			if ( num_fields == 0 ) {
				fprintf(stderr, "No fields found for index %s\n", index->string);
				free(tbl);
				release_data_dictionary(dd);
				return NULL;
			}
			db_index_schema_t *idxschema = malloc(sizeof(db_index_schema_t));
			bzero(idxschema, sizeof(db_index_schema_t));

			strcpy(tbl->indexes[i].index_name, index->string);
			idxschema->table = tbl;

			idxschema->index_order = 5;
			cJSON *idxattr = cJSON_GetObjectItemCaseSensitive(index, "order");
			if ( idxattr != NULL && cJSON_IsNumber(idxattr) ) {
				double ord = cJSON_GetNumberValue(idxattr);
				if ( ord > 0 && ord < UINT8_MAX )
					idxschema->index_order = (uint8_t)ord;
			}
			idxattr = cJSON_GetObjectItemCaseSensitive(index, "unique");
			if ( idxattr != NULL && cJSON_IsBool(idxattr) && cJSON_IsTrue(idxattr))
				idxschema->is_unique = true;
			else
				idxschema->is_unique = false;

			idxschema->fields_sz = num_fields;
			idxschema->fields = calloc(num_fields, sizeof(dd_datafield_t *));

			idxattr = cJSON_GetObjectItemCaseSensitive(index, "fields");
			dd_datafield_t *idxfield = NULL;
			for(uint8_t idxfieldpos = 0; idxfieldpos < num_fields; idxfieldpos++) {
				char *idxfieldname = cJSON_GetStringValue(cJSON_GetArrayItem(idxattr, idxfieldpos));
				if ( (idxfield = find_dd_field(dd, idxfieldname)) != NULL ) {
					idxschema->fields[idxfieldpos] = idxfield;
				} else {
					fprintf(stderr, "Error locating index field member %s on table %s\n", idxfieldname, tbl->table_name);
					free(tbl);
					release_data_dictionary(dd);
					return NULL;
				}
				idxattr = idxattr->next;
			}

			tbl->indexes[i].idx_schema = idxschema;
			tbl->indexes[i].root_node.parent = &tbl->indexes[i].root_node;
			tbl->indexes[i].root_node.is_leaf = true;
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
				db_index_schema_t *idx = t->indexes[idxcnt].idx_schema;
				if ( idx->fields != NULL)
					free(idx->fields);
				if ( idx != NULL )
					free(idx);
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

db_table_t *init_dd_table(char *table_name, dd_table_schema_t *schema, uint64_t size) {
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
		s->fields = calloc(sizeof(dd_datafield_t), num_fields);
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

const char *map_enum_to_name(datatype_t position) {
	return datatype_names[position];
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

	s->fields[s->field_count] = *f;
	(s->field_count)++;
	s->record_size += f->fieldsz;
	return 1;
}

db_table_t *find_dd_table(data_dictionary_t **dd, const char *tbl_name) {
	for(uint32_t i = 0; i<(*dd)->num_tables; i++)
		if ( strcmp((*dd)->tables[i].table_name, tbl_name) == 0 )
			return &(*dd)->tables[i];
	return NULL;
}

dd_table_schema_t *find_dd_schema(data_dictionary_t **dd, const char *schema_name) {
	for(uint32_t i = 0; i<(*dd)->num_schemas; i++)
		if ( strcmp((*dd)->schemas[i].schema_name, schema_name) == 0 )
			return &(*dd)->schemas[i];
	return NULL;
}

dd_datafield_t *find_dd_field(data_dictionary_t **dd, const char *field_name) {
	for(uint32_t i = 0; i<(*dd)->num_fields; i++)
		if ( strcmp((*dd)->fields[i].field_name, field_name) == 0 )
			return &(*dd)->fields[i];
	return NULL;
}

int i8_compare(char *a, char *b) {
	return *(int8_t *)a == *(int8_t *)b ? 0 : *(int8_t *)a > *(int8_t *)b ? 1 : -1;
}
