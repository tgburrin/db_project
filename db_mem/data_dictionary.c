/*
 * data_dictionary.c
 *
 *  Created on: May 19, 2022
 *      Author: tgburrin
 */

#include <string.h>

#include "data_dictionary.h"

const char * datatype_names[] = {
	"STR",
	"TIMESTAMP",
	"BOOL",

	"I8",
	"UI8",
	"I16",
	"UI16",
	"I32",
	"UI32",
	"I64",
	"UI64",

	"UUID",
	"BYTES"
};

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
		if ( (i = fread(buff +  rb, buffsz, 1, fdf)) == 0 && !feof(fdf)) {
			fprintf(stderr, "Unable to read from file\n");
			free(buff);
			fclose(fdf);
			return rv;

		} else if ( feof(fdf) ) {
			rv = buff;
			break;

		} else {
			rb += i * buffsz;
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

	return rv;
}

data_dictionary_t **build_dd_from_json(char *filename) {
	char *filedata = NULL;
	if ( (filedata = read_dd_json_file(filename)) == NULL )
		return NULL; //fprintf(stderr, "Error reading from %s\n", filename);

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
			fprintf(stderr, "could not add dd field %s to dictionary\n", field->field_name);
			attr = attr->next;
			continue;
		} else {
			free(field);
		}

		attr = attr->next;
	}

	attr = tables->child;
	while(attr != NULL) {
		uint8_t num_fields = cJSON_GetArraySize(cJSON_GetObjectItemCaseSensitive(attr, "fields"));
		dd_table_schema_t *schema = init_dd_schema(attr->string, num_fields);
		dd_table_schema_t *created_schema = NULL;
		record_num_t table_size = 0;
		cJSON *c = NULL;

		if ( (c = cJSON_GetObjectItemCaseSensitive(attr, "size")) != NULL && cJSON_IsNumber(c) ) {
			double sv = cJSON_GetNumberValue(c);
			if ( sv >= RECORD_NUM_MAX || sv < 1) {
				fprintf(stderr, "size %lf must be between 1 and %" PRIu64 "\n", sv, (uint64_t)(RECORD_NUM_MAX - 1));
				attr = attr->next;
				continue;
			} else
				table_size = (record_num_t)sv;
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
					set_dd_index_order(tbl->indexes[i]->idx_schema, (index_order_t)ord);
			}
			idxattr = cJSON_GetObjectItemCaseSensitive(index, "unique");
			if ( idxattr != NULL && cJSON_IsBool(idxattr) && cJSON_IsTrue(idxattr))
				tbl->indexes[i]->idx_schema->is_unique = true;

			idxattr = cJSON_GetObjectItemCaseSensitive(index, "fields");
			dd_datafield_t *idxfield = NULL;
			for(uint8_t idxfieldpos = 0; idxfieldpos < num_fields; idxfieldpos++) {
				char *idxfieldname = cJSON_GetStringValue(cJSON_GetArrayItem(idxattr, idxfieldpos));
				if ( (idxfield = find_dd_field(dd, idxfieldname)) != NULL ) {
					tbl->indexes[i]->idx_schema->fields[idxfieldpos] = idxfield;
					tbl->indexes[i]->idx_schema->record_size += idxfield->field_sz;
				} else {
					fprintf(stderr, "Error locating index field member %s on table %s\n", idxfieldname, tbl->table_name);
					free(tbl);
					release_data_dictionary(dd);
					return NULL;
				}
			}

			tbl->indexes[i]->root_node = dbidx_init_root_node(tbl->indexes[i]);

			index = index->next;
		}
		add_dd_table(dd, tbl, NULL);
		free(tbl);

		attr = attr->next;
	}
	cJSON_Delete(doc);
	return dd;
}

data_dictionary_t **build_dd_from_dat(char *filename) {
	char *data = NULL;
	FILE *fdf = NULL;

	if ( filename == NULL )
		return NULL;

	if ( strcmp(filename, "-") == 0 ) {
		fdf = stdin;
		printf("Opened stdin\n");
	} else 	if ( (fdf = fopen(filename, "r")) == NULL ) {
		fprintf(stderr, "Unable open file pointer for %s: %s\n", filename, strerror(errno));
		return NULL;
	} else
		printf("Opened file '%s'\n", filename);

	fseek(fdf, 0, SEEK_END);
	size_t readsz = 0;
	size_t fsize = ftell(fdf);
	fseek(fdf, 0, SEEK_SET);
	data = malloc(fsize);
	bzero(data, fsize);
	readsz = fread(data, fsize, 1, fdf);
	fclose(fdf);
	if (readsz != 1) {
		fprintf(stderr, "Returned %ld, unable to read %ld bytes into memory from %s\n", readsz, fsize, filename);
		return NULL;
	} else
		printf("Read %ld bytes into memory\n", readsz * fsize);

	readsz = 0;
	uint32_t major = *(uint32_t *)(data + readsz);
	readsz += sizeof(uint32_t);
	uint16_t minor = *(uint16_t *)(data + readsz);
	readsz += sizeof(uint16_t);
	uint8_t patch = *(uint8_t *)(data + readsz);
	readsz += sizeof(uint8_t);

	size_t fieldoff = 0, schemaoff = 0, tableoff = 0;
	uint32_t num_fields = 0, num_schemas = 0, num_tables = 0;

	num_fields = *(uint32_t *)(data + readsz);
	readsz += sizeof(uint32_t);
	fieldoff = *(size_t *)(data + readsz);
	readsz += sizeof(size_t);

	num_schemas = *(uint32_t *)(data + readsz);
	readsz += sizeof(uint32_t);
	schemaoff = *(size_t *)(data + readsz);
	readsz += sizeof(size_t);

	num_tables = *(uint32_t *)(data + readsz);
	readsz += sizeof(uint32_t);
	tableoff = *(size_t *)(data + readsz);
	readsz += sizeof(size_t);

	printf("Version %" PRIu32 ".%" PRIu16 ".%" PRIu8"\n", major, minor, patch);
	printf("Reading %" PRIu32 " fields, %" PRIu32 " schemas, and %" PRIu32 " tables\n", num_fields, num_schemas, num_tables);
	printf("Positions %ld, %ld, %ld\n", fieldoff, schemaoff, tableoff);
	printf("Final offset before fields is %ld\n", readsz);
	data_dictionary_t **dd = init_data_dictionary(num_fields, num_schemas, num_tables);

	for(uint32_t i = 0; i < num_fields; i++) {
		add_dd_field(dd, (dd_datafield_t *)(data + readsz));
		readsz += sizeof(dd_datafield_t);
	}

	dd_table_schema_t *s = NULL;
	uint32_t fieldnum = 0;
	for(uint32_t sn = 0; sn < num_schemas; sn++) {
		s = NULL;
		add_dd_schema(dd, (dd_table_schema_t *)(data + readsz), &s);
		readsz += sizeof(dd_table_schema_t);
		s->fields = calloc(sizeof(dd_datafield_t *), num_fields);
		printf("Loaded schema %s with %d fields\n", s->schema_name, s->num_fields);
		for(uint8_t sf = 0; sf < s->num_fields; sf++) {
			fieldnum = *(uint32_t *)(data + readsz);
			readsz += sizeof(uint32_t);
			printf("Adding field %s to schema\n", (*dd)->fields[fieldnum].field_name);
			s->fields[sf] = &(*dd)->fields[fieldnum];
		}
	}

	db_table_t *t = NULL;
	db_index_t *idx = NULL;
	db_index_schema_t *idxs = NULL;
	for(uint32_t tn = 0; tn < num_schemas; tn++) {
		add_dd_table(dd, (db_table_t *)(data + readsz), &t);
		readsz += sizeof(db_table_t);
		fieldnum = *(uint32_t *)(data + readsz);
		readsz += sizeof(uint32_t);

		t->schema = &(*dd)->schemas[fieldnum];
		t->indexes = calloc(t->num_indexes, sizeof(db_index_t *));
		for(uint8_t idxc = 0; idxc < t->num_indexes; idxc++) {
			idx = (db_index_t *)(data + readsz);
			readsz += sizeof(db_index_t);
			idxs = (db_index_schema_t *)(data + readsz);
			readsz += sizeof(db_index_schema_t);

			t->indexes[idxc] = init_db_idx(idx->index_name, idxs->num_fields);
			set_dd_index_order(t->indexes[idxc]->idx_schema, idxs->index_order);

			t->indexes[idxc]->idx_schema->record_size = idxs->record_size;
			t->indexes[idxc]->idx_schema->is_unique = idxs->is_unique;
			for(uint8_t idxfc = 0; idxfc < t->indexes[idxc]->idx_schema->num_fields; idxfc++) {
				fieldnum = *(uint32_t *)(data + readsz);
				readsz += sizeof(uint32_t);
				t->indexes[idxc]->idx_schema->fields[idxfc] = &(*dd)->fields[fieldnum];
			}
		}
	}

	return dd;
}

void print_data_dictionary(data_dictionary_t *data_dictionary) {
	printf("Field list:\n");
	for(uint32_t i = 0; i<data_dictionary->num_fields; i++)
		printf("%s (%d bytes)\n", data_dictionary->fields[i].field_name, data_dictionary->fields[i].field_sz);

	printf("Schemas:\n");
	for(uint32_t i = 0; i<data_dictionary->num_schemas; i++) {
		dd_table_schema_t *s = &data_dictionary->schemas[i];
		printf("%s (%d fields total of %ld bytes)\n",
				s->schema_name, s->field_count, sizeof(dd_table_schema_t) + sizeof(dd_datafield_t *) * s->num_fields);
		for(int k = 0; k < s->field_count; k++) {
			dd_datafield_t *f = s->fields[k];
			printf("\t%s (%s", f->field_name, map_enum_to_name(f->fieldtype));
			if ( f->fieldtype == DD_TYPE_STR || f->fieldtype == DD_TYPE_BYTES )
				printf(" %d", f->field_sz);
			printf(")\n");
		}
	}

	printf("Tables:\n");
	for(uint32_t i = 0; i<data_dictionary->num_tables; i++) {
		db_table_t *t = &data_dictionary->tables[i];
		dd_table_schema_t *s = t->schema;

		printf("%s (%" PRIu64 " records)\n", t->table_name, (uint64_t)t->total_record_count);
		printf("\tschema %s (record is %d fields total of %d bytes)\n", s->schema_name, s->field_count, s->record_size);
		for(int k = 0; k < s->field_count; k++) {
			dd_datafield_t *f = s->fields[k];
			printf("\t\t%s (%s", f->field_name, map_enum_to_name(f->fieldtype));
			if ( f->fieldtype == DD_TYPE_STR || f->fieldtype == DD_TYPE_BYTES )
				printf(" %d", f->field_sz);
			printf(")\n");
		}
		printf("\tIndexes:\n");
		for(int k = 0; k < t->num_indexes; k++) {
			db_index_schema_t *idx = t->indexes[k]->idx_schema;
			printf("\t\t%s (%s of order %d - %ld bytes): ",
					t->indexes[k]->index_name, idx->is_unique ? "unique" : "non-unique", idx->index_order, sizeof(db_indexkey_t) + sizeof(char *) * idx->num_fields);
			for(int f = 0; f < idx->num_fields; f++)
				printf("%s%s", f == 0 ? "" : ", ", idx->fields[f]->field_name);
			printf("\n");
		}
	}
}

bool write_data_dictionary_dat(data_dictionary_t *dd, char *outfile) {
	int ofd = 0;
	if ( outfile == NULL ) {
		fprintf(stderr, "Data dictionary out filename is required\n");
		return false;
	}

	if ( (ofd = open(outfile, O_CREAT | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR)) < 0 ) {
		fprintf(stderr, "Cannot open output filename %s: %s\n", outfile, strerror(errno));
		return false;
	}

	printf("Writing %" PRIu32 " fields, %" PRIu32 " schemas, and %" PRIu32 " tables\n",
			dd->num_fields,
			dd->num_schemas,
			dd->num_tables);

	uint32_t major = 0;
	uint16_t minor = 0;
	uint8_t patch = 1;

	write(ofd, &major, sizeof(major));
	write(ofd, &minor, sizeof(minor));
	write(ofd, &patch, sizeof(patch));

	size_t offset = 0, fieldoff = 0, schemaoff = 0, tableoff = 0;
	write(ofd, &dd->num_fields, sizeof(uint32_t));
	fieldoff = sizeof(uint32_t) * 3 + sizeof(size_t) * 3 + sizeof(major) + sizeof(minor) + sizeof(patch);
	write(ofd, &fieldoff, sizeof(size_t));

	write(ofd, &dd->num_schemas, sizeof(uint32_t));
	write(ofd, &schemaoff, sizeof(size_t)); /* this will be calculated after its written */

	write(ofd, &dd->num_tables, sizeof(uint32_t));
	write(ofd, &tableoff, sizeof(size_t)); /* this will be calculated after its written */

	schemaoff = fieldoff;
	for(uint32_t i = 0; i < dd->num_fields; i++) {
		write(ofd, &dd->fields[i], sizeof(dd_datafield_t));
		schemaoff += sizeof(dd_datafield_t);
	}

	tableoff = schemaoff;
	for(uint32_t i = 0; i < dd->num_schemas; i++) {
		dd_table_schema_t *s = &dd->schemas[i];
		write(ofd, s, sizeof(dd_table_schema_t));
		tableoff += sizeof(dd_table_schema_t);
		for(uint32_t sf = 0; sf < s->num_fields; sf++) {
			for(uint32_t fn = 0; fn < dd->num_fields; fn++) {
				if ( s->fields[sf] == &dd->fields[fn] ) {
					write(ofd, &fn, sizeof(uint32_t));
					tableoff += sizeof(uint32_t);
					break;
				}
			}
		}
	}

	for(uint32_t i = 0; i < dd->num_tables; i++) {
		db_table_t *t = &dd->tables[i];
		write(ofd, t, sizeof(db_table_t));

		for(uint32_t s = 0; s < dd->num_schemas; s++) {
			if ( t->schema == &dd->schemas[s] ) {
				write(ofd, &s, sizeof(uint32_t));
				break;
			}
		}
		for(uint32_t idxc = 0; idxc < t->num_indexes; idxc++) {
			db_index_t *idx = t->indexes[idxc];
			write(ofd, idx, sizeof(db_index_t));
			db_index_schema_t *idx_schema = idx->idx_schema;
			write(ofd, idx_schema, sizeof(db_index_schema_t));
			for(uint8_t idxf = 0; idxf < idx_schema->num_fields; idxf++) {
				for(uint32_t fn = 0; fn < dd->num_fields; fn++) {
					if ( idx_schema->fields[idxf] == &dd->fields[fn] ) {
						write(ofd, &fn, sizeof(uint32_t));
						break;
					}
				}
			}
		}
	}
	size_t rv = 0;
	offset = sizeof(major) + sizeof(minor) + sizeof(patch) + sizeof(uint32_t) * 2 + sizeof(size_t);
	rv = lseek(ofd, offset, SEEK_SET);
	printf("Writing schema offset starting at %ld bytes, value of %ld\n", rv, schemaoff);
	write(ofd, &schemaoff, sizeof(size_t)); /* this will be calculated after its written */
	offset += sizeof(size_t) + sizeof(uint32_t);
	rv = lseek(ofd, offset, SEEK_SET);
	printf("Writing table offset starting at %ld bytes, value of %ld\n", rv, tableoff);
	write(ofd, &tableoff, sizeof(size_t)); /* this will be calculated after its written */

	rv = lseek(ofd, 0, SEEK_END);
	printf("Wrote %ld bytes to file\n", rv);
	close(ofd);
	return true;
}

void release_data_dictionary(data_dictionary_t **dd) {
	for(uint32_t tblcnt = 0; tblcnt < (*dd)->num_tables; tblcnt++) {
		db_table_t *t = &(*dd)->tables[tblcnt];
		dd_table_schema_t *s = t->schema;
		if ( t->indexes != NULL ) {
			for(uint8_t idxcnt = 0; idxcnt < t->num_indexes; idxcnt++) {
				if (t->indexes[idxcnt]->nodeset != NULL) {
					free(t->indexes[idxcnt]->nodeset);
					//munmap(t->indexes[idxcnt]->nodeset, t->indexes[idxcnt]->idx_schema->nodekey_size * t->indexes[idxcnt]->total_node_count);
					t->indexes[idxcnt]->root_node = NULL;
					t->indexes[idxcnt]->nodeset = NULL;
				} else if (t->indexes[idxcnt]->root_node != NULL) {
					dbidx_release_tree(t->indexes[idxcnt], NULL);
				}
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

db_table_t *init_db_table(char *table_name, dd_table_schema_t *schema, record_num_t size) {
	if( strlen(table_name) >= DB_OBJECT_NAME_SZ )
		return NULL;

	db_table_t *t = malloc(sizeof(db_table_t));
	bzero(t, sizeof(db_table_t));
	mlock(t, sizeof(db_table_t));
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

	size_t sz = sizeof(dd_table_schema_t);
	dd_table_schema_t *s = malloc(sz);
	bzero(s, sz);
	mlock(s, sz);
	strcpy(s->schema_name, schema_name);
	s->field_count = 0;
	s->record_size = 0;
	s->num_fields = num_fields;
	s->fields = NULL;
	if ( num_fields > 0 ) {
		s->fields = calloc(sizeof(dd_datafield_t *), num_fields);
		mlock(*s->fields, sizeof(*s->fields));
	}
	return s;
}

db_index_t *init_db_idx(char *index_name, uint8_t num_fields) {
	db_index_t *idx = NULL;
	db_index_schema_t *idxschema = NULL;

	size_t memsz = sizeof(db_index_t) + sizeof(db_index_schema_t) + sizeof(dd_datafield_t *) * num_fields;

	idx = (db_index_t *)malloc(memsz);
	bzero(idx, memsz);
	mlock(idx, memsz);

	idxschema = (db_index_schema_t *)((char *)idx + sizeof(db_index_t));
	idxschema->fields = (dd_datafield_t **)((char *)idxschema + sizeof(db_index_schema_t));

	strcpy(idx->index_name, index_name);
	idxschema->is_unique = false;
	idxschema->num_fields = num_fields;
	idxschema->key_size = sizeof(db_indexkey_t) + sizeof(char *) * idxschema->num_fields;
	set_dd_index_order(idxschema, 5);

	idxschema->fields = calloc(num_fields, sizeof(dd_datafield_t *));
	mlock(*idxschema->fields, sizeof(*idxschema->fields));
	idx->idx_schema = idxschema;
	idx->root_node = NULL;
	return idx;
}

void set_dd_index_order(db_index_schema_t *idxs, index_order_t ord) {
	idxs->index_order = ord;
	idxs->node_size = sizeof(db_idxnode_t) + sizeof(db_indexkey_t *) * idxs->index_order;
	idxs->nodekey_size = idxs->node_size +  idxs->key_size * idxs->index_order;
}

dd_datafield_t *init_dd_field_type(char *field_name, datatype_t type, uint8_t size) {
	if( strlen(field_name) >= DB_OBJECT_NAME_SZ )
		return NULL;

	size_t sz = sizeof(dd_datafield_t);
	dd_datafield_t *f = malloc(sz);
	bzero(f, sz);
	mlock(f, sz);
	strcpy(f->field_name, field_name);
	f->field_sz = get_dd_field_size(type, size);
	f->fieldtype = type;

	return f;
}

dd_datafield_t *init_dd_field_str(char *field_name, char *type, uint8_t size) {
	if( strlen(field_name) >= DB_OBJECT_NAME_SZ )
		return NULL;

	datatype_t field_type;
	if (strcmp(type, "STR") == 0)
		field_type = DD_TYPE_STR;
	else if (strcmp(type, "TIMESTAMP") == 0)
		field_type = DD_TYPE_TIMESTAMP;
	else if (strcmp(type, "BOOL") == 0)
		field_type = DD_TYPE_BOOL;
	else if (strcmp(type, "UUID") == 0)
		field_type = DD_TYPE_UUID;
	else if (type[0] == 'I')
		if (strcmp(type, "I64") == 0)
			field_type = DD_TYPE_I64;
		else if (strcmp(type, "I32") == 0)
			field_type = DD_TYPE_I32;
		else if (strcmp(type, "I16") == 0)
			field_type = DD_TYPE_I16;
		else if (strcmp(type, "I8") == 0)
			field_type = DD_TYPE_I8;
		else
			return NULL;
	else if (type[0] == 'U')
		if (strcmp(type, "UI64") == 0)
			field_type = DD_TYPE_UI64;
		else if (strcmp(type, "UI32") == 0)
			field_type = DD_TYPE_UI32;
		else if (strcmp(type, "UI16") == 0)
			field_type = DD_TYPE_UI16;
		else if (strcmp(type, "UI8") == 0)
			field_type = DD_TYPE_UI8;
		else
			return NULL;
	else if (strcmp(type, "BYTES") == 0)
		field_type = DD_TYPE_BYTES;
	else
		return NULL;

	return init_dd_field_type(field_name, field_type, size);
}

const char *map_enum_to_name(datatype_t position) {
	return datatype_names[position];
}

bool dd_type_to_str(dd_datafield_t *field, char *data, char *str) {
	bool rv = false;
	char timestampstr[31];

	switch (field->fieldtype) {
	case DD_TYPE_STR:
		snprintf(str, field->field_sz + 1, "%s", data);
		rv = true;
		break;
	case DD_TYPE_TIMESTAMP:
		bzero(&timestampstr, sizeof(timestampstr));
		if ( data != NULL )
			format_timestamp((struct timespec *)data, timestampstr);
		sprintf(str, "%s", timestampstr);
		rv = true;
		break;
	case DD_TYPE_UUID:
		uuid_unparse(*(uuid_t *)data, str);
		rv = true;
		break;
	case DD_TYPE_UI64:
		sprintf(str, "%" PRIu64, *(uint64_t *)data);
		rv = true;
		break;
	case DD_TYPE_I64:
		sprintf(str, "%" PRIi64, *(int64_t *)data);
		rv = true;
		break;
	case DD_TYPE_UI32:
		sprintf(str, "%" PRIu32, *(uint32_t *)data);
		rv = true;
		break;
	case DD_TYPE_I32:
		sprintf(str, "%" PRIi32, *(int32_t *)data);
		rv = true;
		break;
	case DD_TYPE_UI16:
		sprintf(str, "%" PRIu16, *(uint16_t *)data);
		rv = true;
		break;
	case DD_TYPE_I16:
		sprintf(str, "%" PRIi16, *(int16_t *)data);
		rv = true;
		break;
	case DD_TYPE_UI8:
		sprintf(str, "%" PRIu8, *(uint8_t *)data);
		rv = true;
		break;
	case DD_TYPE_I8:
		sprintf(str, "%" PRIi8, *(int8_t *)data);
		rv = true;
		break;
	case DD_TYPE_BOOL:
		sprintf(str, "%s", *(bool *)data == true ? "true" : "false");
		rv = true;
		break;
	case DD_TYPE_BYTES:
		for(uint32_t i = 0; i < field->field_sz; i++)
			sprintf(str + i * 2, "%02x", ((unsigned char *)data)[i]);
		rv = true;
		break;
	default:
		break;
	}
	return rv;
}

bool dd_type_to_allocstr(dd_datafield_t *field, char *indata, char **outstr) {
	bool rv = false;
	char *data = NULL;
	size_t sl = dd_type_strlen(field);
	if ( sl > 0 ) {
		data = malloc(sl);
		bzero(data, sl);
	}

	if ( data != NULL ) {
		*outstr = data;
		return dd_type_to_str(field, indata, data);
	} else
		return rv;
}

size_t dd_type_strlen(dd_datafield_t *field) {
	size_t field_str_size = 0;
	switch(field->fieldtype) {
		case DD_TYPE_STR:
			field_str_size = field->field_sz + 1;
			break;
		case DD_TYPE_TIMESTAMP:
			field_str_size = 31; // 2023-01-01T00:00:00.000000000Z
			break;
		case DD_TYPE_BOOL:
			field_str_size = 6; // true / false
			break;
		case DD_TYPE_I8:
		case DD_TYPE_UI8:
			field_str_size = 5; // -127
			break;
		case DD_TYPE_I16:
		case DD_TYPE_UI16:
			field_str_size = 7; // -32767
			break;
		case DD_TYPE_I32:
		case DD_TYPE_UI32:
			field_str_size = 12; // -2147483647
			break;
		case DD_TYPE_I64:
		case DD_TYPE_UI64:
			field_str_size = 21; // -9223372036854775807
			break;
		case DD_TYPE_UUID:
			field_str_size = 37; // 00000000-0000-0000-0000-000000000000
			break;
		case DD_TYPE_BYTES:
			field_str_size = (field->field_sz * 2) + 1;
			break;
		default:
			fprintf(stderr, "Unknown field type");
	}
	return field_str_size;
}

bool str_to_dd_type(dd_datafield_t *field, char *instr, char *outdata) {
	bool rv = false;
	int ival = 0;
	char *err = NULL;
	uint64_t puv = 0;
	int64_t piv = 0;
	unsigned char hex = 0;
	unsigned int hexi = 0;

	switch (field->fieldtype) {
	case DD_TYPE_STR:
		if( strlen(instr) <= field->field_sz ) {
			memcpy(outdata, instr, field->field_sz);
			//snprintf(outdata, field->field_sz, "%s", instr);
			rv = true;
		}
		break;
	case DD_TYPE_TIMESTAMP:
		if ( is_utc_timestamp(instr) && parse_timestamp(instr, (struct timespec *)outdata) )
			rv = true;
		break;
	case DD_TYPE_UUID:
		if ( uuid_parse(instr, *(uuid_t *)outdata) == 0 )
			rv = true;
		break;
	case DD_TYPE_UI64:
		puv = strtoumax(instr, &err, 0);
		if( puv < UINT64_MAX ) {
			*(uint64_t *)outdata = puv;
			rv = true;
		}
		//if ( errno == ERANGE ) {}
		break;
	case DD_TYPE_I64:
		piv = strtoimax(instr, &err, 0);
		if( piv < INT64_MAX ) {
			*(int64_t *)outdata = puv;
			rv = true;
		}
		break;
	case DD_TYPE_UI32:
		puv = strtoumax(instr, &err, 0);
		if( puv < UINT32_MAX ) {
			*(uint32_t *)outdata = puv;
			rv = true;
		}
		break;
	case DD_TYPE_I32:
		piv = strtoimax(instr, &err, 0);
		if( piv < INT32_MAX ) {
			*(int32_t *)outdata = puv;
			rv = true;
		}
		break;
	case DD_TYPE_UI16:
		puv = strtoumax(instr, &err, 0);
		if( puv < UINT16_MAX ) {
			*(uint16_t *)outdata = puv;
			rv = true;
		}
		break;
	case DD_TYPE_I16:
		piv = strtoimax(instr, &err, 0);
		if( piv < INT16_MAX ) {
			*(int16_t *)outdata = puv;
			rv = true;
		}
		break;
	case DD_TYPE_UI8:
		puv = strtoumax(instr, &err, 0);
		if( puv < UINT8_MAX ) {
			*(uint8_t *)outdata = puv;
			rv = true;
		}
		break;
	case DD_TYPE_I8:
		piv = strtoimax(instr, &err, 0);
		if( piv < INT8_MAX ) {
			*(int8_t *)outdata = puv;
			rv = true;
		}
		break;
	case DD_TYPE_BOOL:
		if ( strcmp(instr, "true") == 0 ) {
			*(bool *)outdata = true;
			rv = true;
		} else if ( strcmp(instr, "false") == 0 ) {
			*(bool *)outdata = false;
			rv = true;
		}
		break;
	case DD_TYPE_BYTES:
		if ( strlen(instr) % 4 == 0 ) {
			hex = 0;
			for(puv = 0; puv < strlen(instr) / 8; puv++) {
				if ( (ival = sscanf(instr + puv * 8, "%08X", &hexi)) == EOF ) {
					hex++;
					break;
				} else {
					hexi = htonl(hexi);
					memcpy(outdata + sizeof(unsigned int) * puv, &hexi, sizeof(unsigned int));
				}
			}
			if (hex == 0)
				rv = true;
		} else if ( strlen(instr) % 2 == 0 ) {
			for(puv = 0; puv < strlen(instr) / 2; puv++) {
			}
		}
		break;
	default:
		break;
	}
	return rv;
}

void idx_key_to_str(db_index_schema_t *idx, db_indexkey_t *key, char *buff) {
	bzero(buff, sizeof(*buff));

	char *p = buff;
	size_t offset = 0;

	for( uint8_t i = 0; i < idx->num_fields; i++ ) {
		if ( i > 0) {
			sprintf(p, ", ");
			p = buff + strlen(buff);
		}
		dd_type_to_str(idx->fields[i], (*key->data + offset), p);
		offset += idx->fields[i]->field_sz;
		p = buff + strlen(buff);
	}
}

int add_dd_table(data_dictionary_t **dd, db_table_t *table, db_table_t **created) {
	for(uint32_t i = 0; i < (*dd)->num_tables; i++)
		if(strcmp((*dd)->tables[i].table_name, table->table_name) == 0) // prevents a duplicate entry
			return 0;

	if ( (*dd)->num_tables >= (*dd)->num_alloc_tables )
		return 0;

	memcpy(((*dd)->tables + (*dd)->num_tables), table, sizeof(db_table_t));
	if ( created != NULL )
		*created = &(*dd)->tables[(*dd)->num_tables];
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
	case DD_TYPE_TIMESTAMP:
		size = sizeof(struct timespec);
		break;

	case DD_TYPE_BOOL:
		size = sizeof(bool);
		break;

	case DD_TYPE_UUID:
		size = sizeof(uuid_t);
		break;

	case DD_TYPE_UI64:
		size = sizeof(uint64_t);
		break;
	case DD_TYPE_I64:
		size = sizeof(int64_t);
		break;

	case DD_TYPE_UI32:
		size = sizeof(uint32_t);
		break;
	case DD_TYPE_I32:
		size = sizeof(int32_t);
		break;

	case DD_TYPE_UI16:
		size = sizeof(uint16_t);
		break;
	case DD_TYPE_I16:
		size = sizeof(int16_t);
		break;

	case DD_TYPE_UI8:
		size = sizeof(uint8_t);
		break;
	case DD_TYPE_I8:
		size = sizeof(int8_t);
		break;

	case DD_TYPE_BYTES:
		size = sizeof(unsigned char) * size;
		break;
	default:
		break;
	}
	return size;
}

int add_dd_table_schema_field(dd_table_schema_t *s, dd_datafield_t *f) {
	if ( s->field_count + 1 > s->num_fields ) {
		fprintf(stderr, "Resizing schema fields\n");
		s->num_fields++;
		s->fields = realloc(s->fields, sizeof(dd_datafield_t *) * s->num_fields);
	}

	s->fields[s->field_count] = f;
	(s->field_count)++;
	s->record_size += f->field_sz;
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

signed char bytes_compare(const unsigned char *a, const unsigned char *b, size_t n) {
	for(size_t i = 0; i < n; i++) {
		if ( a[i] > b[i])
			return 1;
		else if ( a[i] < b[i])
			return -1;
	}
	return 0;
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
