/*
 * data_dictionary_test.c
 *
 *  Created on: Nov 20, 2022
 *      Author: tgburrin
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <strings.h>

#include <cjson/cJSON.h>

#include "data_dictionary.h"

#define SUBSCRIPTION_ID_LENTH 18
#define CUSTOMER_ID_LENTH 18

typedef struct Records {
	uuid_t record_id;
	struct timespec created_dt;
	char record_msg[129];
	uint64_t record_counter;
} record_t;

char *read_json_file(char *filename) {
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
		printf("Reading up to %ld bytes from file\n", buffsz);
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
			printf("Read %ld bytes...allocating more space\n", i * buffsz);
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
	printf("File of %ld bytes loaded\n", strlen(rv));

	return rv;
}

int data_dictionary_test (int argc, char **argv) {
	int errs = 0;
	char c;
	char *dd_filename = NULL;

	while ((c = getopt(argc, argv, "d:")) != -1) {
		switch(c) {
		case 'd': ;
			dd_filename = optarg;
			break;
		case ':': ;
			fprintf(stderr, "Option -%c requires an option\n", optopt);
			errs++;
			break;
		case '?': ;
			fprintf(stderr, "Unknown option '-%c'\n", optopt);
		}
	}

	if ( errs > 0 )
		exit(EXIT_FAILURE);

	if ( dd_filename == NULL ) {
		fprintf(stderr, "Data dictionary filename (-d) is required\n");
		exit(EXIT_FAILURE);
	}

	char *filedata = NULL;
	if ( (filedata = read_json_file(dd_filename)) == NULL ) {
		fprintf(stderr, "Error reading from %s\n", dd_filename);
		exit(EXIT_FAILURE);
	}

	cJSON *doc = NULL;
	if ( (doc = cJSON_Parse(filedata)) == NULL ) {
		const char *err = NULL;
		if ( (err = cJSON_GetErrorPtr()) != NULL )
			fprintf(stderr, "Error parsing json: %s\n", err);
		exit(EXIT_FAILURE);
	} else
		free(filedata);

	cJSON *fields = cJSON_GetObjectItemCaseSensitive(doc, "fields");
	data_dictionary_t **data_dictionary = init_data_dictionary();

	cJSON *attr = fields->child;
	while(attr != NULL) {
		char *type = NULL;
		uint8_t size = 0;
		cJSON *c = NULL;
		if ( (c = cJSON_GetObjectItemCaseSensitive(attr, "type")) == NULL || !cJSON_IsString(c)) {
			fprintf(stderr, "type not provided for field: %s\n", attr->string);
			attr = attr->next;
			continue;
		} else
			type = cJSON_GetStringValue(c);

		if ( (c = cJSON_GetObjectItemCaseSensitive(attr, "size")) != NULL && cJSON_IsNumber(c) ) {
			double sv = cJSON_GetNumberValue(c);
			if ( sv >= UINT8_MAX || sv <= 0) {
				fprintf(stderr, "size %lf must be between 1 and %d\n", sv, UINT8_MAX - 1);
				attr = attr->next;
				continue;
			} else
				size = (uint8_t)sv;
		}

		dd_datafield_t *field = NULL;
		if ( (field = init_dd_field_str(attr->string, type, size)) == NULL ) {
			fprintf(stderr, "could not create dd field %s (%s)\n", attr->string, type);
			attr = attr->next;
			continue;
		}
		if ( !add_dd_field(data_dictionary, field)) {
			fprintf(stderr, "could add dd field %s to dictionary\n", field->field_name);
			attr = attr->next;
			continue;
		} else {
			printf("Added field %s to dictionary\n", field->field_name);
			free(field);
		}

		attr = attr->next;
	}

	cJSON *tables = cJSON_GetObjectItemCaseSensitive(doc, "tables");
	attr = tables->child;
	while(attr != NULL) {
		printf("Creating schema and table for %s\n", attr->string);
		dd_schema_t *schema = init_dd_schema(attr->string);
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
				for(int i = 0; i<(*data_dictionary)->num_fields; i++) {
					dd_datafield_t *cf = (*data_dictionary)->fields + i;
					if ( strcmp(cf->field_name, table_field) == 0 ) {
						field = cf;
						break;
					}
				}
				if ( field == NULL ) {
					fprintf(stderr, "field %s is defined on table %s, but does not have its own definition\n", table_field, attr->string);
					continue;
				}
				add_dd_schema_field(schema, field);
			}
		}
		add_dd_schema(data_dictionary, schema);
		free(schema);
		schema = NULL;
		for(int i = 0; i<(*data_dictionary)->num_schemas; i++) {
			schema = ((*data_dictionary)->schemas + i);
			if ( strcmp(schema->schema_name, attr->string) == 0 )
				break;
		}

		if ( schema != NULL ) {
			dd_table_t *tbl = init_dd_table(attr->string, schema, table_size);
			add_dd_table(data_dictionary, tbl);
			free(tbl);
		}

		attr = attr->next;
	}
	printf("Running data dictionary test\n");
	printf("Field list:\n");
	for(int i = 0; i<(*data_dictionary)->num_fields; i++)
		printf("%s\n", ((*data_dictionary)->fields + i)->field_name);

	printf("Schemas:\n");
	for(int i = 0; i<(*data_dictionary)->num_schemas; i++) {
		dd_schema_t *s = ((*data_dictionary)->schemas + i);
		printf("%s (%d fields total of %d bytes)\n", s->schema_name, s->field_count, s->record_size);
		for(int k = 0; k < s->field_count; k++) {
			dd_datafield_t *f = s->fields[k];
			printf("\t%s (%s", f->field_name, map_enum_to_name(f->fieldtype));
			if ( f->fieldtype == STR )
				printf(" %d", f->fieldsz);
			printf(")\n");
		}
	}


	exit(EXIT_SUCCESS);

	record_t *recs = malloc(sizeof(record_t) * 1000);

	free(recs);
	printf("exiting\n");
	return EXIT_SUCCESS;
}
