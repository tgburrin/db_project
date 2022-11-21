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
	int errs;
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
		char *err = NULL;
		if ( (err = cJSON_GetErrorPtr()) != NULL )
			fprintf(stderr, "Error parsing json: %s\n", err);
		exit(EXIT_FAILURE);
	} else
		free(filedata);

	printf("%s\n", cJSON_PrintUnformatted(doc));
	cJSON *fields = cJSON_GetObjectItemCaseSensitive(doc, "fields");
	printf("%s\n", cJSON_PrintUnformatted(fields));
	cJSON *attr = fields->child;
	while(attr != NULL) {
		printf("%s\n", attr->string);
		attr = attr->next;
	}
	exit(EXIT_SUCCESS);

	printf("Running data dictionary test\n");
	dd_schema_t *schema = init_dd_schema("test_schema");

	dd_datafield_t *test_str_field = init_dd_field_str("test_str_field", "STR", (uint8_t)20);
	add_dd_schema_field(schema, test_str_field);
	dd_datafield_t *test_int_field = init_dd_field_str("test_int_field", "I8", (uint8_t)20);
	add_dd_schema_field(schema, test_int_field);

	for ( int i = 0; i < schema->field_count; i++) {
		dd_datafield_t *f = &schema->fields[i];
		printf("(%d): %s of type %d size %d\n", i, f->fieldname, f->fieldtype, f->fieldsz);
	}

	printf("Freeing fields\n");
	free(test_str_field);
	free(test_int_field);

	printf("Freeing schema\n");
	free(schema->fields);
	free(schema);

	printf("exiting\n");
	return EXIT_SUCCESS;
}
