/*
 ============================================================================
 Name        : db_dictionary.c
 Author      : Tim Burrington
 Version     :
 Copyright   : 
 Description : Hello World in C, Ansi-style
 ============================================================================
 */
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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <cjson/cJSON.h>

#include <table_tools.h>
#include <index_tools.h>
#include <data_dictionary.h>

typedef struct Records {
	uuid_t record_id;
	struct timespec created_dt;
	char record_msg[128];
	uint64_t record_counter;
} record_t;

int main (int argc, char **argv) {
	int errs = 0, ofd = 0;
	char c;
	char *dd_in_filename = NULL, *dd_out_filename = NULL;

	while ((c = getopt(argc, argv, "d:o:")) != -1) {
		switch(c) {
		case 'd': ;
			dd_in_filename = optarg;
			break;
		case 'o': ;
			dd_out_filename = optarg;
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

	if ( dd_in_filename == NULL ) {
		fprintf(stderr, "Data dictionary in filename (-d) is required\n");
		exit(EXIT_FAILURE);
	}

	if ( dd_out_filename == NULL ) {
		fprintf(stderr, "Data dictionary out filename (-o) is required\n");
		exit(EXIT_FAILURE);
	}

	if ( (ofd = open(dd_out_filename, O_CREAT | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR)) < 0 ) {
		fprintf(stderr, "Cannot open output filename %s: %s\n", dd_out_filename, strerror(errno));
		exit(EXIT_FAILURE);
	}

	data_dictionary_t **data_dictionary = NULL;
	if ( (data_dictionary = build_dd_from_json(dd_in_filename)) == NULL ) {
		fprintf(stderr, "Error while building data dictionary from file %s\n", dd_in_filename);
		exit(EXIT_FAILURE);
	}

	printf("Running data dictionary test\n");
	printf("Field list:\n");
	for(int i = 0; i<(*data_dictionary)->num_fields; i++)
		printf("%s\n", (&(*data_dictionary)->fields[i])->field_name);

	printf("Schemas:\n");
	for(int i = 0; i<(*data_dictionary)->num_schemas; i++) {
		dd_table_schema_t *s = &(*data_dictionary)->schemas[i];
		printf("%s (%d fields total of %d bytes)\n", s->schema_name, s->field_count, s->record_size);
		for(int k = 0; k < s->field_count; k++) {
			dd_datafield_t *f = s->fields[k];
			printf("\t%s (%s", f->field_name, map_enum_to_name(f->fieldtype));
			if ( f->fieldtype == STR )
				printf(" %d", f->fieldsz);
			printf(")\n");
		}
	}

	printf("Tables:\n");
	for(int i = 0; i<(*data_dictionary)->num_tables; i++) {
		db_table_t *t = &(*data_dictionary)->tables[i];
		dd_table_schema_t *s = t->schema;

		printf("%s\n", t->table_name);
		printf("\tschema %s (%d fields total of %d bytes)\n", s->schema_name, s->field_count, s->record_size);
		for(int k = 0; k < s->field_count; k++) {
			dd_datafield_t *f = s->fields[k];
			printf("\t\t%s (%s", f->field_name, map_enum_to_name(f->fieldtype));
			if ( f->fieldtype == STR )
				printf(" %d", f->fieldsz);
			printf(")\n");
		}
		printf("\tIndexes:\n");
		for(int k = 0; k < t->num_indexes; k++) {
			db_index_schema_t *idx = t->indexes[k]->idx_schema;
			printf("\t\t%s (%s of order %d): ", t->indexes[k]->index_name, idx->is_unique ? "unique" : "non-unique", idx->index_order);
			for(int f = 0; f < idx->fields_sz; f++)
				printf("%s%s", f == 0 ? "" : ", ", idx->fields[f]->field_name);
			printf("\n");
		}
	}
	write(ofd, &(*data_dictionary)->num_fields, sizeof(uint32_t));
	write(ofd, &(*data_dictionary)->num_schemas, sizeof(uint32_t));
	write(ofd, &(*data_dictionary)->num_tables, sizeof(uint32_t));
	for(uint32_t i = 0; i < (*data_dictionary)->num_fields; i++)
		write(ofd, &(*data_dictionary)->fields[i], sizeof(dd_datafield_t));

	close(ofd);
	release_data_dictionary(data_dictionary);

	printf("exiting\n");
	return EXIT_SUCCESS;
}
