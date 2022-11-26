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

#include <cjson/cJSON.h>

#include <table_tools.h>
#include <data_dictionary.h>

#define SUBSCRIPTION_ID_LENTH 18
#define CUSTOMER_ID_LENTH 18

typedef struct Records {
	uuid_t record_id;
	struct timespec created_dt;
	char record_msg[128];
	uint64_t record_counter;
} record_t;

int main (int argc, char **argv) {
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

	data_dictionary_t **data_dictionary = NULL;
	if ( (data_dictionary = build_dd_from_json(dd_filename)) == NULL ) {
		fprintf(stderr, "Error while building data dictionary from file %s\n", dd_filename);
		exit(EXIT_FAILURE);
	}

	printf("Running data dictionary test\n");
	printf("Field list:\n");
	for(int i = 0; i<(*data_dictionary)->num_fields; i++)
		printf("%s\n", (&(*data_dictionary)->fields[i])->field_name);

	dd_table_schema_t *subs = NULL;
	printf("Schemas:\n");
	for(int i = 0; i<(*data_dictionary)->num_schemas; i++) {
		dd_table_schema_t *s = &(*data_dictionary)->schemas[i];
		printf("%s (%d fields total of %d bytes)\n", s->schema_name, s->field_count, s->record_size);
		for(int k = 0; k < s->field_count; k++) {
			dd_datafield_t *f = &s->fields[k];
			printf("\t%s (%s", f->field_name, map_enum_to_name(f->fieldtype));
			if ( f->fieldtype == STR )
				printf(" %d", f->fieldsz);
			printf(")\n");
		}
		if ( strcmp(s->schema_name, "subscriptions") == 0 )
			subs = s;
	}

	db_table_t *tbl = NULL;
	db_table_t *mapped_tbl = NULL;

	printf("Tables:\n");
	for(int i = 0; i<(*data_dictionary)->num_tables; i++) {
		db_table_t *t = &(*data_dictionary)->tables[i];
		dd_table_schema_t *s = t->schema;

		if ( strcmp(t->table_name, "subscriptions") == 0 )
			printf("%p vs %p\n", t->schema, subs);

		printf("%s\n", t->table_name);
		printf("\tschema %s (%d fields total of %d bytes)\n", s->schema_name, s->field_count, s->record_size);
		for(int k = 0; k < s->field_count; k++) {
			dd_datafield_t *f = &s->fields[k];
			printf("\t\t%s (%s", f->field_name, map_enum_to_name(f->fieldtype));
			if ( f->fieldtype == STR )
				printf(" %d", f->fieldsz);
			printf(")\n");
		}

		if ( strcmp(t->table_name, "test_table") == 0 )
			tbl = t;
	}

	if ( tbl != NULL ) {
		char rec_uuid[37];
		bzero(&rec_uuid, sizeof(rec_uuid));

		printf("Running test with table %s\n", tbl->table_name);
		record_t rec;
		printf("Record is %ld bytes in size\n", sizeof(record_t));
		uuid_generate_random(rec.record_id);
		uuid_unparse_lower(rec.record_id, rec_uuid);

		clock_gettime(CLOCK_REALTIME, &rec.created_dt);
		strcpy(rec.record_msg, "hello world");
		rec.record_counter = 10;

		record_t *outrec = NULL;

		open_dd_table(tbl, &mapped_tbl);
		uint64_t rs1 = add_db_record(mapped_tbl, (char *)&rec);
		printf("Record added %s to %" PRIu64 "\n", rec_uuid, rs1);

		outrec = (record_t *)read_db_record(mapped_tbl, rs1);
		bzero(&rec_uuid, sizeof(rec_uuid));
		uuid_unparse_lower(outrec->record_id, rec_uuid);

		uuid_generate_random(rec.record_id);
		bzero(&rec.record_msg, sizeof(rec.record_msg));
		strcpy(rec.record_msg, "goodbye world");
		uint64_t rs2 = add_db_record(mapped_tbl, (char *)&rec);
		bzero(&rec_uuid, sizeof(rec_uuid));
		uuid_unparse_lower(rec.record_id, rec_uuid);
		printf("Record added %s to %" PRIu64 "\n", rec_uuid, rs2);

		bzero(&rec_uuid, sizeof(rec_uuid));
		uuid_unparse_lower(outrec->record_id, rec_uuid);
		printf("Message: %s (%s)\n", outrec->record_msg, rec_uuid);

		outrec = (record_t *)read_db_record(mapped_tbl, rs2);

		delete_db_record(mapped_tbl, rs1, (char *)&rec);

		bzero(&rec_uuid, sizeof(rec_uuid));
		uuid_unparse_lower(outrec->record_id, rec_uuid);
		printf("Message: %s (%s)\n", outrec->record_msg, rec_uuid);

		bzero(&rec_uuid, sizeof(rec_uuid));
		uuid_unparse_lower(rec.record_id, rec_uuid);
		printf("Deleted: %s (%s)\n", rec.record_msg, rec_uuid);

		close_dd_table(mapped_tbl);
	}

	release_data_dictionary(data_dictionary);

	printf("exiting\n");
	return EXIT_SUCCESS;
}
