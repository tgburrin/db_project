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

	data_dictionary_t **data_dictionary = NULL;
	if ( (data_dictionary = build_dd_from_json(dd_in_filename)) == NULL ) {
		fprintf(stderr, "Error while building data dictionary from file %s\n", dd_in_filename);
		exit(EXIT_FAILURE);
	}

	print_data_dictionary(*data_dictionary);
	write_data_dictionary_dat(*data_dictionary, dd_out_filename);
	release_data_dictionary(data_dictionary);

	printf("exiting\n");
	return EXIT_SUCCESS;
}
