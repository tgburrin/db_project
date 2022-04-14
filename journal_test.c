/*
 * journal_test.c
 *
 *  Created on: Apr 19, 2022
 *      Author: tgburrin
 */

#include "table_tools.h"
#include "index_tools.h"
#include "journal_tools.h"

typedef struct IndexNameKey {
	// requires -fms-extensions option for gcc
	struct IndexKey;
	char first_name[33];
	char last_name[33];
} idxnamekey_t;

typedef struct Person {
	uint64_t person_id;
	char first_name[33];
	char last_name[33];
	bool is_active;
} person_t;

int journal_test (int argc, char **argv) {
	journal_t jnl;
	bzero(&jnl, sizeof(journal_t));

	new_journal(&jnl);
	journal_record_t *jr = NULL;
	size_t recsz = sizeof(journal_record_t)+sizeof(person_t);
	jr = malloc(recsz);
	bzero(jr, recsz);

	person_t jd;
	bzero(&jd, sizeof(person_t));
	jd.person_id = 1;
	strcpy(jd.first_name, "John");
	strcpy(jd.last_name, "Doe");
	jd.is_active = true;

	jr->msgsz = recsz;
	jr->objtype = 'i';
	strcpy(jr->objname, "person");
	jr->objsz = sizeof(person_t);
	jr->objdata = &jd;

	write_journal_record(&jnl, jr);
	free(jr);
	jr = NULL;
	close_journal(&jnl);

	open_journal(&jnl);
	printf("Reading records from journal\n");
	while( (jr = read_journal(&jnl)) != NULL ) {
		printf("Read journal record %c\n", jr->objtype);
		free(jr);
	}
	close_journal(&jnl);
	return 0;
}
