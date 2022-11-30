/*
 * journal_tools.h
 *
 *  Created on: Apr 11, 2022
 *      Author: tgburrin
 */

#ifndef JOURNAL_TOOLS_H_
#define JOURNAL_TOOLS_H_

#include <unistd.h>

#include "table_tools.h"
#include "utils.h"

#define DEFAULT_JOURNAL "txn.jnl"

typedef struct Journal {
	uint8_t opened;
	struct timespec openedtm;
	bool fsync_on;
	int jfd;
	FILE *jfp;
} journal_t;

typedef struct JournalRecord {
	size_t msgsz;
	struct timespec txntime;
	char objtype; // j = journal, i = insert, u = update, d = delete
	char objkey[DB_OBJECT_NAME_SZ];
	char objname[DB_OBJECT_NAME_SZ];
	size_t objsz;
	char *objdata;
} journal_record_t;

bool new_journal (journal_t *j);
bool open_journal (journal_t *j);
bool journal_sync_off(journal_t *j);
bool journal_sync_on(journal_t *j);
void close_journal (journal_t *);
journal_record_t *read_journal (journal_t *j);
bool write_journal_record(journal_t *j, journal_record_t *jr);
void print_journal_record(journal_record_t *jr);

#endif /* JOURNAL_TOOLS_H_ */
