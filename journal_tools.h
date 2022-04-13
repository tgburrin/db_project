/*
 * journal_tools.h
 *
 *  Created on: Apr 11, 2022
 *      Author: tgburrin
 */

#ifndef JOURNAL_TOOLS_H_
#define JOURNAL_TOOLS_H_

#include "table_tools.h"
#include "utils.h"

typedef struct Journal {
	int jfd;
} journal_t;

typedef struct JournalRecord {
	size_t msgsz;
	struct timespec txntime;
	char objtype;
	char objname[DB_OBJECT_NAME_SZ];
	void *objdata;
} journal_record_t;

void new_journal (journal_t *j);
void open_journal (journal_t *j);
void close_journal (journal_t *);
void read_journal (journal_t *j);
void print_journal_record(journal_record_t *jr);

#endif /* JOURNAL_TOOLS_H_ */
