/*
 * db_interface.h
 *
 *  Created on: May 17, 2022
 *      Author: tgburrin
 */

#ifndef DB_INTERFACE_H_
#define DB_INTERFACE_H_

#include "table_tools.h"
#include "index_tools.h"
#include "journal_tools.h"
#include "server_tools.h"
#include "db_interface.h"

#include "utils.h"

void read_index_from_record_numbers(table_t *tbl, index_t *idx);
void write_record_numbers_from_index(index_t *idx);

uint64_t add_db_record(db_table_t *, char *);
bool delete_db_record(db_table_t *, char *, char *);
char * read_db_record(db_table_t *, uint64_t);

/*
void print_db_record(db_table_t *, uint64_t, char **);
char * find_db_record(db_table_t *, char *, char *);
*/
#endif /* DB_INTERFACE_H_ */
