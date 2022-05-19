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

#include "utils.h"

void read_index_from_record_numbers(table_t *tbl, index_t *idx);
void write_record_numbers_from_index(index_t *idx);

#endif /* DB_INTERFACE_H_ */
