/*
 * index_tools.h
 *
 *  Created on: Mar 26, 2022
 *      Author: tgburrin
 */

#ifndef INDEX_TOOLS_H_
#define INDEX_TOOLS_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdbool.h>
#include <string.h>

#include "utils.h"
#include "data_dictionary.h"

db_idxnode_t *dbidx_init_root_node(db_index_schema_t *);
db_idxnode_t *dbidx_allocate_node(db_index_schema_t *);
db_indexkey_t *dbidx_allocate_key(db_index_schema_t *); /* allocates just the key, data must be maintained separately */
void dbidx_reset_key(db_index_schema_t *, db_indexkey_t *);
void dbidx_reset_key_with_data(db_index_schema_t *, db_indexkey_t *);
char *dbidx_allocate_key_data(db_index_schema_t *); /* allocates just the data to be attached to the key */
/* the allocates both key and data, attaches it to the key, but copies will not account for the data payload
 * it is useful only for comparisons */
db_indexkey_t *dbidx_allocate_key_with_data(db_index_schema_t *);
bool dbidx_copy_key(db_indexkey_t *, db_indexkey_t *);

void dbidx_release_tree(db_index_t *, db_idxnode_t *);

bool dbidx_set_key_data_field_value(db_index_schema_t *, char *, char *, char *);
bool dbidx_set_key_field_value(db_index_schema_t *, char *, db_indexkey_t *, char *);
signed char dbidx_compare_keys(db_index_schema_t *, db_indexkey_t *, db_indexkey_t *);

uint64_t dbidx_num_child_records(db_idxnode_t *);
db_indexkey_t *dbidx_find_record(db_index_schema_t *, db_idxnode_t *, db_indexkey_t *);
signed char dbidx_find_node_index(db_index_schema_t *, db_idxnode_t *, db_indexkey_t *, index_order_t *);
db_idxnode_t *dbidx_find_node(db_index_schema_t *, db_idxnode_t *, db_indexkey_t *);

bool dbidx_add_index_value (db_index_t *, db_idxnode_t *, db_indexkey_t *);
bool dbidx_remove_index_value (db_index_t *, db_idxnode_t *, db_indexkey_t *);

db_idxnode_t *dbidx_add_node_value(db_index_schema_t *, db_idxnode_t *, db_indexkey_t *);
bool dbidx_remove_node_value(db_index_schema_t *idx, db_idxnode_t *idxnode, db_indexkey_t *key);

db_idxnode_t *dbidx_split_node(db_index_schema_t *, db_idxnode_t *, db_indexkey_t *);
void dbidx_collapse_nodes(db_index_schema_t *, db_idxnode_t *);

void dbidx_update_max_value (db_idxnode_t *, db_idxnode_t *, db_indexkey_t *);

void dbidx_print_tree(db_index_t *, db_idxnode_t *, uint64_t *);
void dbidx_print_tree_totals(db_index_t *, db_idxnode_t *, uint64_t *);
void dbidx_print_index_scan_lookup(db_index_t *idx, db_indexkey_t *key);

void dbidx_read_file_records(db_index_t *idx);
void dbidx_write_file_records(db_index_t *);
void dbidx_write_file_keys(db_index_t *);

#endif /* INDEX_TOOLS_H_ */
