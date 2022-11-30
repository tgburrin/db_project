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

#define IDX_ORDER 5

typedef struct IndexNode {
	bool is_leaf;
	uint16_t num_children;

	struct idxnode_t *parent;
	struct idxnode_t *next;
	struct idxnode_t *prev;

	char *children[IDX_ORDER];
} idxnode_t;

typedef struct IndexKey {
	idxnode_t *childnode;
	uint64_t record;
} indexkey_t;

typedef int (*compare_key_f)(char *, char *);
typedef char * (*copy_key_f)(char *, char *); // in key, out key
typedef char * (*create_key_f)(void);
typedef void (*release_key_f)(char *);
typedef char * (*create_record_key_f)(char *);
typedef void (*set_key_value_f)(char *, uint64_t);
typedef uint64_t (*get_key_value_f)(char *);
typedef void (*print_key_f)(char *, char *);

typedef struct Index {
	char index_name[DB_OBJECT_NAME_SZ];
	uint16_t record_size;

	bool is_unique;
	idxnode_t root_node;
	compare_key_f compare_key;
	create_key_f create_key;
	release_key_f release_key;
	create_record_key_f create_record_key;
	copy_key_f copy_key;
	set_key_value_f set_key_value;
	get_key_value_f get_key_value;
	print_key_f print_key;
} index_t;

void read_index_from_file(index_t *idx);
void write_file_from_index(index_t *idx);

/* old static record functions */
void init_index_node(idxnode_t *idxnode);

void print_tree(index_t *idx, idxnode_t *idxnode, int *counter);
void print_tree_totals(index_t *idx, idxnode_t *idxnode, int *counter);
void print_index_scan_lookup(index_t *idx, char *key);

void release_tree(index_t *idx, idxnode_t *idxnode);

bool add_index_value (index_t *idx, idxnode_t *idxnode, char *key);
bool remove_index_value (index_t *idx, idxnode_t *idxnode, char *key);

void collapse_nodes(idxnode_t *idxnode);
bool remove_node_value(index_t *idx, idxnode_t *idxnode, char *key);

void update_max_value (index_t *idx, idxnode_t *parent_idx, idxnode_t *idxnode, char *new_key);
idxnode_t *split_node(index_t *idx, idxnode_t *idxnode, char *key);
idxnode_t *add_node_value (index_t *idx, idxnode_t *idxnode, char *key);
idxnode_t *find_node(index_t *idx, idxnode_t *idxnode, char *find_key);
int num_child_records(idxnode_t *idxnode);
idxnode_t *find_node(index_t *idx, idxnode_t *idxnode, char *find_rec);
char *find_record(index_t *idx, idxnode_t *idxnode, char *find_rec);
int find_node_index(index_t *idx, idxnode_t *idxnode, char *find_rec, int *index);

/* new data dictionary functions */

db_idxnode_t *dbidx_init_root_node(db_index_schema_t *);
db_idxnode_t *dbidx_allocate_node(db_index_schema_t *);
db_indexkey_t *dbidx_allocate_key(db_index_schema_t *); /* allocates just the key, data must be maintained separately */
void dbidx_reset_key(db_index_schema_t *, db_indexkey_t *);
char *dbidx_allocate_key_data(db_index_schema_t *); /* allocates just the data to be attached to the key */
/* the allocates both key and data, attaches it to the key, but copies will not account for the data payload
 * it is useful only for comparisons */
db_indexkey_t *dbidx_allocate_key_with_data(db_index_schema_t *);
bool dbidx_copy_key(db_indexkey_t *, db_indexkey_t *);

void dbidx_release_tree(db_index_t *, db_idxnode_t *);

bool dbidx_set_key_data_field_value(db_index_schema_t *, char *, char *, char *);
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
