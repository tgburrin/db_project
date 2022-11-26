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

//#define IDX_ORDER 64
//#define IDX_ORDER 21
//#define IDX_ORDER 13
//#define IDX_ORDER 9
#define IDX_ORDER 5
//#define IDX_ORDER 3

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

void init_index_node(idxnode_t *idxnode);
int num_child_records(idxnode_t *idxnode);
idxnode_t *find_node(index_t *idx, idxnode_t *idxnode, char *find_rec);
char *find_record(index_t *idx, idxnode_t *idxnode, char *find_rec);
int find_node_index(index_t *idx, idxnode_t *idxnode, char *find_rec, int *index);

idxnode_t *split_node(index_t *idx, idxnode_t *idxnode, char *key);
idxnode_t *add_node_value (index_t *idx, idxnode_t *idxnode, char *key);
idxnode_t *find_node(index_t *idx, idxnode_t *idxnode, char *find_key);
bool add_index_value (index_t *idx, idxnode_t *idxnode, char *key);
void update_max_value (index_t *idx, idxnode_t *parent_idx, idxnode_t *idxnode, char *new_key);

void collapse_nodes(idxnode_t *idxnode);
bool remove_index_value (index_t *idx, idxnode_t *idxnode, char *key);
bool remove_node_value(index_t *idx, idxnode_t *idxnode, char *key);
void release_tree(index_t *idx, idxnode_t *idxnode);

void read_index_from_file(index_t *idx);
void write_file_from_index(index_t *idx);

void print_tree(index_t *idx, idxnode_t *idxnode, int *counter);
void print_tree_totals(index_t *idx, idxnode_t *idxnode, int *counter);
void print_index_scan_lookup(index_t *idx, char *key);

#endif /* INDEX_TOOLS_H_ */
