/*
 * data_dictionary.h
 *
 *  Created on: May 19, 2022
 *      Author: tgburrin
 */

#ifndef DATA_DICTIONARY_H_
#define DATA_DICTIONARY_H_

#define _GNU_SOURCE 1

#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <inttypes.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>

#include <cjson/cJSON.h>
#include "utils.h"

typedef uint8_t index_order_t;
#define INDEX_ORDER_MAX UINT8_MAX
typedef uint32_t record_num_t;
#define RECORD_NUM_MAX UINT32_MAX

/* empty typedef definitions for early use, these will be redefined later */
typedef struct DDDataField dd_data_field_t;
typedef struct DDTableSchema dd_table_schema_t;
typedef struct DDIndexSchema db_index_schema_t;
typedef struct DbTable db_table_t;
typedef struct DbIndex db_index_t;
typedef struct DbBtNode db_btnode_t;
typedef struct DbIndexNode db_idxnode_t;
typedef struct DbIndexKey db_indexkey_t;

typedef enum {
	DD_TYPE_STR,
	DD_TYPE_TIMESTAMP,
	DD_TYPE_BOOL,

	DD_TYPE_I8,
	DD_TYPE_UI8,
	DD_TYPE_I16,
	DD_TYPE_UI16,
	DD_TYPE_I32,
	DD_TYPE_UI32,
	DD_TYPE_I64,
	DD_TYPE_UI64,

	DD_TYPE_UUID,
	DD_TYPE_BYTES
} datatype_t;

// this and the item above need to be kept in sync
extern const char * datatype_names[];

typedef enum {
	DD_TABLE_TYPE_BOTH,
	DD_TABLE_TYPE_DISK,
	DD_TABLE_TYPE_SHM
} tabletype_t;

typedef enum {
	DD_IDX_TYPE_BPTIDX,  // binary plus tree idx
	DD_IDX_TYPE_BNTIDX   // binary node tree idx
} indextype_t;

/* description structures */
typedef struct DDDataField {
	char field_name[DB_OBJECT_NAME_SZ];
	datatype_t fieldtype;
	uint8_t field_sz;  /* in bytes */
} dd_datafield_t;

typedef struct DDTableSchema {
	char schema_name[DB_OBJECT_NAME_SZ];
	uint8_t field_count;
	uint16_t record_size;
	uint8_t num_fields;  /* this is the size of the array below */
	dd_datafield_t **fields;
} dd_table_schema_t;

typedef struct DDIndexSchema {
	index_order_t index_order; /* must be the same as num_children below in index nodes */
	uint8_t num_fields;        /* this is both the number of fields in the index and the size of the array below */
	uint16_t node_size;        /* node + children_ptr * index order */
	uint16_t key_size;         /* index key + data_ptr * num fields in the index */
	uint16_t nodekey_size;     /* this is the node + key * index order */
	uint16_t record_size;      /* this is the cumulative size, in bytes, of the fields e.g. str(20) + uint64_t = 28 */
	bool is_unique;
	dd_datafield_t **fields;
} db_index_schema_t;

/* functional structures that hold state */
typedef struct DbTable {
	char table_name[DB_OBJECT_NAME_SZ];
	uint16_t header_size;
	struct timespec closedtm;
	record_num_t total_record_count;
	record_num_t free_record_slot;

	db_table_t *mapped_table;
	int filedes;
	tabletype_t table_type;
	size_t filesize;

	pthread_mutex_t read_lock;
	uint32_t reader_count;
	pthread_mutex_t write_lock;
	uint32_t writer_session;

	dd_table_schema_t *schema;

	uint8_t num_indexes;
	db_index_t **indexes;

	record_num_t *used_slots;
	record_num_t *free_slots;
	char *data;
} db_table_t;

typedef struct DbBtNode {
	db_btnode_t *right;
	db_btnode_t *left;
	record_num_t record;
	char **data;
} db_btnode_t;

typedef struct DbIndexNode {
	bool is_leaf;
	index_order_t num_children;
	db_idxnode_t *parent;
	db_idxnode_t *next;
	db_idxnode_t *prev;
	db_indexkey_t **children;
} db_idxnode_t;

typedef struct DbIndexKey { /* an index key may either be a terminal leaf or a jump to another node */
	db_idxnode_t *childnode;
	record_num_t record;
	char **data;
} db_indexkey_t;

typedef struct DbIndexPosition {
	db_idxnode_t *node;
	index_order_t nodeidx;
} db_index_position_t;

typedef struct DbIndex {
	char index_name[DB_OBJECT_NAME_SZ];
	indextype_t index_type;
	db_idxnode_t *root_node;

	db_index_schema_t *idx_schema;
	db_table_t *table;

	record_num_t total_node_count;
	record_num_t free_node_slot;
	record_num_t *used_slots;
	record_num_t *free_slots;

	char *nodeset;
} db_index_t;

/* container that holds all types */
typedef struct DataDictionary {
	uint32_t num_alloc_fields;
	uint32_t num_alloc_schemas;
	uint32_t num_alloc_tables;

	uint32_t num_fields;
	uint32_t num_schemas;
	uint32_t num_tables;
	dd_datafield_t *fields;
	dd_table_schema_t *schemas;
	db_table_t *tables;
} data_dictionary_t;


const char *map_enum_to_name(datatype_t);

// comparison functions
signed char str_compare (const char *, const char *);
signed char str_compare_sz (const char *, const char *, size_t);
signed char i64_compare (int64_t *, int64_t *);
signed char ui64_compare (uint64_t *, uint64_t *);
signed char i32_compare (int32_t *, int32_t *);
signed char ui32_compare (uint32_t *, uint32_t *);
signed char i16_compare (int16_t *, int16_t *);
signed char ui16_compare (uint16_t *, uint16_t *);
signed char i8_compare (int8_t *, int8_t *);
signed char ui8_compare (uint8_t *, uint8_t *);
signed char bytes_compare(const unsigned char *, const unsigned char *, size_t);
signed char ts_compare (struct timespec *, struct timespec *);

/* Generic data dictionary - data_dictionary.c */
data_dictionary_t **init_data_dictionary(uint32_t, uint32_t, uint32_t);

data_dictionary_t **build_dd_from_json(char *);
char *read_dd_json_file(char *);

data_dictionary_t **build_dd_from_dat(char *);
bool write_data_dictionary_dat(data_dictionary_t *, char *);

data_dictionary_t **build_dd_from_json_str(char *);

void print_data_dictionary(data_dictionary_t *);
void release_data_dictionary(data_dictionary_t **);

// end dd

dd_table_schema_t *init_dd_schema(char *, uint8_t);
db_table_t *init_db_table(char *, dd_table_schema_t *, record_num_t);
db_index_t *init_db_idx(char *, uint8_t);
void set_dd_index_order(db_index_schema_t *, index_order_t);
dd_datafield_t *init_dd_field_type(char *, datatype_t, uint8_t);
dd_datafield_t *init_dd_field_str(char *, char *, uint8_t);

void idx_key_to_str(db_index_schema_t *, db_indexkey_t *, char *);
bool dd_type_to_str(dd_datafield_t *, char *, char *);
bool dd_type_to_allocstr(dd_datafield_t *, char *, char **);
size_t dd_type_strlen(dd_datafield_t *); // returns the length of the string plus the terminating null;
bool str_to_dd_type(dd_datafield_t *, char *, char *);

char *make_table_path(const char *, const char *, const char *);

int add_dd_table(data_dictionary_t **, db_table_t *, db_table_t **);
int add_dd_schema(data_dictionary_t **, dd_table_schema_t *, dd_table_schema_t **);
int add_dd_field(data_dictionary_t **, dd_datafield_t *);

uint8_t get_dd_field_size(datatype_t, uint8_t);
int add_dd_table_schema_field(dd_table_schema_t *, dd_datafield_t *);

db_table_t *find_db_table(data_dictionary_t **, const char *);
db_index_t *find_db_index(db_table_t *, const char *);

dd_table_schema_t *find_dd_schema(data_dictionary_t **, const char *);
db_index_schema_t *find_dd_idx_schema(db_table_t *, const char *);
dd_datafield_t *find_dd_field(data_dictionary_t **, const char *);


/* Table Functions - table_tools.c */
bool open_dd_table(db_table_t *tablemeta);
bool open_dd_disk_table(db_table_t *tablemeta);
bool open_dd_shm_table(db_table_t *tablemeta);

bool close_dd_table(db_table_t *tablemeta);
bool close_dd_disk_table(db_table_t *tablemeta);
bool close_dd_shm_table(db_table_t *tablemeta);

record_num_t add_db_table_record(db_table_t *, char *);
bool delete_db_table_record(db_table_t *, record_num_t, char *);
char * read_db_table_record(db_table_t *, record_num_t);
char * new_db_table_record(dd_table_schema_t *);
void reset_db_table_record(dd_table_schema_t *, char *);
void release_table_record(dd_table_schema_t *, char *);
dd_datafield_t *get_db_table_field(dd_table_schema_t *, char *);
bool set_db_table_record_field(dd_table_schema_t *, char *, char *, char *);
bool set_db_table_record_field_str(dd_table_schema_t *, char *, char *, char *);
bool set_db_table_record_field_num(dd_table_schema_t *, uint8_t, char *, char *);
bool get_db_table_record_field_num(dd_table_schema_t *, uint8_t, char *, char *);

/* functions for reading a specific field out of a table record */
bool get_db_table_record_field_value_str_alloc(dd_table_schema_t *, char *, char *, char **);
bool get_db_table_record_field_value_str(dd_table_schema_t *, char *, char *, char *);

void db_table_record_print(dd_table_schema_t *, char *);
char *db_table_record_print_alloc(dd_table_schema_t *, char *);
char *db_table_record_print_line_alloc(dd_table_schema_t *, char *);
void db_table_record_str(dd_table_schema_t *, char *, char *, size_t);

/* Index Functions - index_tools.c */
db_idxnode_t *dbidx_init_root_node(db_index_t *);
db_idxnode_t *dbidx_allocate_node(db_index_schema_t *);
db_idxnode_t *dbidx_reserve_node(db_index_t *);
bool dbidx_release_node(db_index_t *, db_idxnode_t *);
char *dbidx_allocate_node_block(db_index_schema_t *, record_num_t, uint64_t *);
db_indexkey_t *dbidx_allocate_key(db_index_schema_t *);
char *dbidx_allocate_key_data(db_index_schema_t *); /* allocates just the data to be attached to the key */
void dbidx_reset_key(db_index_schema_t *, db_indexkey_t *);
/* the allocates both key and data, attaches it to the key, but copies will not account for the data payload
 * it is useful only for comparisons */
db_indexkey_t *dbidx_allocate_key_with_data(db_index_schema_t *);
void dbidx_reset_key_with_data(db_index_schema_t *, db_indexkey_t *);
bool dbidx_copy_key(db_index_schema_t *, db_indexkey_t *, db_indexkey_t *);

void dbidx_release_tree(db_index_t *, db_idxnode_t *);

bool dbidx_set_key_data_field_value(db_index_schema_t *, char *, char *, char *);
bool dbidx_set_key_field_value(db_index_schema_t *, char *, db_indexkey_t *, char *);
signed char dbidx_compare_keys(db_index_schema_t *, db_indexkey_t *, db_indexkey_t *);

/* Generic Index Functions */
bool dbidx_add_index_value (db_index_t *, db_indexkey_t *);
bool dbidx_remove_index_value (db_index_t *, db_indexkey_t *);
db_indexkey_t *dbidx_find_record(db_index_t *, db_indexkey_t *);
db_indexkey_t *dbidx_find_first_record(db_index_t *, db_indexkey_t *, db_index_position_t *);
db_indexkey_t *dbidx_find_last_record(db_index_t *, db_indexkey_t *, db_index_position_t *);
db_indexkey_t *dbidx_find_next_record(db_index_t *, db_indexkey_t *, db_index_position_t *);
db_indexkey_t *dbidx_find_prev_record(db_index_t *, db_indexkey_t *, db_index_position_t *);

uint64_t dbidx_num_child_records(db_idxnode_t *);
signed char dbidx_find_node_index(db_index_schema_t *, db_idxnode_t *, db_indexkey_t *, index_order_t *);
signed char dbidx_find_node_index_div(db_index_schema_t *, db_idxnode_t *, db_indexkey_t *, index_order_t *);
signed char dbidx_find_node_index_add(db_index_schema_t *, db_idxnode_t *, db_indexkey_t *, index_order_t *);
signed char dbidx_find_node_index_reverse(db_index_schema_t *, db_idxnode_t *, db_indexkey_t *, index_order_t *);
db_idxnode_t *dbidx_find_node(db_index_schema_t *, db_idxnode_t *, db_indexkey_t *);
db_idxnode_t *dbidx_find_node_reverse(db_index_schema_t *, db_idxnode_t *, db_indexkey_t *);

db_indexkey_t *dbidx_add_node_value(db_index_t *, db_idxnode_t *, db_indexkey_t *);
bool dbidx_remove_node_value(db_index_t *idx, db_idxnode_t *idxnode, db_indexkey_t *key);

db_idxnode_t *dbidx_split_node(db_index_t *, db_idxnode_t *, db_indexkey_t *);
void dbidx_collapse_nodes(db_index_t *, db_idxnode_t *);

void dbidx_update_max_value (db_index_schema_t *, db_idxnode_t *, db_idxnode_t *, db_indexkey_t *);

void dbidx_key_print(db_index_schema_t *, db_indexkey_t *);

void dbidx_print_tree(db_index_t *, db_idxnode_t *, uint64_t *);
void dbidx_print_full_tree(db_index_t *, db_idxnode_t *, uint64_t *);
void dbidx_print_tree_totals(db_index_t *, db_idxnode_t *, uint64_t *);
void dbidx_print_index_scan_lookup(db_index_t *idx, db_indexkey_t *key);

void dbidx_write_file_records(db_index_t *);
void dbidx_write_file_keys(db_index_t *);

#endif /* DATA_DICTIONARY_H_ */
