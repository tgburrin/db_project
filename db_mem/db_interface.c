/*
 * db_interface.c
 *
 *  Created on: May 17, 2022
 *      Author: tgburrin
 */

#include "db_interface.h"

bool add_db_record(db_table_t *tbl, char *record, record_num_t *rv) {
	*rv = add_db_table_record(tbl, record);
	return *rv == RECORD_NUM_MAX ? false : true;
}

bool load_all_dd_tables(data_dictionary_t *dd) {
	printf("Opening %" PRIu32 " tables from the data dictionary\n", dd->num_tables);
	for(uint32_t i = 0; i < dd->num_tables; i++) {
		if ( !open_dd_table(&dd->tables[i]) ) {
			fprintf(stderr, "failed to load table %s\n", dd->tables[i].table_name);
			return false;
		} else
			printf("Opened table %s\n", dd->tables[i].table_name);
	}
	return true;
}
bool close_all_dd_tables(data_dictionary_t *dd) {
	printf("Closing %" PRIu32 " tables from the data dictionary\n", dd->num_tables);
	for(uint32_t i = 0; i < dd->num_tables; i++) {
		if ( !close_dd_table(&dd->tables[i]) ) {
			fprintf(stderr, "failed to close table %s\n", dd->tables[i].table_name);
			return false;
		}
	}
	return true;
}

bool load_all_dd_disk_tables(data_dictionary_t *dd) {
	printf("Opening %" PRIu32 " tables from the data dictionary\n", dd->num_tables);
	for(uint32_t i = 0; i < dd->num_tables; i++) {
		if ( !open_dd_disk_table(&dd->tables[i]) ) {
			fprintf(stderr, "failed to load table %s\n", dd->tables[i].table_name);
			return false;
		} else
			printf("Opened table %s\n", dd->tables[i].table_name);
	}
	return true;
}
bool close_all_dd_disk_tables(data_dictionary_t *dd) {
	printf("Closing %" PRIu32 " tables from the data dictionary\n", dd->num_tables);
	for(uint32_t i = 0; i < dd->num_tables; i++) {
		if ( !close_dd_disk_table(&dd->tables[i]) ) {
			fprintf(stderr, "failed to close table %s\n", dd->tables[i].table_name);
			return false;
		}
	}
	return true;
}

bool load_dd_index_from_table(db_table_t *tbl) {
	if ( tbl == NULL || tbl->mapped_table == NULL)
		return false;

	bool rv = true;
	printf("%" PRIu64 " slots to be examined for %" PRIu8 " indexes\n", (uint64_t)tbl->mapped_table->total_record_count, tbl->num_indexes);
	for(uint8_t i = 0; i < tbl->num_indexes; i++)
		if ( !read_index_table_records(tbl, tbl->indexes[i]) )
			rv = false;
	return rv;
}

bool load_dd_index_from_file(db_table_t *tbl) {
	if ( tbl == NULL || tbl->mapped_table == NULL)
		return false;

	bool rv = true;
	printf("%" PRIu64 " slots to be examined for %" PRIu8 " indexes\n", (uint64_t)tbl->mapped_table->total_record_count, tbl->num_indexes);
	for(uint8_t i = 0; i < tbl->num_indexes; i++)
		if ( !read_index_file_records(tbl, tbl->indexes[i]) )
			rv = false;
	return rv;
}

bool load_dd_indexes(db_table_t *tbl) {
	if ( tbl == NULL || tbl->mapped_table == NULL)
		return false;

	char *ipth;
	if ( (ipth = getenv("TABLE_DATA")) == NULL )
		ipth = DEFAULT_BASE;

	int fdok = 0;
	size_t sz = 0;
	char *idxfile = NULL;
	for(uint8_t i = 0; i < tbl->num_indexes; i++) {
		db_index_t *idx = tbl->indexes[i];

		if ( strcmp(idx->index_name, "client_id_id_idx_uq") == 0 ) {
			printf("Using special handling for index\n");
			uint64_t nc = 0;
			idx->nodeset = dbidx_allocate_node_block(idx->idx_schema, tbl->total_record_count, &nc);
			idx->total_node_count = nc;
			idx->free_node_slot = nc - 1;
			idx->used_slots = calloc(sizeof(record_num_t), nc);
			idx->free_slots = calloc(sizeof(record_num_t), nc);
			for(record_num_t i = 0; i < nc; i++) {
				idx->free_slots[i] = i;
				idx->used_slots[i] = RECORD_NUM_MAX;
			}
		}

		sz = strlen(ipth) + 1 + strlen(idx->index_name) + 5;
		idxfile = malloc(sz);
		bzero(idxfile, sz);
		strcat(idxfile, ipth);
		strcat(idxfile, "/");
		strcat(idxfile, idx->index_name);
		strcat(idxfile, ".idx");
		fdok = access(idxfile, R_OK | W_OK);

		if ( fdok == 0 )
			read_index_file_records(tbl, idx);
		else {
			printf("File %s does not exist, building index from table records\n", idxfile);
			read_index_table_records(tbl, idx);
		}
		free(idxfile);
	}
	return true;
}

bool read_db_record(db_table_t *tbl, record_num_t slot, char **record) {
	*record = read_db_table_record(tbl, slot);
	return *record == NULL ? false : true;
}

bool delete_db_record(db_table_t *tbl, record_num_t slot, char **deleted_record) {
	if ( tbl == NULL )
		return false;

	bool rv = delete_db_table_record(tbl, slot, *deleted_record);
	return rv;
}

void set_key_from_record_slot(db_table_t *tbl, record_num_t slot, db_index_schema_t *idx, db_indexkey_t *key) {
	if ( key == NULL )
		return;

	char *record = NULL;
	read_db_record(tbl->mapped_table, slot, &record);
	if ( record == NULL )
		return;

	key->record = slot;

	size_t offset;
	dd_datafield_t *idxfield = NULL;
	dd_datafield_t *tblfield = NULL;
	for(uint8_t i = 0; i < idx->num_fields; i++) {
		idxfield = idx->fields[i];
		bool found = false;
		offset = 0;
		for(uint8_t k = 0; k < tbl->schema->num_fields; k++) {
			tblfield = tbl->schema->fields[k];
			if ( idxfield == tblfield ) {
				found = true;
				break;
			} else
				offset += tblfield->field_sz;
		}
		if ( found == true )
			key->data[i] = record + offset;
	}
}

void set_key_from_record_data(dd_table_schema_t *tbl, db_index_schema_t *idx, char *record, db_indexkey_t *key) {
	size_t offset;
	dd_datafield_t *idxfield = NULL;
	dd_datafield_t *tblfield = NULL;

	for(uint8_t i = 0; i < idx->num_fields; i++) {
		idxfield = idx->fields[i];
		bool found = false;
		offset = 0;
		for(uint8_t k = 0; k < tbl->num_fields; k++) {
			tblfield = tbl->fields[k];
			if ( idxfield == tblfield ) {
				found = true;
				break;
			} else
				offset += tblfield->field_sz;
		}
		if ( found == true )
			key->data[i] = record + offset;
	}
}

db_indexkey_t *create_key_from_record_data(dd_table_schema_t *tbl, db_index_schema_t *idx, char *record) {
	db_indexkey_t *key = dbidx_allocate_key(idx);

	size_t offset;
	dd_datafield_t *idxfield = NULL;
	dd_datafield_t *tblfield = NULL;
	for(uint8_t i = 0; i < idx->num_fields; i++) {
		idxfield = idx->fields[i];
		bool found = false;

		offset = 0;
		for(uint8_t k = 0; !found && k < tbl->num_fields; k++) {
			tblfield = tbl->fields[k];
			if ( idxfield == tblfield ) {
				found = true;
				key->data[i] = (record + offset);
			} else
				offset += tblfield->field_sz;
		}
	}
	return key;
}

/*
char * find_db_record(db_table_t *tbl, char *record, char *index_name) {
	char *rv = NULL;
	tbl = NULL;
	record = NULL;
	index_name = NULL;
	return rv;
}
*/


bool read_index_file_records(db_table_t *tbl, db_index_t *idx) {
	char *ipth;
	if ( (ipth = getenv("TABLE_DATA")) == NULL )
		ipth = DEFAULT_BASE;

	int fd = -1;
	size_t sz = strlen(ipth) + 1 + strlen(idx->index_name) + 5;

	// path + '/' + name + '.idx' + \0
	char *record, *idxfile = malloc(sz);
	db_indexkey_t *findkey = dbidx_allocate_key(idx->idx_schema);
	bzero(idxfile, sz);

	strcat(idxfile, ipth);
	strcat(idxfile, "/");
	strcat(idxfile, idx->index_name);
	strcat(idxfile, ".idx");

	printf("Reading keys from file %s\n", idxfile);
	int i = access(idxfile, F_OK);
	if ( i < 0 && errno != ENOENT ) {
		fprintf(stderr, "Error: %s\n", strerror(errno));
		return false;
	}

	uint64_t recordcount = 0, counter = 0, nodecount = 0;
	record_num_t record_number = 0;
	db_idxnode_t *parent = NULL, *current = NULL, *start = NULL;
	if ( idx->nodeset != NULL )
		start = dbidx_reserve_node(idx);
	else
		start = dbidx_allocate_node(idx->idx_schema);

	if ( (fd = open(idxfile, O_RDONLY, 0640)) >= 0 ) {

		if ( read(fd, &recordcount, sizeof(uint64_t)) != sizeof(uint64_t) )
			fprintf(stderr, "Incomplete read of recordcount\n");

		printf("%" PRIu64 " records of size %ld exist in index file\n", recordcount, sizeof(uint64_t));

		current = start;
		current->is_leaf = true;
		current->prev = NULL;

		for(counter = 0; counter < recordcount; counter++) {
			if ( read(fd, &record_number, sizeof(record_num_t)) != sizeof(record_num_t) ) {
				fprintf(stderr, "ERROR Missed read of %ld bytes\n", sizeof(record_num_t));
				exit(EXIT_FAILURE);  // we have allocated memory that would be difficult to release
			} else {
				if ( current->num_children >= idx->idx_schema->index_order - 1) {
					if ( idx->nodeset != NULL )
						current->next = dbidx_reserve_node(idx);
					else
						current->next = dbidx_allocate_node(idx->idx_schema);
					current->next->prev = current;
					current = current->next;
					current->is_leaf = true;
					nodecount++;
				}

				record = read_db_table_record(tbl->mapped_table, record_number);
				dbidx_reset_key(idx->idx_schema, findkey);
				set_key_from_record_data(tbl->schema, idx->idx_schema, record, findkey);

				dbidx_copy_key(idx->idx_schema, findkey, current->children[current->num_children]);
				current->children[current->num_children]->childnode = current;
				current->children[current->num_children]->record = record_number;

				current->num_children++;

				//if ( (counter+1) % 1000000 == 0 )
				//	printf("Read %" PRIu64 " records so far (%" PRIu64 " nodes)\n", counter + 1, nodecount + 1);
			}
		}

		current->next = NULL;

	} else {
		fprintf(stderr, "Error: %s\n", strerror(errno));
		return false;
	}
	close(fd);

	printf("%" PRIu64 " keys read with %" PRIu64 " nodes created\n", (uint64_t)counter, (uint64_t)(nodecount + 1));

	i = 0;
	if ( nodecount > 0 ) {
		do {
			counter = 0;
			parent = NULL;
			if ( idx->nodeset != NULL )
				parent = dbidx_reserve_node(idx);
			else
				parent = dbidx_allocate_node(idx->idx_schema);

			parent->parent = parent;
			parent->prev = NULL;
			current = start;
			start = parent;

			nodecount = 0;
			while ( current != NULL ) {
				if ( parent->num_children >= idx->idx_schema->index_order - 1) {
					if ( idx->nodeset != NULL )
						parent->next = dbidx_reserve_node(idx);
					else
						parent->next = dbidx_allocate_node(idx->idx_schema);

					parent->next->prev = parent;
					parent = parent->next;
					nodecount++;
				}

				dbidx_copy_key(idx->idx_schema, current->children[current->num_children - 1], parent->children[parent->num_children]);
				parent->children[parent->num_children]->childnode = current;

				parent->num_children++;
				current->parent = parent;
				current = current->next;

				counter++;
				//if ( (counter) % 1000000 == 0 )
				//	printf("Read %" PRIu64 " nodes so far (%" PRIu64 " parent nodes)\n", counter, nodecount + 1);
			}
			parent->next = NULL;
			printf("Read %" PRIu64 " nodes (%" PRIu64 " parent nodes) in layer %d\n", (uint64_t)counter, (uint64_t)(nodecount + 1), i++);
		} while ( nodecount > 0 );
		//printf("Finished building layers\n");
		printf("Read %" PRIu64 " nodes (%" PRIu64 " parent nodes) in layer %d\n", (uint64_t)(nodecount + 1), (uint64_t)0, i);

		if ( idx->root_node != NULL ) {
			if ( idx->nodeset != NULL ) {
				dbidx_release_node(idx, idx->root_node);
			} else {
				free(idx->root_node);
			}
		}
		idx->root_node = parent;
	} else if ( current == start ) {
		if ( idx->root_node != NULL ) {
			if ( idx->nodeset != NULL ) {
				dbidx_release_node(idx, idx->root_node);
			} else {
				free(idx->root_node);
			}
		}
		start->parent = start;
		idx->root_node = start;
	} else {
		fprintf(stderr, "Error: there are 0 nodes, but the current & start do not match\n");
		exit(EXIT_FAILURE); // allocated memory would need to be released
	}
	free(findkey);
	free(idxfile);

	uint64_t cnt = 0;
	dbidx_print_tree_totals(idx, NULL, &cnt);
	return true;
}

bool read_index_table_records(db_table_t *tbl, db_index_t *idx) {
	if ( tbl == NULL || tbl->mapped_table == NULL)
		return false;

	record_num_t recordcount = 0;
	printf("%" PRIu64 " slots to be examined\n", (uint64_t)tbl->mapped_table->total_record_count);
	for(uint8_t i = 0; i < tbl->num_indexes; i++) {
		db_indexkey_t *key = dbidx_allocate_key(tbl->indexes[i]->idx_schema);
		for(record_num_t slot = 0; slot < tbl->mapped_table->total_record_count; slot++) {
			if ( tbl->mapped_table->used_slots[slot] == RECORD_NUM_MAX )
				continue;
			dbidx_reset_key(tbl->indexes[i]->idx_schema, key);
			set_key_from_record_slot(tbl, slot, tbl->indexes[i]->idx_schema, key);
			dbidx_add_index_value(tbl->indexes[i], key);
			recordcount++;
		}
		free(key);
		printf("%" PRIu64 " records added to %s\n", (uint64_t)recordcount, tbl->indexes[i]->index_name);
	}

	uint64_t cnt = 0;
	dbidx_print_tree_totals(idx, NULL, &cnt);
	return true;
}

