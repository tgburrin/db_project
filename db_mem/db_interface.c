/*
 * db_interface.c
 *
 *  Created on: May 17, 2022
 *      Author: tgburrin
 */

#include "db_interface.h"

bool add_db_record(db_table_t *tbl, char *record, uint64_t *rv) {
	*rv = add_db_table_record(tbl, record);
	return *rv == UINT64_MAX ? false : true;
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

bool load_dd_index_from_table(db_table_t *tbl) {
	if ( tbl == NULL || tbl->mapped_table == NULL)
		return false;

	bool rv = true;
	printf("%" PRIu64 " slots to be examined for %" PRIu8 " indexes\n", tbl->mapped_table->total_record_count, tbl->num_indexes);
	for(uint8_t i = 0; i < tbl->num_indexes; i++)
		if ( !read_index_table_records(tbl, tbl->indexes[i]) )
			rv = false;
	return rv;
}

bool load_dd_index_from_file(db_table_t *tbl) {
	if ( tbl == NULL || tbl->mapped_table == NULL)
		return false;

	bool rv = true;
	printf("%" PRIu64 " slots to be examined for %" PRIu8 " indexes\n", tbl->mapped_table->total_record_count, tbl->num_indexes);
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

bool read_db_record(db_table_t *tbl, uint64_t slot, char **record) {
	*record = read_db_table_record(tbl, slot);
	return *record == NULL ? false : true;
}

bool delete_db_record(db_table_t *tbl, uint64_t slot, char **deleted_record) {
	if ( tbl == NULL )
		return false;

	bool rv = delete_db_table_record(tbl, slot, *deleted_record);
	return rv;
}

void set_key_from_record_slot(db_table_t *tbl, uint64_t slot, db_index_schema_t *idx, db_indexkey_t *key) {
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
	db_indexkey_t *key = NULL, *findkey = dbidx_allocate_key(idx->idx_schema);
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

	uint64_t recordcount = 0, record_number = 0, counter = 0, nodecount = 0;
	db_idxnode_t *parent = NULL, *current = NULL, *start = dbidx_allocate_node(idx->idx_schema);
	if ( (fd = open(idxfile, O_RDONLY, 0640)) >= 0 ) {

		if ( read(fd, &recordcount, sizeof(uint64_t)) != sizeof(uint64_t) ) {
			printf("Incomplete read of recordcount\n");
		}
		printf("%" PRIu64 " records of size %ld exist in index file\n", recordcount, sizeof(uint64_t));

		current = start;
		current->is_leaf = true;
		current->prev = NULL;

		//printf("Building leaf nodes (layer 0)\n");
		for(counter = 0; counter < recordcount; counter++) {
			if ( read(fd, &record_number, sizeof(uint64_t)) != sizeof(uint64_t) ) {
				fprintf(stderr, "ERROR Missed read of %ld bytes\n", sizeof(uint64_t));
				exit(EXIT_FAILURE);  // we have allocated memory that would be difficult to release
			} else {
				//printf("Record number %" PRIu64 "\n", record_number);
				//dbidx_reset_key(idx->idx_schema, key);
				record = read_db_table_record(tbl->mapped_table, record_number);
				key = dbidx_allocate_key(idx->idx_schema);
				set_key_from_record_data(tbl->schema, idx->idx_schema, record, key);
				key->record = record_number;
				current->children[current->num_children] = key;
				current->num_children++;

				if ( current->num_children >= idx->idx_schema->index_order - 1) {
					current->next = dbidx_allocate_node(idx->idx_schema);
					current->next->prev = current;
					current = current->next;
					current->is_leaf = true;
					nodecount++;
				}

				//if ( (counter+1) % 1000000 == 0 )
				//	printf("Read %" PRIu64 " records so far (%" PRIu64 " nodes)\n", counter + 1, nodecount + 1);
				//dbidx_key_print(idx->idx_schema, key);
				//add_index_value(idx, &idx->root_node, buff);
			}
		}

		current->next = NULL;

	} else {
		fprintf(stderr, "Error: %s\n", strerror(errno));
		return false;
	}
	close(fd);

	printf("%" PRIu64 " keys read with %" PRIu64 " nodes created\n", counter, nodecount + 1);

	i = 0;
	if ( nodecount > 0 ) {
		do {
			counter = 0;
			parent =  dbidx_allocate_node(idx->idx_schema);
			parent->parent = parent;
			parent->prev = NULL;
			current = start;
			start = parent;

			i++;
			//printf("Building layer %d of nodes\n", i);
			nodecount = 0;
			while ( current != NULL ) {
				if ( parent->num_children >= idx->idx_schema->index_order - 1) {
					parent->next = dbidx_allocate_node(idx->idx_schema);
					parent->next->prev = parent;
					parent = parent->next;
					nodecount++;
				}
				key = dbidx_allocate_key(idx->idx_schema);
				dbidx_copy_key(current->children[current->num_children - 1], key);
				key->childnode = current;
				parent->children[parent->num_children] = key;
				parent->num_children++;
				current->parent = parent;
				current = current->next;

				counter++;
				//if ( (counter) % 1000000 == 0 )
				//	printf("Read %" PRIu64 " nodes so far (%" PRIu64 " parent nodes)\n", counter, nodecount + 1);
			}
			parent->next = NULL;
			printf("Read %" PRIu64 " nodes (%" PRIu64 " parent nodes) in layer %d\n", counter, nodecount + 1, i);
		} while ( nodecount > 0 );
		//printf("Finished building layers\n");

		if ( idx->root_node != NULL )
			free(idx->root_node);
		idx->root_node = parent;
	} else if ( current == start ) {
		if ( idx->root_node != NULL )
			free(idx->root_node);
		start->parent = start;
		idx->root_node = start;
	} else {
		fprintf(stderr, "Error: there are 0 nodes, but the current & start do not match\n");
		exit(EXIT_FAILURE); // allocated memory would need to be released
	}
	free(findkey);
	free(idxfile);

	recordcount = 0;
	dbidx_print_tree_totals(idx, NULL, &recordcount);
	return true;
}

bool read_index_table_records(db_table_t *tbl, db_index_t *idx) {
	if ( tbl == NULL || tbl->mapped_table == NULL)
		return false;

	uint64_t recordcount = 0;
	printf("%" PRIu64 " slots to be examined\n", tbl->mapped_table->total_record_count);
	for(uint8_t i = 0; i < tbl->num_indexes; i++) {
		db_indexkey_t *key = dbidx_allocate_key(tbl->indexes[i]->idx_schema);
		for(uint64_t slot = 0; slot < tbl->mapped_table->total_record_count; slot++) {
			if ( tbl->mapped_table->used_slots[slot] == UINT64_MAX )
				continue;
			dbidx_reset_key(tbl->indexes[i]->idx_schema, key);
			set_key_from_record_slot(tbl, slot, tbl->indexes[i]->idx_schema, key);
			dbidx_add_index_value(tbl->indexes[i], key);
			recordcount++;
		}
		free(key);
		printf("%" PRIu64 " records added to %s\n", recordcount, tbl->indexes[i]->index_name);
	}

	recordcount = 0;
	dbidx_print_tree_totals(idx, NULL, &recordcount);
	return true;
}

