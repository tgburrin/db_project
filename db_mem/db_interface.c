/*
 * db_interface.c
 *
 *  Created on: May 17, 2022
 *      Author: tgburrin
 */

#include "db_interface.h"

uint64_t add_db_record(db_table_t *tbl, char *record) {
	uint64_t rv = add_db_table_record(tbl, record);
	return rv;
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
	if ( tbl != NULL && tbl->mapped_table != NULL) {
		printf("%" PRIu64 " slots to be examined\n", tbl->mapped_table->total_record_count);
		for(uint8_t i = 0; i < tbl->num_indexes; i++) {
			uint64_t record_count = 0;
			db_indexkey_t *key = dbidx_allocate_key(tbl->indexes[i]->idx_schema);
			for(uint64_t slot = 0; slot < tbl->mapped_table->total_record_count; slot++) {
				if ( tbl->mapped_table->used_slots[slot] == UINT64_MAX )
					continue;
				dbidx_reset_key(tbl->indexes[i]->idx_schema, key);
				set_key_from_record_slot(tbl, slot, tbl->indexes[i]->idx_schema, key);
				dbidx_add_index_value(tbl->indexes[i], NULL, key);
				record_count++;
			}
			free(key);
			printf("%" PRIu64 " records added to %s\n", record_count, tbl->indexes[i]->index_name);
		}
	}
	return true;
}

char * read_db_record(db_table_t *tbl, uint64_t slot) {
	char *record = read_db_table_record(tbl, slot);
	return record;
}

bool delete_db_record(db_table_t *tbl, char *record, char *deleted_record) {
	if ( tbl == NULL )
		return false;
	if ( record == NULL )
		return false;

	bool rv = delete_db_table_record(tbl, 0, deleted_record);
	return rv;
}

void set_key_from_record_slot(db_table_t *tbl, uint64_t slot, db_index_schema_t *idx, db_indexkey_t *key) {
	if ( key == NULL )
		return;

	char *record = read_db_record(tbl->mapped_table, slot);
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
