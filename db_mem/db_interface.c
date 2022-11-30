/*
 * db_interface.c
 *
 *  Created on: May 17, 2022
 *      Author: tgburrin
 */

#include "db_interface.h"

void read_index_from_record_numbers(table_t *tbl, index_t *idx) {
	char *ipth;
	//char msg[256];
	if ( (ipth = getenv("TABLE_DATA")) == NULL )
		ipth = DEFAULT_BASE;

	int fd = -1;
	size_t sz = strlen(ipth) + 1 + strlen(idx->index_name) + 9;

	// path + '/' + name + '.idx_num' + \0
	char *idxfile = malloc(sz);
	bzero(idxfile, sz);

	strcat(idxfile, ipth);
	strcat(idxfile, "/");
	strcat(idxfile, idx->index_name);
	strcat(idxfile, ".idx_num");

	printf("Reading keys from file %s\n", idxfile);
	int i = access(idxfile, F_OK);
	if ( i < 0 && errno != ENOENT ) {
		fprintf(stderr, "Error: %s\n", strerror(errno));
	}
	if ( (fd = open(idxfile, O_RDONLY, 0640)) >= 0 ) {
		uint16_t recordsize = 0;
		uint64_t recordnum = 0;
		uint64_t recordcount = 0;
		uint64_t counter = 0;

		if ( read(fd, &recordsize, sizeof(uint16_t)) != sizeof(uint16_t) ) {
			printf("Incomplete read of recordsize\n");
		}
		if ( read(fd, &recordcount, sizeof(uint64_t)) != sizeof(uint64_t) ) {
			printf("Incomplete read of recordcount\n");
		}
		printf("%" PRIu64 " records of size %"PRIu16" exist in index file\n", recordcount, recordsize);
		for(counter = 0; counter < recordcount; counter++) {
			recordnum = 0;
			if ( read(fd, &recordnum, recordsize) != recordsize ) {
				// an index needs to be remapped in some way
			} else {
				void *rec = (*tbl->read_record)(tbl, recordnum);
				void *idx_key = (*idx->create_record_key)(rec);
				(*idx->set_key_value)(idx_key, recordnum);
				add_index_value(idx, &idx->root_node, idx_key);
				/*
				bzero(msg, sizeof(msg));
				(*idx->print_key)(idx_key, msg);
				printf("Adding %s\n", msg);
				*/
				free(idx_key);
			}
		}
	} else
		fprintf(stderr, "Error: %s\n", strerror(errno));

	free(idxfile);
}

void write_record_numbers_from_index(index_t *idx) {
	int fd = -1;
	char *ipth;
	uint16_t recordsize = sizeof(uint64_t);

	void *buffer = malloc(sizeof(char)*recordsize);

	if ( (ipth = getenv("TABLE_DATA")) == NULL )
		ipth = DEFAULT_BASE;

	size_t sz = strlen(ipth) + 1 + strlen(idx->index_name) + 9;

	// path + '/' + name + '.idx_num' + \0
	char *idxfile = malloc(sz);
	bzero(idxfile, sz);

	strcat(idxfile, ipth);
	strcat(idxfile, "/");
	strcat(idxfile, idx->index_name);
	strcat(idxfile, ".idx_num");

	printf("Writing keys to file %s\n", idxfile);
	int i = access(idxfile, F_OK);
	if ( i < 0 && errno != ENOENT ) {
		fprintf(stderr, "Error: %s\n", strerror(errno));
	}

	if ( (fd = open(idxfile, O_CREAT | O_RDWR | O_TRUNC, 0640)) >= 0 ) {
		idxnode_t *cn = &idx->root_node;

		while(!cn->is_leaf)
			cn = ((indexkey_t *)(cn->children[0]))->childnode;

		uint64_t recordcount = 0;
		uint64_t recordnumber = 0;

		write(fd, &recordsize, sizeof(recordsize));
		write(fd, &recordcount, sizeof(recordcount));
		while ( cn != NULL ) {
			for(i = 0; i < cn->num_children; i++) {
				recordnumber = (uint64_t)((*idx->get_key_value)(cn->children[i]));
				//printf("writing record %" PRIu64 "\n", recordnumber);
				write(fd, &recordnumber, recordsize);
				recordcount++;
			}
			cn = (idxnode_t *)cn->next;
		}
		lseek(fd, sizeof(recordsize), SEEK_SET);
		write(fd, &recordcount, sizeof(recordcount));
		close(fd);

		printf("%" PRIu64 " records of size %"PRIu16" written\n", recordcount, recordsize);
	}
	free(idxfile);
	free(buffer);
}

uint64_t add_db_record(db_table_t *tbl, char *record) {
	uint64_t rv = add_db_table_record(tbl, record);
	return rv;
}

bool load_all_dd_tables(data_dictionary_t *dd) {
	for(uint32_t i; i < dd->num_tables; i++) {
		if ( !open_dd_table(&dd->tables[i]) ) {
			fprintf(stderr, "failed to load table %s\n", dd->tables[i].table_name);
			return false;
		}
	}
	return true;
}
bool close_all_dd_tables(data_dictionary_t *dd) {
	for(uint32_t i; i < dd->num_tables; i++) {
		if ( !close_dd_table(&dd->tables[i]) ) {
			fprintf(stderr, "failed to close table %s\n", dd->tables[i].table_name);
			return false;
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

void set_key_from_record(db_table_t *mtbl, db_table_t *tbl, uint64_t slot, db_index_schema_t *idx, db_indexkey_t *key) {
	if ( key == NULL )
		return;

	char *record = read_db_record(mtbl, slot);
	if ( record == NULL )
		return;

	key->record = slot;

	size_t offset;
	dd_datafield_t *idxfield = NULL;
	dd_datafield_t *tblfield = NULL;
	for(uint8_t i = 0; i < idx->fields_sz; i++) {
		idxfield = idx->fields[i];
		bool found = false;
		offset = 0;
		for(uint8_t k = 0; k < tbl->schema->fields_sz; k++) {
			tblfield = tbl->schema->fields[k];
			if ( idxfield == tblfield ) {
				found = true;
				break;
			} else
				offset += tblfield->fieldsz;
		}
		if ( found == true )
			key->data[i] = record + offset;
	}
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
