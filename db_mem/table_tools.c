/*
 * table_tools.c
 *
 *  Created on: Mar 28, 2022
 *      Author: tgburrin
 */

#include "table_tools.h"

int open_table(table_t *tablemeta, table_t **mapped_table) {
	int rv = 0, i = 0;
	int fd = -1;
	char *dbfile;
	table_t *mt;
	char *tpth, *table_file_name;

	table_file_name = malloc(sizeof(char) * (strlen(tablemeta->table_name) + 5));
	bzero(table_file_name, sizeof(char) * (strlen(tablemeta->table_name) + 5));
	strcat(table_file_name, tablemeta->table_name);
	strcat(table_file_name, ".shm");

	if ( (tpth = getenv("TABLE_DATA")) == NULL )
		tpth = DEFAULT_BASE;

	size_t data_size = tablemeta->record_size * tablemeta->total_record_count;
	size_t metadatazie = sizeof(table_t) + sizeof(uint64_t) * tablemeta->total_record_count * 2 + sizeof(void *) * tablemeta->total_record_count;

	// path + separator + tablename + .shm \0
	i = strlen(tpth) + 1 + strlen(tablemeta->table_name) + 5;
	char *diskfile = malloc(sizeof(char)*i);
	bzero(diskfile, sizeof(char)*i);

	i = strlen(DEFAULT_SHM) + 1 + strlen(tablemeta->table_name) + 5;
	char *shmfile = malloc(sizeof(char)*i);
	bzero(shmfile, sizeof(char)*i);

	strcat(diskfile, tpth);
	strcat(diskfile, "/");
	strcat(diskfile, tablemeta->table_name);
	strcat(diskfile, ".shm");

	strcat(shmfile, DEFAULT_SHM);
	strcat(shmfile, "/");
	strcat(shmfile, tablemeta->table_name);
	strcat(shmfile, ".shm");

	size_t fs = initialize_file(diskfile, metadatazie + data_size, &fd);

	if ( (dbfile = mmap(NULL, fs, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)) == MAP_FAILED ) {
		fprintf(stderr, "Unable to map file to memory\n");
		fprintf(stderr, "Error: %s\n", strerror(errno));
		return rv;

	}

	mt = (table_t *)dbfile;

	if ( mt->total_record_count == 0 ||
		(mt->total_record_count == tablemeta->total_record_count && mt->record_size == tablemeta->record_size)) {
		munmap(dbfile, fs);
		close(fd);

		copy_and_replace_file(tpth, DEFAULT_SHM, table_file_name);
		fd = open(shmfile, O_RDWR, 0640);
		dbfile = mmap(NULL, fs, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
		mt = (table_t *)dbfile;
	} else if ( mt->total_record_count != tablemeta->total_record_count ||
				mt->record_size != tablemeta->record_size) {
		// remap event
	} else {
		// unexpected failure
	}

	if ( mt->total_record_count == 0 ) {
		printf("Initializing table %s\n", tablemeta->table_name);
		strcpy(mt->table_name, tablemeta->table_name);
		mt->header_size = tablemeta->header_size;
		mt->record_size = tablemeta->record_size;
		mt->total_record_count = tablemeta->total_record_count;
		mt->free_record_slot = tablemeta->total_record_count - 1;

		char *offset = dbfile + sizeof(table_t);
		mt->used_slots = (uint64_t *) (offset);

		offset += sizeof(uint64_t) * tablemeta->total_record_count;
		mt->free_slots = (uint64_t *) (offset);

		offset += sizeof(uint64_t) * tablemeta->total_record_count;
		mt->data = offset;

		for(uint64_t i = 0; i < mt->total_record_count; i++) {
			mt->free_slots[mt->free_record_slot - i] = i;
			mt->used_slots[i] = UINT64_MAX;
		}

	} else {
		char *offset = dbfile + sizeof(table_t);
		mt->used_slots = (uint64_t *) (offset);

		offset += sizeof(uint64_t) * tablemeta->total_record_count;
		mt->free_slots = (uint64_t *) (offset);

		offset += sizeof(uint64_t) * tablemeta->total_record_count;
		mt->data = (void *) (offset);
		printf("Opened table %s\n", mt->table_name);

	}

	mt->filedes = fd;
	mt->filesize = fs;

	mt->add_record = tablemeta->add_record;
	mt->delete_record = tablemeta->delete_record;
	mt->read_record = tablemeta->read_record;

	*mapped_table = mt;
	free(diskfile);
	free(shmfile);
	free(table_file_name);
	return rv;
}

int close_table(table_t *mapped_table) {
	int rv = 0, fd = mapped_table->filedes;
	size_t fs = mapped_table->filesize;
	size_t tnsz = sizeof(char) * strlen(mapped_table->table_name) + 5;

	printf("Closing table %s\n", mapped_table->table_name);

	char *tpth = NULL;
	if ( (tpth = getenv("TABLE_DATA")) == NULL )
		tpth = DEFAULT_BASE;

	char *tn = malloc(tnsz);
	bzero(tn, tnsz);
	strcpy(tn, mapped_table->table_name);
	strcat(tn, ".shm");

	munmap(mapped_table, fs);
	close(fd);

	move_and_replace_file(DEFAULT_SHM, tpth, tn);

	free(tn);
	return rv;
}

// Data Dictionary table structs
bool open_dd_table(db_table_t *tbl) {
	int i = 0;
	int fd = -1;
	char *dbfile;
	db_table_t *mt;
	char *tpth, *table_file_name;

	table_file_name = malloc(sizeof(char) * (strlen(tbl->table_name) + 5));
	bzero(table_file_name, sizeof(char) * (strlen(tbl->table_name) + 5));
	strcat(table_file_name, tbl->table_name);
	strcat(table_file_name, ".shm");

	if ( (tpth = getenv("TABLE_DATA")) == NULL )
		tpth = DEFAULT_BASE;

	size_t data_size = tbl->schema->record_size * tbl->total_record_count;
	size_t metadatasize =
			sizeof(db_table_t) +
			sizeof(dd_table_schema_t) +
			sizeof(dd_datafield_t **) +
			sizeof(dd_datafield_t *) * tbl->schema->field_count +
			sizeof(dd_datafield_t) * tbl->schema->field_count +
			sizeof(uint64_t) * tbl->total_record_count * 2;

	// path + separator + tablename + .shm \0
	i = strlen(tpth) + 1 + strlen(tbl->table_name) + 5;
	char *diskfile = malloc(sizeof(char)*i);
	bzero(diskfile, sizeof(char)*i);

	i = strlen(DEFAULT_SHM) + 1 + strlen(tbl->table_name) + 5;
	char *shmfile = malloc(sizeof(char)*i);
	bzero(shmfile, sizeof(char)*i);

	strcat(diskfile, tpth);
	strcat(diskfile, "/");
	strcat(diskfile, tbl->table_name);
	strcat(diskfile, ".shm");

	strcat(shmfile, DEFAULT_SHM);
	strcat(shmfile, "/");
	strcat(shmfile, tbl->table_name);
	strcat(shmfile, ".shm");

	size_t fs = initialize_file(diskfile, metadatasize + data_size, &fd);

	if ( (dbfile = mmap(NULL, fs, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)) == MAP_FAILED ) {
		fprintf(stderr, "Unable to map file to memory\n");
		fprintf(stderr, "Error: %s\n", strerror(errno));
		return false;

	}

	char *offset = dbfile;
	mt = (db_table_t *)offset;
	offset += sizeof(db_table_t);
	mt->schema = (dd_table_schema_t *)offset;
	offset += sizeof(dd_table_schema_t);

	if ( mt->total_record_count == 0 ||
		(mt->total_record_count == tbl->total_record_count && mt->schema->record_size == tbl->schema->record_size)) {
		munmap(dbfile, fs);
		close(fd);

		copy_and_replace_file(tpth, DEFAULT_SHM, table_file_name);
		fd = open(shmfile, O_RDWR, 0640);
		dbfile = mmap(NULL, fs, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
		offset = dbfile;
		mt = (db_table_t *)offset;
		//printf("Reopened table %s (%p %ld)\n", tablemeta->table_name, (void *)mt, sizeof(db_table_t));
		offset += sizeof(db_table_t);

		mt->schema = (dd_table_schema_t *)offset;
		offset += sizeof(dd_table_schema_t);

		int field_count = tbl->schema->field_count;
		if ( mt->schema->field_count > 0 )
			field_count = mt->schema->field_count;

		/* this is ugly, but works for what it needs to do */
		mt->schema->fields = (dd_datafield_t **)offset;
		offset += sizeof(dd_datafield_t **);
		*mt->schema->fields = (dd_datafield_t *)offset;
		offset += sizeof(dd_datafield_t *) * field_count;
		for(uint8_t i = 0; i < field_count; i++) {
			mt->schema->fields[i] = (dd_datafield_t *)offset;
			offset += sizeof(dd_datafield_t);
		}

		mt->used_slots = (uint64_t *)(offset);
		if ( mt->total_record_count > 0 )
			offset += sizeof(uint64_t) * mt->total_record_count;
		else
			offset += sizeof(uint64_t) * tbl->total_record_count;

		mt->free_slots = (uint64_t *)(offset);
		if ( mt->total_record_count > 0 )
			offset += sizeof(uint64_t) * mt->total_record_count;
		else
			offset += sizeof(uint64_t) * tbl->total_record_count;

		mt->data = (void *)(offset);
	} else if ( mt->total_record_count != tbl->total_record_count ||
				mt->schema->record_size != tbl->schema->record_size) {
		// remap event
		printf("Remap event\n");
		printf("Total records on disk %" PRIu64 " sized to %" PRIu64 "\n", mt->total_record_count, tbl->total_record_count);
		printf("Record size on disk %" PRIu16 " sized to %" PRIu16 "\n", mt->schema->record_size, tbl->schema->record_size);
	} else {
		// unexpected failure
		printf("Unexpected failure\n");
	}

	if ( mt->total_record_count == 0 ) {
		printf("Initializing table %s\n", tbl->table_name);
		strcpy(mt->table_name, tbl->table_name);
		mt->header_size = metadatasize;
		mt->total_record_count = tbl->total_record_count;
		mt->free_record_slot = tbl->total_record_count - 1;

		/* retain the file mapped set of fields */
		char **field_ptr = (char **)mt->schema->fields;
		/*
		 * after this copy, the fields array no longer is properly pointed
		 *
		 */
		memcpy(mt->schema, tbl->schema, sizeof(dd_table_schema_t));
		mt->schema->fields = (dd_datafield_t **)field_ptr;
		for(uint8_t i = 0; i < mt->schema->field_count; i++) {
			dd_datafield_t *dst = mt->schema->fields[i];
			dd_datafield_t *src = tbl->schema->fields[i];
			memcpy(dst, src, sizeof(dd_datafield_t));
		}

		for(uint64_t i = 0; i < mt->total_record_count; i++) {
			mt->free_slots[mt->free_record_slot - i] = i;
			mt->used_slots[i] = UINT64_MAX;
		}

	} else {
		char ts[31];
		format_timestamp(&mt->closedtm, ts);
		printf("Opened table %s last closed at %s\n", mt->table_name, ts);
	}

	mt->filedes = fd;
	mt->filesize = fs;

	tbl->mapped_table = mt;
	free(diskfile);
	free(shmfile);
	free(table_file_name);
	return true;
}

bool close_dd_table(db_table_t *tbl) {
	int fd = tbl->filedes;
	size_t fs = tbl->filesize;
	size_t tnsz = sizeof(char) * strlen(tbl->table_name) + 5;

	char *tpth = NULL;
	if ( (tpth = getenv("TABLE_DATA")) == NULL )
		tpth = DEFAULT_BASE;

	char *tn = malloc(tnsz);
	bzero(tn, tnsz);
	strcpy(tn, tbl->table_name);
	strcat(tn, ".shm");

	clock_gettime(CLOCK_REALTIME, &tbl->mapped_table->closedtm);
	char ts[31];
	format_timestamp(&tbl->mapped_table->closedtm, ts);
	printf("Closing %s at %s\n", tn, ts);

	munmap(tbl->mapped_table, fs);
	close(fd);

	move_and_replace_file(DEFAULT_SHM, tpth, tn);

	free(tn);
	return true;
}

uint64_t add_db_table_record(db_table_t *tbl, char *record) {
	char *sr = 0;
	uint64_t slot = UINT64_MAX;
	uint64_t cs = tbl->free_record_slot;

	if ( cs < tbl->total_record_count ) {
		slot = tbl->free_slots[cs];
		sr = (char *)(tbl->data + (slot * tbl->schema->record_size));
		memcpy(sr, record, tbl->schema->record_size);

		tbl->used_slots[slot] = cs;
		tbl->free_slots[cs] = tbl->total_record_count;
		tbl->free_record_slot = cs == 0 ? UINT64_MAX : cs - 1;
	}
	return slot;
}

bool delete_db_table_record(db_table_t *tbl, uint64_t slot, char *deleted_record) {
	bool rv = false;
	char *target = NULL;

	if ( slot < tbl->total_record_count && tbl->used_slots[slot] < UINT64_MAX) {
		target = (char *)(tbl->data + (slot * tbl->schema->record_size));
		if ( deleted_record != NULL )
			memcpy(deleted_record, target, tbl->schema->record_size);
		bzero(target, tbl->schema->record_size);

		tbl->used_slots[slot] = UINT64_MAX;
		tbl->free_record_slot++;
		tbl->free_slots[tbl->free_record_slot] = slot;
		rv = true;
	}

	return rv;
}

char *read_db_table_record(db_table_t *tbl, uint64_t slot) {
	char *record = NULL;
	if ( slot < tbl->total_record_count && tbl->used_slots[slot] < UINT64_MAX)
		record = (char *)(tbl->data + (slot * tbl->schema->record_size));

	return record;
}

char *new_db_table_record(dd_table_schema_t *tbl) {
	char *record = NULL;
	record = malloc(tbl->record_size);
	bzero(record, tbl->record_size);
	return record;
}

void reset_db_table_record(dd_table_schema_t *tbl, char *record) {
	bzero(record, tbl->record_size);
}

void release_table_record(dd_table_schema_t *tbl, char *record) {
	if (record != NULL) {
		memset(record, 0, tbl->record_size);
		free(record);
	}
}

bool set_db_table_record_field(dd_table_schema_t *tbl, char *field_name, char *value, char *data) {
	size_t offset = 0;
	bool found = false;
	for(uint8_t i = 0; i < tbl->fields_sz; i++) {
		if ( strcmp(tbl->fields[i]->field_name, field_name) == 0 ) {
			found = true;
			memcpy(data + offset, value, tbl->fields[i]->fieldsz);
		} else
			offset += tbl->fields[i]->fieldsz;
	}
	return found;
}
