/*
 * table_tools.c
 *
 *  Created on: Mar 28, 2022
 *      Author: tgburrin
 */

#include "data_dictionary.h"

char *make_table_path(const char *base, const char *table_name, const char *extension) {
	int i = 0;
	//  '/this/path'    '/'  'filename' '.' 'extension'
	i = strlen(base) + 1 + strlen(table_name) + 1 + strlen(extension) + 1;
	char *filepath = malloc(sizeof(char)*i);
	bzero(filepath, sizeof(char)*i);

	strcat(filepath, base);
	strcat(filepath, "/");
	strcat(filepath, table_name);
	strcat(filepath, ".");
	strcat(filepath, extension);
	return filepath;
}

bool open_dd_table(db_table_t *tbl) {
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

	size_t data_size = (uint64_t)tbl->schema->record_size * (uint64_t)tbl->total_record_count;
	size_t metadatasize =
			sizeof(db_table_t) +
			sizeof(dd_table_schema_t) +
			sizeof(dd_datafield_t **) +
			sizeof(dd_datafield_t *) * tbl->schema->field_count +
			sizeof(dd_datafield_t) * tbl->schema->field_count +
			sizeof(uint64_t) * tbl->total_record_count * 2;


	char *diskfile = make_table_path(tpth, tbl->table_name, "shm");
	char *shmfile = make_table_path(DEFAULT_SHM, tbl->table_name, "shm");

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
		if ( (fd = open(shmfile, O_RDWR | O_NOATIME, 0640)) < 0 ) {
			fprintf(stderr, "Error opening shm file %s: %s\n", shmfile, strerror(errno));
			exit(EXIT_FAILURE);
		}
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

		mt->used_slots = (record_num_t *)(offset);
		if ( mt->total_record_count > 0 )
			offset += sizeof(record_num_t) * mt->total_record_count;
		else
			offset += sizeof(record_num_t) * tbl->total_record_count;

		mt->free_slots = (record_num_t *)(offset);
		if ( mt->total_record_count > 0 )
			offset += sizeof(record_num_t) * mt->total_record_count;
		else
			offset += sizeof(record_num_t) * tbl->total_record_count;

		mt->data = (void *)(offset);
	} else if ( mt->total_record_count != tbl->total_record_count ||
				mt->schema->record_size != tbl->schema->record_size) {
		// remap event
		printf("Remap event\n");
		printf("Total records on disk %" PRIu64 " sized to %" PRIu64 "\n", (uint64_t)mt->total_record_count, (uint64_t)tbl->total_record_count);
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
			mt->used_slots[i] = RECORD_NUM_MAX;
		}

	} else {
		char ts[31];
		format_timestamp(&mt->closedtm, ts);
		printf("Opened table %s last closed at %s\n", mt->table_name, ts);
	}

	mt->filedes = fd;
	mt->table_type = DD_TABLE_TYPE_BOTH;
	mt->filesize = fs;

	tbl->mapped_table = mt;
	free(diskfile);
	free(shmfile);
	free(table_file_name);
	return true;
}

bool open_dd_disk_table(db_table_t *tbl) {
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

	char *diskfile = make_table_path(tpth, tbl->table_name, "shm");

	i = access(diskfile, F_OK);
	if ( i < 0 ) {
		if ( errno == ENOENT )
			fprintf(stderr, "%s not found exiting...\n", diskfile);
		else
			fprintf(stderr, "File %s could not be accessed\n", diskfile);
		exit(EXIT_FAILURE);
	} else if ( i == 0 ) {
		if ( (fd = open(diskfile, O_RDWR, 0640)) < 0 ) {
			fprintf(stderr, "Unable to open file %s\n", diskfile);
			fprintf(stderr, "Error: %s\n", strerror(errno));
			exit(EXIT_FAILURE);
		}
	}

	size_t fs = lseek(fd, 0, SEEK_END);

	if ( (dbfile = mmap(NULL, fs, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)) == MAP_FAILED ) {
		fprintf(stderr, "Unable to map file %s to memory\n", diskfile);
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

		mt->used_slots = (record_num_t *)(offset);
		if ( mt->total_record_count > 0 )
			offset += sizeof(record_num_t) * mt->total_record_count;
		else
			offset += sizeof(record_num_t) * tbl->total_record_count;

		mt->free_slots = (record_num_t *)(offset);
		if ( mt->total_record_count > 0 )
			offset += sizeof(record_num_t) * mt->total_record_count;
		else
			offset += sizeof(record_num_t) * tbl->total_record_count;

		mt->data = (void *)(offset);
	} else if ( mt->total_record_count != tbl->total_record_count ||
				mt->schema->record_size != tbl->schema->record_size) {
		// remap event
		printf("Remap event\n");
		printf("Total records on disk %" PRIu64 " sized to %" PRIu64 "\n", (uint64_t)mt->total_record_count, (uint64_t)tbl->total_record_count);
		printf("Record size on disk %" PRIu16 " sized to %" PRIu16 "\n", mt->schema->record_size, tbl->schema->record_size);
	} else {
		// unexpected failure
		printf("Unexpected failure\n");
	}

	char ts[31];
	format_timestamp(&mt->closedtm, ts);
	printf("Opened table %s last closed at %s\n", mt->table_name, ts);

	mt->filedes = fd;
	mt->table_type = DD_TABLE_TYPE_DISK;
	mt->filesize = fs;

	tbl->mapped_table = mt;
	free(diskfile);
	free(table_file_name);
	return true;
}

bool open_dd_shm_table(db_table_t *tbl) {
	printf("opening shm table\n");
	int fd = -1;
	char *dbfile;
	db_table_t *mt;

	bool table_created = false;

	size_t data_size = (uint64_t)tbl->schema->record_size * (uint64_t)tbl->total_record_count;
	size_t metadatasize =
			sizeof(db_table_t) +
			sizeof(dd_table_schema_t) +
			sizeof(dd_datafield_t **) +
			sizeof(dd_datafield_t *) * tbl->schema->field_count +
			sizeof(dd_datafield_t) * tbl->schema->field_count +
			sizeof(uint64_t) * tbl->total_record_count * 2;

	char *shmfile = make_table_path(DEFAULT_SHM, tbl->table_name, "shm");

	size_t fs = initialize_file(shmfile, metadatasize + data_size, &fd);

	if ( (dbfile = mmap(NULL, fs, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)) == MAP_FAILED ) {
		fprintf(stderr, "Unable to map file to memory\n");
		fprintf(stderr, "Error: %s\n", strerror(errno));
		return false;

	}

	char *offset = dbfile;
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

	mt->used_slots = (record_num_t *)(offset);
	if ( mt->total_record_count > 0 )
		offset += sizeof(record_num_t) * mt->total_record_count;
	else
		offset += sizeof(record_num_t) * tbl->total_record_count;

	mt->free_slots = (record_num_t *)(offset);
	if ( mt->total_record_count > 0 )
		offset += sizeof(record_num_t) * mt->total_record_count;
	else
		offset += sizeof(record_num_t) * tbl->total_record_count;

	mt->data = (void *)(offset);

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
			mt->used_slots[i] = RECORD_NUM_MAX;
		}
		table_created = true;

	}

	if ( table_created ) {
		printf("Opened table %s for the first time\n", mt->table_name);
	} else {
		char ts[31];
		format_timestamp(&mt->closedtm, ts);
		printf("Opened table %s last closed at %s\n", mt->table_name, ts);
	}

	mt->filedes = fd;
	mt->table_type = DD_TABLE_TYPE_SHM;
	mt->filesize = fs;

	tbl->mapped_table = mt;
	free(shmfile);
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

	tbl->mapped_table = NULL;

	move_and_replace_file(DEFAULT_SHM, tpth, tn);

	free(tn);
	return true;
}

bool close_dd_disk_table(db_table_t *tbl) {
	int fd = tbl->filedes;
	size_t fs = tbl->filesize;
	size_t tnsz = sizeof(char) * strlen(tbl->table_name) + 5;

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

	tbl->mapped_table = NULL;
	free(tn);
	return true;
}

bool close_dd_shm_table (db_table_t *tbl) {
	int fd = tbl->filedes;
	size_t fs = tbl->filesize;
	size_t tnsz = sizeof(char) * strlen(tbl->table_name) + 5;

	char *tn = malloc(tnsz);
	bzero(tn, tnsz);
	strcpy(tn, tbl->table_name);
	strcat(tn, ".shm");

	size_t sfile = strlen(DEFAULT_SHM) + strlen(tn) + sizeof(char)*2;

	char *shm_file_path = malloc(sfile);
	bzero(shm_file_path, sfile);
	strcat(shm_file_path, DEFAULT_SHM);
	strcat(shm_file_path, "/");
	strcat(shm_file_path, tn);

	clock_gettime(CLOCK_REALTIME, &tbl->mapped_table->closedtm);
	char ts[31];
	format_timestamp(&tbl->mapped_table->closedtm, ts);
	printf("Closing %s at %s\n", tn, ts);

	munmap(tbl->mapped_table, fs);
	close(fd);

	tbl->mapped_table = NULL;
	unlink(shm_file_path);
	free(tn);
	return true;
}

record_num_t add_db_table_record(db_table_t *tbl, char *record) {
	char *sr = 0;
	record_num_t slot = RECORD_NUM_MAX;
	record_num_t cs = tbl->free_record_slot;

	if ( cs < tbl->total_record_count ) {
		slot = tbl->free_slots[cs];
		sr = tbl->data + (uintptr_t)slot * (uintptr_t)tbl->schema->record_size;
		memcpy(sr, record, tbl->schema->record_size);

		tbl->used_slots[slot] = cs;
		tbl->free_slots[cs] = tbl->total_record_count;
		tbl->free_record_slot = cs == 0 ? RECORD_NUM_MAX : cs - 1;
	}
	return slot;
}

bool delete_db_table_record(db_table_t *tbl, record_num_t slot, char *deleted_record) {
	bool rv = false;
	char *target = NULL;

	if ( slot < tbl->total_record_count && tbl->used_slots[slot] < RECORD_NUM_MAX) {
		target = tbl->data + (uintptr_t)slot * (uintptr_t)tbl->schema->record_size;
		if ( deleted_record != NULL )
			memcpy(deleted_record, target, tbl->schema->record_size);

		bzero(target, tbl->schema->record_size);
		tbl->used_slots[slot] = RECORD_NUM_MAX;
		tbl->free_record_slot++;
		tbl->free_slots[tbl->free_record_slot] = slot;
		rv = true;
	}

	return rv;
}

char *read_db_table_record(db_table_t *tbl, record_num_t slot) {
	char *record = NULL;
	if ( slot < tbl->total_record_count && tbl->used_slots[slot] < RECORD_NUM_MAX)
		record = tbl->data + (uintptr_t)slot * (uintptr_t)tbl->schema->record_size;

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

dd_datafield_t *get_db_table_field(dd_table_schema_t *tbl, char *field_name) {
	dd_datafield_t *rv = NULL;
	for(uint8_t i = 0; i < tbl->num_fields && rv == NULL; i++)
		if ( strcmp(tbl->fields[i]->field_name, field_name) == 0 )
			rv = tbl->fields[i];
	return rv;
}

bool set_db_table_record_field(dd_table_schema_t *tbl, char *field_name, char *value, char *data) {
	size_t offset = 0;
	bool found = false;
	for(uint8_t i = 0; i < tbl->num_fields && !found; i++) {
		if ( strcmp(tbl->fields[i]->field_name, field_name) == 0 ) {
			found = true;
			memcpy(data + offset, value, tbl->fields[i]->field_sz);
		} else
			offset += tbl->fields[i]->field_sz;
	}
	return found;
}

bool set_db_table_record_field_str(dd_table_schema_t *tbl, char *field_name, char *value, char *data) {
	//size_t offset = 0;
	bool found = false;
	for(uint8_t i = 0; i < tbl->num_fields && !found; i++) {
		if ( strcmp(tbl->fields[i]->field_name, field_name) == 0 ) {
			dd_datafield_t *field = tbl->fields[i];
			char *fielddata = malloc(field->field_sz);
			bzero(fielddata, field->field_sz);
			str_to_dd_type(field, value, fielddata);
			set_db_table_record_field(tbl, field_name, fielddata, data);
			free(fielddata);
			found = true;
			break;
		}
	}
	return found;
}

bool set_db_table_record_field_num(dd_table_schema_t *tbl, uint8_t pos, char *invalue, char *outdata) {
	size_t offset = 0;
	bool found = false;
	for(uint8_t i = 0; !found && i <= pos && i < tbl->num_fields; i++) {
		if ( i == pos ) {
			found = true;
			memcpy(outdata + offset, invalue, tbl->fields[i]->field_sz);
		} else
			offset += tbl->fields[i]->field_sz;
	}
	return found;
}

bool get_db_table_record_field_value_str_alloc(dd_table_schema_t *tbl, char *field_name, char *indata, char **outdata) {
	char *rv = NULL;
	size_t offset = 0;
	bool found = false;

	for(uint8_t i = 0; !found && i < tbl->num_fields; i++) {
		if ( strcmp(tbl->fields[i]->field_name, field_name) == 0) {
			found = true;
			rv = malloc(tbl->fields[i]->field_sz);
			bzero(rv, tbl->fields[i]->field_sz);
			memcpy(rv, indata + offset, tbl->fields[i]->field_sz);
			*outdata = rv;
		} else
			offset += tbl->fields[i]->field_sz;
	}
	return found;
}

bool get_db_table_record_field_value_str(dd_table_schema_t *tbl, char *field_name, char *indata, char *outdata) {
	size_t offset = 0;
	bool found = false;

	for(uint8_t i = 0; !found && i < tbl->num_fields; i++) {
		if ( strcmp(tbl->fields[i]->field_name, field_name) == 0) {
			found = true;
			memcpy(outdata, indata + offset, tbl->fields[i]->field_sz);
		} else
			offset += tbl->fields[i]->field_sz;
	}
	return found;
}

bool get_db_table_record_field_num(dd_table_schema_t *tbl, uint8_t pos, char *indata, char *outvalue) {
	size_t offset = 0;
	bool found = false;
	for(uint8_t i = 0; !found && i <= pos && i < tbl->num_fields; i++) {
		if ( i == pos ) {
			found = true;
			memcpy(outvalue + offset, indata, tbl->fields[i]->field_sz);
		} else
			offset += tbl->fields[i]->field_sz;
	}
	return found;
}

void db_table_record_print(dd_table_schema_t *tbl, char *data) {
	char *buff = db_table_record_print_alloc(tbl, data);
	printf("%s", buff);
	free(buff);
}

char *db_table_record_print_alloc(dd_table_schema_t *tbl, char *data) {
	char *rv = NULL;
	if ( data == NULL ) {
		fprintf(stderr, "No table data to print\n");
		return rv;
	}

	size_t rec_offset = 0, rv_offset = 0, max_label_size = 0, max_field_size = 0, buffsz = 0;
	dd_datafield_t *f = NULL;
	for(uint8_t i = 0; i < tbl->num_fields; i++) {
		f = tbl->fields[i];
		if ( strlen(f->field_name)+1 > max_label_size)
			max_label_size = strlen(f->field_name)+1;
		size_t field_str_size = dd_type_strlen(f);
		if ( field_str_size > max_field_size)
			max_field_size = field_str_size;
	}
	buffsz = (max_label_size + strlen(": ") + max_field_size + strlen("\n")) * tbl->num_fields;
	char field_label[max_label_size+1];
	char field_value[max_field_size+1];

	rv = malloc(buffsz);
	bzero(rv, buffsz);

	for(uint8_t i = 0; i < tbl->num_fields; i++) {
		bzero(&field_label, sizeof(field_label));
		bzero(&field_value, sizeof(field_value));

		memset(field_label, ' ', max_label_size);
		field_label[max_label_size] = '\0';

		memcpy(field_label, tbl->fields[i]->field_name, strlen(tbl->fields[i]->field_name));
		dd_type_to_str(tbl->fields[i], data + rec_offset, field_value);

		sprintf(rv + rv_offset, "%s: %s\n", field_label, field_value);
		rv_offset = strlen(rv);
		rec_offset += tbl->fields[i]->field_sz;
	}
	return rv;
}

char *db_table_record_print_line_alloc(dd_table_schema_t *tbl, char *data) {
	char *rv = NULL;
	if ( data == NULL ) {
		fprintf(stderr, "No table data to print\n");
		return rv;
	}

	size_t rec_offset = 0, rv_offset = 0, label_size_total = 0, field_size_total = 0, buffsz = 0;
	dd_datafield_t *f = NULL;
	for(uint8_t i = 0; i < tbl->num_fields; i++) {
		f = tbl->fields[i];
		label_size_total += strlen(f->field_name);
		field_size_total += dd_type_strlen(f);
	}
	buffsz = (strlen(" -> ") +  strlen(", ")) * tbl->num_fields + label_size_total + field_size_total + 4;

	rv = malloc(buffsz);
	bzero(rv, buffsz);

	sprintf(rv, "<<");
	rv_offset += 2;

	for(uint8_t i = 0; i < tbl->num_fields; i++) {
		f = tbl->fields[i];
		char field_value[dd_type_strlen(f)];
		bzero(&field_value, sizeof(field_value));
		dd_type_to_str(tbl->fields[i], data + rec_offset, field_value);
		sprintf(rv + rv_offset, "%s%s -> %s", i > 0 ? ", ": "", f->field_name, field_value);

		rv_offset = strlen(rv);
		rec_offset += tbl->fields[i]->field_sz;
	}

	sprintf(rv + rv_offset, ">>");
	rv_offset += 2;

	return rv;
}
