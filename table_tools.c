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
	void *dbfile;
	table_t *mt;
	char *tpth, *shmpth, *table_file_name;

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

	printf("Checking path %s\n", diskfile);

	size_t fs = initialize_file(diskfile, metadatazie + data_size, &fd);

	if ( (dbfile = mmap(NULL, fs, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)) == MAP_FAILED ) {
		fprintf(stderr, "Unable to map file to memory\n");
		fprintf(stderr, "Error: %s\n", strerror(errno));
		return rv;
	} else {
		printf("Mapped %ld bytes to fd %d\n", fs, fd);
	}

	mt = dbfile;

	if ( mt->total_record_count == 0 ||
		(mt->total_record_count == tablemeta->total_record_count && mt->record_size == tablemeta->record_size)) {
		munmap(dbfile, fs);
		close(fd);

		copy_and_replace_file(tpth, DEFAULT_SHM, table_file_name);
		fd = open(shmfile, O_RDWR, 0640);
		dbfile = mmap(NULL, fs, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
		mt = dbfile;
	} else if ( mt->total_record_count != tablemeta->total_record_count ||
				mt->record_size != tablemeta->record_size) {
		// remap event
	} else {
		// unexpected failure
	}

	if ( mt->total_record_count == 0 ) {
		strcpy(mt->table_name, tablemeta->table_name);
		mt->record_size = tablemeta->record_size;
		mt->total_record_count = tablemeta->total_record_count;
		mt->free_record_slot = tablemeta->total_record_count - 1;

		void *offset = dbfile + sizeof(table_t);
		mt->used_slots = (uint64_t *) (offset);

		offset += sizeof(uint64_t) * tablemeta->total_record_count;
		mt->free_slots = (uint64_t *) (offset);

		offset += sizeof(uint64_t) * tablemeta->total_record_count;
		mt->data = (void *) (offset);

		for(int i = 0; i < mt->total_record_count; i++) {
			mt->free_slots[mt->free_record_slot - i] = i;
			mt->used_slots[i] = -1;
		}

	} else {
		void *offset = dbfile + sizeof(table_t);
		mt->used_slots = (uint64_t *) (offset);

		offset += sizeof(uint64_t) * tablemeta->total_record_count;
		mt->free_slots = (uint64_t *) (offset);

		offset += sizeof(uint64_t) * tablemeta->total_record_count;
		mt->data = (void *) (offset);

	}

	mt->filedes = fd;
	mt->filesize = fs;

	*mapped_table = mt;
	free(diskfile);
	free(shmfile);
	free(table_file_name);

	return rv;
}

int close_table(table_t *mapped_table) {
	int rv = 0, fd = mapped_table->filedes;
	size_t fs = mapped_table->filesize;

	munmap(mapped_table, fs);
	close(fd);
	return rv;
}
