/*
 * journal_tools.c
 *
 *  Created on: Apr 11, 2022
 *      Author: tgburrin
 */

#include "utils.h"
#include "journal_tools.h"

bool new_journal (journal_t *j) {
	bool rv = false;
	char *jpth;
	char *filepath;

	// this must be turned off intentionally in the app
	j->fsync_on = true;

	if ( (jpth = getenv("TABLE_DATA")) == NULL )
		jpth = DEFAULT_BASE;

	uint16_t fname_len = strlen(jpth) + 1 + strlen(DEFAULT_JOURNAL) + 1;
	filepath = malloc(fname_len);
	bzero(filepath, fname_len);

	strcat(filepath, jpth);
	strcat(filepath, "/");
	strcat(filepath, DEFAULT_JOURNAL);

	printf("Starting new journal at %s\n", filepath);

	int i = access(filepath, F_OK);
	if ( i < 0 ) {
		if ( errno == ENOENT ) {
			printf("File not found, initializing...\n");
			if ( (j->jfd = open(filepath, O_CREAT | O_RDWR, 0640)) < 0 ) {
				fprintf(stderr, "Unable to open file\n");
				fprintf(stderr, "Error: %s\n", strerror(errno));
			} else {
				printf("%s created\n", filepath);
				j->opened = 1;
				j->jfp = fdopen(j->jfd, "w+");
			}

		} else {
			fprintf(stderr, "File %s could not be accessed\n", filepath);

		}
	} else if ( i == 0 ) {
		if ( (j->jfd = open(filepath, O_CREAT | O_TRUNC | O_RDWR, 0640)) < 0 ) {
			fprintf(stderr, "Unable to open file\n");
			fprintf(stderr, "Error: %s\n", strerror(errno));

		} else {
			printf("%s truncated\n", filepath);
			j->opened = 1;
			j->jfp = fdopen(j->jfd, "w+");
		}
	}

	if ( j->opened ) {
		journal_record_t new_journal;
		bzero(&new_journal, sizeof(journal_record_t));
		new_journal.msgsz = sizeof(journal_record_t);
		new_journal.objtype = 'a';
		if ( write_journal_record(j, &new_journal) ) {
			memcpy(&j->openedtm, &new_journal.txntime, sizeof(struct timespec));
			rv = true;
		} else {
			j->opened = 0;
			close(j->jfd);
		}
	}

	free(filepath);
	return rv;
}

bool open_journal (journal_t *j) {
	bool rv = false;
	char *jpth;
	char *filepath;

	// this must be turned off intentionally in the app
	j->fsync_on = true;

	if ( (jpth = getenv("TABLE_DATA")) == NULL )
		jpth = DEFAULT_BASE;

	uint16_t fname_len = strlen(jpth) + 1 + strlen(DEFAULT_JOURNAL) + 1;
	filepath = malloc(fname_len);
	bzero(filepath, fname_len);

	strcat(filepath, jpth);
	strcat(filepath, "/");
	strcat(filepath, DEFAULT_JOURNAL);

	int i = access(filepath, F_OK);
	if ( i == 0 ) {
		if ( (j->jfd = open(filepath, O_RDWR, 0640)) < 0 ) {
			fprintf(stderr, "Unable to open file\n");
			fprintf(stderr, "Error: %s\n", strerror(errno));

		} else {
			lseek(j->jfd, 0, SEEK_SET);
			printf("%s opened with descriptor %d\n", filepath, j->jfd);
			j->opened = 1;
			j->jfp = fdopen(j->jfd, "r+");
			rv = true;
		}
	}

	return rv;
}

bool journal_sync_off(journal_t *j) {
	bool rv = false;
	if ( j->opened && j->fsync_on ) {
		j->fsync_on = false;
		rv = true;
	}
	return rv;
}

bool journal_sync_on(journal_t *j) {
	bool rv = false;
	if ( j->opened && !j->fsync_on ) {
		j->fsync_on = true;
		fsync(j->jfd);
		rv = true;
	}
	return rv;
}

void close_journal (journal_t *j) {
	if ( j->opened ) {
		journal_record_t new_journal;
		bzero(&new_journal, sizeof(journal_record_t));
		new_journal.msgsz = sizeof(journal_record_t);
		new_journal.objtype = 'z';

		write_journal_record(j, &new_journal);

		fclose(j->jfp);
		close(j->jfd);
		j->opened = 0;
	}
}

journal_record_t *read_journal (journal_t *j) {
	journal_record_t *rv = NULL, *rec = NULL;
	if ( j->opened ) {
		printf("Starting at postition %ld/%ld\n", lseek(j->jfd, 0, SEEK_CUR), ftell(j->jfp));
		char stx = 2;
		int c;
		while ( (c = getc(j->jfp)) != EOF && c != stx);
		if ( c == stx ) {
			printf("Start of journal record found\n");
			size_t rb = 0;
			rec = malloc(sizeof(journal_record_t));
			bzero(rec, sizeof(journal_record_t));

			lseek(j->jfd, ftell(j->jfp), SEEK_SET);

			while ( (c = read(j->jfd, rec + rb, sizeof(journal_record_t) - rb)) > 0 ) {
				if ( c < 0 )
					break;

				rb += c;
				if ( rb == sizeof(journal_record_t) )
					break;

			}
			if ( rb == sizeof(journal_record_t) ) {
				printf("Record of %ld size found\n", rec->msgsz);
				if ( rec->objsz > 0 ) {
					rec = realloc(rec, rec->msgsz);
					// realloc does not initialize the new memory
					memset(rec + sizeof(journal_record_t), 0, rec->objsz);
					rec->objdata = (char *)rec + sizeof(journal_record_t);
					rb = 0;
					while ( (c = read(j->jfd, rec->objdata + rb, rec->objsz - rb)) > 0 ) {
						if ( c < 0 )
							break;

						rb += c;
						if ( rb == rec->objsz )
							break;
					}
					if ( rb == rec->objsz ) {
						rv = rec;
						fseek(j->jfp, lseek(j->jfd, 0, SEEK_CUR), SEEK_SET);
						printf("Complete journal record consumed\n");
					} else
						fprintf(stderr, "Error reading from file: %s\n", strerror(errno));
				} else {
					rv = rec;
					fseek(j->jfp, lseek(j->jfd, 0, SEEK_CUR), SEEK_SET);
					printf("Complete journal record consumed\n");
				}
			} else
				fprintf(stderr, "Error reading from file: %s\n", strerror(errno));

		} else if ( c == EOF ) {
			printf("End of journal reached\n");
		}
	}

	if ( rv == NULL && rec != NULL )
		free(rec);

	return rv;
}

bool write_journal_record(journal_t *j, journal_record_t *jr) {
	bool rv = false;
	if ( j->opened ) {
		char stx = 2;

		//printf("Writing record of type %c\n", jr->objtype);
		struct timespec txn_time;
		clock_gettime(CLOCK_REALTIME, &txn_time);
		jr->txntime = txn_time;

		write(j->jfd, &stx, 1);
		write(j->jfd, jr, sizeof(journal_record_t));
		if ( jr->objsz > 0 )
			write(j->jfd, jr->objdata, jr->objsz);

		if ( j->fsync_on )
			fdatasync(j->jfd); // fsync(j->jfd);
		rv = true;
	}
	return rv;
}
