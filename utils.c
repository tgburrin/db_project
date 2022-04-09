/*
 * utils.c
 *
 *  Created on: Apr 3, 2022
 *      Author: tgburrin
 */

#include "utils.h"

size_t initialize_file(char *filepath, size_t sz, int *rfd) {
	size_t rv = 0;
	int i, fd = -1;

	i = access(filepath, F_OK);
	if ( i < 0 ) {
		if ( errno == ENOENT ) {
			printf("File not found, initializing...\n");
			if ( (fd = open(filepath, O_CREAT | O_RDWR, 0640)) < 0 ) {
				fprintf(stderr, "Unable to open file\n");
				fprintf(stderr, "Error: %s\n", strerror(errno));

			} else {
				//printf("Creating file of %ld bytes\n", sz);
				//printf("Sizing file...\n");
				ftruncate(fd, sz);

			}
		} else {
			fprintf(stderr, "File %s could not be accessed\n", filepath);

		}
	} else if ( i == 0 ) {
		if ( (fd = open(filepath, O_RDWR, 0640)) < 0 ) {
			fprintf(stderr, "Unable to open file\n");
			fprintf(stderr, "Error: %s\n", strerror(errno));

		}
	}

	if ( fd >= 0 ) {
		rv = lseek(fd, 0, SEEK_END);
		*rfd = fd;
		//printf("%ld byte file opened for descriptor %d\n", rv, *rfd);
	}

	return rv;
}

int copy_and_replace_file(char *src_path, char *dst_path, char *filename) {
	int rv = 0, rfd, wfd;
	char *src_file, *dst_file;
	void *buffer;
	size_t sfile = strlen(src_path) + strlen(filename) + sizeof(char)*2;
	size_t dfile = strlen(dst_path) + strlen(filename) + sizeof(char)*2;
	size_t read_size = 1024 * 8; // 8kb aka 2 pages

	src_file = malloc(sfile);
	bzero(src_file, sfile);

	dst_file = malloc(dfile);
	bzero(dst_file, dfile);

	buffer = malloc(sizeof(char) * read_size);
	bzero(buffer, sizeof(char) * read_size);

	strcat(dst_file, dst_path);
	strcat(dst_file, "/");
	strcat(dst_file, filename);

	strcat(src_file, src_path);
	strcat(src_file, "/");
	strcat(src_file, filename);

	printf("Copying %s to %s\n", src_file, dst_file);

	if ( (wfd = open(dst_file, O_CREAT | O_TRUNC | O_RDWR, 0640)) >= 0 ) {
		if ( (rfd = open(src_file, O_RDONLY)) >= 0 ) {
			size_t rb = 0;
			while( (rb = read(rfd, buffer, read_size)) > 0 ) {
				printf("Read batch of %ld bytes\n", rb);
				long written = 0;
				int bytes = 0;

				while ( (bytes = write(wfd, buffer, rb)) >= 0 && written < rb )
					written += bytes;

				if ( bytes < 0 ) {
					fprintf(stderr, "Error writing to %s: %s\n", dst_file, strerror(errno));
					break;
				}
				if ( written > 0 ) {
					printf("Copied batch of %ld bytes\n", written);
					rv += written;
				}
			}
			printf("Copied %d bytes\n", rv);
		}
	}

	if ( wfd >= 0 )
		close(wfd);

	if ( rfd >= 0)
		close(rfd);

	free(buffer);
	free(src_file);
	free(dst_file);

	return rv;
}

int move_and_replace_file(char *src_path, char *dst_path, char *filename) {
	int rv = 0;
	return rv;
}
