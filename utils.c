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
	}

	return rv;
}

int copy_and_replace_file(char *src_path, char *dst_path, char *filename) {
	int rv = 0, rfd = -1, wfd = -1;
	char *src_file, *dst_file;
	void *buffer;
	size_t sfile = strlen(src_path) + strlen(filename) + sizeof(char)*2;
	size_t dfile = strlen(dst_path) + strlen(filename) + sizeof(char)*2;
	size_t read_size = sizeof(char) * 1024 * 8; // 8kb aka 2 pages

	src_file = malloc(sfile);
	bzero(src_file, sfile);

	dst_file = malloc(dfile);
	bzero(dst_file, dfile);

	buffer = malloc(read_size);
	bzero(buffer, read_size);

	strcat(dst_file, dst_path);
	strcat(dst_file, "/");
	strcat(dst_file, filename);

	strcat(src_file, src_path);
	strcat(src_file, "/");
	strcat(src_file, filename);

	if ( (wfd = open(dst_file, O_CREAT | O_TRUNC | O_RDWR, 0640)) >= 0 ) {
		if ( (rfd = open(src_file, O_RDONLY)) >= 0 ) {
			size_t rb = 0;
			bzero(buffer, read_size);

			while( (rb = read(rfd, buffer, read_size)) > 0 ) {
				long written = 0;
				int bytes = 0;

				while ( written < rb && (bytes = write(wfd, buffer + sizeof(char) * written, rb)) >= 0 )
					written += bytes;

				if ( bytes < 0 ) {
					fprintf(stderr, "Error writing to %s: %s\n", dst_file, strerror(errno));
					break;
				}
				if ( written > 0 ) {
					rv += written;
					bzero(buffer, read_size);
				}
			}
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
	size_t sfile = strlen(src_path) + strlen(filename) + sizeof(char)*2;

	char *src_file = malloc(sfile);
	bzero(src_file, sfile);
	strcat(src_file, src_path);
	strcat(src_file, "/");
	strcat(src_file, filename);

	int rv = copy_and_replace_file(src_path, dst_path, filename);
	unlink(src_file);

	free(src_file);
	return rv;
}
