/*
 * utils.h
 *
 *  Created on: Apr 3, 2022
 *      Author: tgburrin
 */

#ifndef UTILS_H_
#define UTILS_H_

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <stdbool.h>
#include <regex.h>

#define DEFAULT_BASE "/data/tables"
#define DEFAULT_SHM "/dev/shm"

#define DB_OBJECT_NAME_SZ 64

#define UTC_TIMESTAMP "^[0-9]{4}-[0-2][0-9]-[0-3][0-9][ T][0-2][0-9]:[0-5][0-9]:[0-6][0-9](\\.[0-9]+)?(Z|[+]00)$"

size_t initialize_file(char *filepath, size_t sz, int *rfd);
int copy_and_replace_file(char *src_path, char *dst_path, char *filename);
int move_and_replace_file(char *src_path, char *dst_path, char *filename);
bool parse_timestamp(char *timestr, struct timespec *tm);
void format_timestamp(struct timespec *t, char out[31]);

#endif /* UTILS_H_ */
