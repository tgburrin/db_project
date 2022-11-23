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
#include <stdint.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <stdbool.h>
#include <regex.h>
#include <uuid/uuid.h>

#define DEFAULT_BASE "/data/tables"
#define DEFAULT_SHM "/dev/shm"

#define DB_OBJECT_NAME_SZ 64

#define ISO8601_UTC_DATETIME_EXPR "^([0-9]{4}-[0-2][0-9]-[0-3][0-9])[ T]?([0-2][0-9]:[0-5][0-9]:[0-6][0-9]\\.?[0-9]+?)?(Z|[+]00)?$"
#define ISO8601_DATE_EXPR "^([0-9]{4})-([0-2][0-9])-([0-3][0-9])$"
#define ISO8601_TIME_EXPR "^([0-2][0-9]):([0-6][0-9]):([0-6][0-9])\\.?([0-9]+)?$"

extern regex_t datetime_expr, date_expr, time_expr;

bool init_common(void);
bool cleanup_common(void);
size_t initialize_file(char *filepath, size_t sz, int *rfd);
int copy_and_replace_file(char *src_path, char *dst_path, char *filename);
int move_and_replace_file(char *src_path, char *dst_path, char *filename);
bool is_utc_timestamp(char *timestr);
bool parse_date(char *datestr, struct timespec *tm);
bool parse_timestamp(char *timestr, struct timespec *tm);
void format_timestamp(struct timespec *t, char out[31]);

#endif /* UTILS_H_ */
