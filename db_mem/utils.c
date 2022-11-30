/*
 * utils.c
 *
 *  Created on: Apr 3, 2022
 *      Author: tgburrin
 */

#include "utils.h"

regex_t datetime_expr, date_expr, time_expr;

bool init_common(void) {
	int errcd = 0;
	char errmsg[1024];
	setenv("TZ", "UTC", 1);

	bzero(&datetime_expr, sizeof(regex_t));
	if ( (errcd = regcomp(&datetime_expr, ISO8601_UTC_DATETIME_EXPR, REG_EXTENDED)) != 0 ) {
		regerror(errcd, &datetime_expr, errmsg, sizeof(errmsg));
		fprintf(stderr, "Failed to compile expression '%s': %s\n", ISO8601_UTC_DATETIME_EXPR, errmsg);
		return false;
	}

	bzero(&date_expr, sizeof(regex_t));
	if ( (errcd = regcomp(&date_expr, ISO8601_DATE_EXPR, REG_EXTENDED)) != 0 ) {
		regerror(errcd, &date_expr, errmsg, sizeof(errmsg));
		fprintf(stderr, "Failed to compile expression '%s': %s\n", ISO8601_DATE_EXPR, errmsg);
		return false;
	}

	bzero(&time_expr, sizeof(regex_t));
	if ( (errcd = regcomp(&time_expr, ISO8601_TIME_EXPR, REG_EXTENDED)) != 0 ) {
		regerror(errcd, &time_expr, errmsg, sizeof(errmsg));
		fprintf(stderr, "Failed to compile expression '%s': %s\n", ISO8601_TIME_EXPR, errmsg);
		return false;
	}

	return true;
}

bool cleanup_common(void) {
	regfree(&datetime_expr);
	regfree(&date_expr);
	regfree(&time_expr);
	return true;
}

size_t initialize_file(char *filepath, size_t sz, int *rfd) {
	size_t rv = 0;
	int i, fd = -1;

	i = access(filepath, F_OK);
	if ( i < 0 ) {
		if ( errno == ENOENT ) {
			printf("%s not found, initializing...\n", filepath);
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
	char *buffer;
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
				size_t written = 0;
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

bool is_utc_timestamp(char *timestr) {
	bool rv = false;

	if ( regexec(&datetime_expr, timestr, 0, 0, 0) == 0 )
		rv = true;

	 return rv;
}

bool parse_time(char *timestr, struct timespec *tm) {
	bool rv = false;

	time_t t1 = 0, t2 = 0;
	struct tm pt;
	localtime_r(&t1, &pt);
	struct tm day;
	localtime_r(&t2, &day);

	regmatch_t m[5];
	bzero(m, sizeof(m));

	uint16_t regmsz = sizeof(m)/sizeof(regmatch_t);

	if ( regexec(&time_expr, timestr, regmsz, m, 0) == 0 ) {
		size_t msglen = strlen(timestr)+1;
		char *msg = malloc(msglen);
		int i = 0;
		for ( i=1; i < regmsz && m[i].rm_so >= 0; i++ ) {
			bzero(msg, msglen);
			strncpy(msg, timestr + m[i].rm_so, m[i].rm_eo - m[i].rm_so);
			switch (i) {
				case 1:
					pt.tm_hour = atoi(msg);
					break;
				case 2:
					pt.tm_min = atoi(msg);
					break;
				case 3:
					pt.tm_sec = atoi(msg);
					break;
				case 4: ;
					uint8_t len = 9 - strlen(msg);
					uint64_t mult = 1; //power of 10
					for(int k = 0; k < len; k++)
						mult *= 10;
					long nsec = atol(msg) * mult;
					tm->tv_nsec = nsec;
					break;
			}
		}
		if ( i >= 3 ) {
			tm->tv_sec += mktime(&pt) - mktime(&day);
			rv = true;
		}
		free(msg);
	}
	return rv;
}

bool parse_date(char *datestr, struct timespec *tm) {
	bool rv = false;

	struct tm pt;
	bzero(&pt, sizeof(pt));

	regmatch_t m[4];
	bzero(m, sizeof(m));
	uint16_t regmsz = sizeof(m)/sizeof(regmatch_t);

	if ( regexec(&date_expr, datestr, regmsz, m, 0) == 0 ) {
		size_t msglen = strlen(datestr)+1;
		char *msg = malloc(msglen);
		int i = 0;
		for ( i=1; i < regmsz && m[i].rm_so >= 0; i++ ) {
			int timepart = 0;
			bzero(msg, msglen);
			strncpy(msg, datestr + m[i].rm_so, m[i].rm_eo - m[i].rm_so);
			switch (i) {
				case 1:
					timepart = atoi(msg);
					pt.tm_year = timepart - 1900;
					break;
				case 2:
					timepart = atoi(msg);
					pt.tm_mon = timepart - 1;
					break;
				case 3:
					timepart = atoi(msg);
					pt.tm_mday = timepart;
					break;
			}
		}
		free(msg);
		if ( i >= 3 ) {
			//printf("Adding %ld seconds based on date %s\n", mktime(&pt), datestr);
			tm->tv_sec += mktime(&pt);
			rv = true;
		}
	}

	return rv;
}

bool parse_timestamp(char *timestr, struct timespec *tm) {
	bool rv = false;

	bzero(tm, sizeof(struct timespec));

	regmatch_t m[6];
	bzero(m, sizeof(m));

	uint16_t regmsz = sizeof(m)/sizeof(regmatch_t);

	if ( regexec(&datetime_expr, timestr, regmsz, m, 0) == 0 ) {
		size_t msglen = strlen(timestr)+1;
		char *msg = malloc(msglen);
		for ( int i=1; i < regmsz && m[i].rm_so >= 0; i++ ) {
			bzero(msg, msglen);
			strncpy(msg, timestr + m[i].rm_so, m[i].rm_eo - m[i].rm_so);
			switch (i) {
				case 1:
					parse_date(msg, tm);
					break;
				case 2:
					parse_time(msg, tm);
					break;
			}
		}
		free(msg);
	}
	return rv;
}

void format_timestamp(struct timespec *t, char out[31]) {
    const int tmpsize = 21;
    struct tm tm;

    gmtime_r(&t->tv_sec, &tm);
    strftime(out, tmpsize, "%Y-%m-%dT%H:%M:%S.", &tm);
    sprintf(out + tmpsize -1, "%09luZ", t->tv_nsec);
}
