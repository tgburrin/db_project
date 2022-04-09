/*
 * utils.h
 *
 *  Created on: Apr 3, 2022
 *      Author: tgburrin
 */

#ifndef UTILS_H_
#define UTILS_H_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

size_t initialize_file(char *filepath, size_t sz, int *rfd);
int copy_and_replace_file(char *src_path, char *dst_path, char *filename);
int move_and_replace_file(char *src_path, char *dst_path, char *filename);

#endif /* UTILS_H_ */
