/*
 * fruit_index_test.c
 *
 *  Created on: Apr 16, 2022
 *      Author: tgburrin
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <inttypes.h>

#include "table_tools.h"
#include "index_tools.h"

#define FRUITNAME_SZ 32;

void print_fruit_key(void *vk, char *dst);
int compare_fruit_name (void *va, void *vb);
void * create_fruit_key(void);
void release_fruit_key(void *);
void * copy_fruit_key(void *inkey, void *outkey);
void set_fruit_key_value(void *k, uint64_t v);
uint64_t get_fruit_key_value(void *k);

typedef struct IndexFruitKey {
	// requires -fms-extensions option for gcc
	struct IndexKey;
	char *fruitname;
} idxfruitkey_t;

void print_fruit_key(void *vk, char *dst) {
	sprintf(dst, "%s (%" PRIu64 " / %p)", ((idxfruitkey_t *)vk)->fruitname, ((idxfruitkey_t *)vk)->record, ((idxfruitkey_t *)vk)->childnode);
}

int compare_fruit_name (void *va, void *vb) {
	idxfruitkey_t *a = (idxfruitkey_t *)va;
	idxfruitkey_t *b = (idxfruitkey_t *)vb;

	int rv = 0;

	// if the order ids aren't equal return immediately
	if ( (rv = strncmp(a->fruitname, b->fruitname, sizeof(char)*32)) != 0 )
		return rv;

	// if either record id is a max value pointer ignore the comparison further
	if ( a->record == UINT64_MAX || b->record == UINT64_MAX )
		return rv;

	if ( a->record == b->record )
		return 0;
	else if ( a->record < b->record )
		return -1;
	else
		return 1;
}

void * create_fruit_key(void) {
	idxfruitkey_t *newkey = malloc(sizeof(idxfruitkey_t));
	bzero(newkey, sizeof(idxfruitkey_t));
	return (void *)newkey;
}

void release_fruit_key(void *rec) {
	free(((idxfruitkey_t *)rec)->fruitname);
}

void * copy_fruit_key(void *inkey, void *outkey) {
	memcpy(outkey, inkey, sizeof(idxfruitkey_t));
	((idxfruitkey_t *)outkey)->childnode = NULL;
	return outkey;

}

void set_fruit_key_value(void *k, uint64_t v) {
	((idxfruitkey_t *)k)->record = v;
}

uint64_t get_fruit_key_value(void *k) {
	return ((idxfruitkey_t *)k)->record;
}

int index_test (int argc, char **argv) {
	index_t fruit_idx;
	char msg[128];

	bzero(&fruit_idx, sizeof(index_t));
	strcpy(fruit_idx.index_name, "fruit_name_idx");
	fruit_idx.record_size = sizeof(idxfruitkey_t);
	fruit_idx.is_unique = false;
	init_index_node(&fruit_idx.root_node);
	fruit_idx.compare_key = &compare_fruit_name;
	fruit_idx.create_key = &create_fruit_key;
	fruit_idx.release_key = &release_fruit_key;
	fruit_idx.copy_key = &copy_fruit_key;
	fruit_idx.set_key_value = &set_fruit_key_value;
	fruit_idx.get_key_value = &get_fruit_key_value;
	fruit_idx.print_key = &print_fruit_key;

	idxfruitkey_t k[32];
	for(int i=0; i<32; i++)
		bzero(&k[i], sizeof(idxfruitkey_t));

	k[0].fruitname = "grapes";
	k[0].record = 0;
	k[1].fruitname = "cantaloupe";
	k[1].record = 1;
	k[2].fruitname = "banana";
	k[2].record = 2;
	k[2].childnode = NULL;
	k[3].fruitname = "orange";
	k[3].record = 3;
	k[4].fruitname = "strawberry";
	k[4].record = 4;
	k[5].fruitname = "strawberry";
	k[5].record = 5;
	k[6].fruitname = "mango";
	k[6].record = 6;
	k[7].fruitname = "apple";
	k[7].record = 7;
	k[8].fruitname = "watermelon";
	k[8].record = 8;
	k[9].fruitname = "strawberry";
	k[9].record = 9;
	k[10].fruitname = "strawberry";
	k[10].record = 10;
	k[11].fruitname = "strawberry";
	k[11].record = 11;
	k[12].fruitname = "strawberry";
	k[12].record = 12;

	int counter;
	add_index_value(&fruit_idx, &fruit_idx.root_node, &k[0]);
	add_index_value(&fruit_idx, &fruit_idx.root_node, &k[1]);
	add_index_value(&fruit_idx, &fruit_idx.root_node, &k[2]);
	counter = 0;
	print_tree(&fruit_idx, &fruit_idx.root_node, &counter);
	add_index_value(&fruit_idx, &fruit_idx.root_node, &k[3]);
	counter = 0;
	print_tree(&fruit_idx, &fruit_idx.root_node, &counter);
	add_index_value(&fruit_idx, &fruit_idx.root_node, &k[4]);
	add_index_value(&fruit_idx, &fruit_idx.root_node, &k[5]);
	add_index_value(&fruit_idx, &fruit_idx.root_node, &k[6]);
	add_index_value(&fruit_idx, &fruit_idx.root_node, &k[7]);
	add_index_value(&fruit_idx, &fruit_idx.root_node, &k[8]);
	add_index_value(&fruit_idx, &fruit_idx.root_node, &k[9]);
	add_index_value(&fruit_idx, &fruit_idx.root_node, &k[10]);
	add_index_value(&fruit_idx, &fruit_idx.root_node, &k[11]);
	add_index_value(&fruit_idx, &fruit_idx.root_node, &k[12]);

	counter = 0;
	print_tree(&fruit_idx, &fruit_idx.root_node, &counter);

	remove_index_value(&fruit_idx, &fruit_idx.root_node, &k[0]);

	counter = 0;
	print_tree(&fruit_idx, &fruit_idx.root_node, &counter);

	idxfruitkey_t fk, *f;
	bzero(&fk, sizeof(idxfruitkey_t));
	fk.fruitname = "strawberry";
	//fk.record = UINT64_MAX;
	fk.record = 5;

	f = find_record(&fruit_idx, &fruit_idx.root_node, &fk);
	if ( f != NULL ) {
		(*fruit_idx.print_key)(f, msg);
		printf("found key %s\n", msg);
	} else {
		printf("%s not found\n", fk.fruitname);
	}
	return 0;
}
