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

#define ORDER_ID_SIZE 24
//#define NUM_ORDERS 50000000
#define NUM_ORDERS 50

typedef struct IndexOrderKey {
	// requires -fms-extensions option for gcc
	struct IndexKey;
	char orderid[ORDER_ID_SIZE + 1];
} idxordkey_t;

typedef struct Order {
	char orderid[ORDER_ID_SIZE + 1];
	char productid[25];
	int quantity;
	int cost;
	char direction;
} order_t;

void print_order_key(void *vk, char *dst);
int compare_order_id (void *a, void *b);
void * create_order_key(void);
void * copy_order_key(void *inkey, void *outkey);
void set_order_key_value(void *k, void *v);
void * get_order_key_value(void *k);

void print_order_key(void *vk, char *dst) {
	sprintf(dst, "%s (%p)", ((idxordkey_t *)vk)->orderid, (void *)((idxordkey_t *)vk)->record);
}

int compare_order_id (void *va, void *vb) {
	idxordkey_t *a = (idxordkey_t *)va;
	idxordkey_t *b = (idxordkey_t *)vb;
	//printf("%s vs %s\n", a->orderid, b->orderid);
	int rv = 0;

	// if the order ids aren't equal return immediately
	if ( (rv = strncmp(a->orderid, b->orderid, ORDER_ID_SIZE)) != 0 )
		return rv;

	// if either record id is a max value pointer ignore the comparison further
	if ( a->record == (void *)-1 || b->record == (void *)-1 )
		return rv;

	if ( a->record == b->record )
		return 0;
	else if ( a->record < b->record )
		return -1;
	else
		return 1;
}

void * create_order_key(void) {
	idxordkey_t *newkey = malloc(sizeof(idxordkey_t));
	bzero(newkey, sizeof(idxordkey_t));
	return (void *)newkey;
}

void * copy_order_key(void *inkey, void *outkey) {
	void *rec_value = (void *)((idxordkey_t *)outkey)->record;
	memcpy(outkey, inkey, sizeof(idxordkey_t));
	((idxordkey_t *)outkey)->record = rec_value;
	return outkey;
}

void set_order_key_value(void *k, void *v) {
	((idxordkey_t *)k)->record = v;
}

void * get_order_key_value(void *k) {
	return ((idxordkey_t *)k)->record;
}

uint64_t add_order_record(table_t *ot, void *o) {
	order_t *s = 0;
	uint64_t slot = UINT64_MAX;
	uint64_t cs = ot->free_record_slot;

	if ( cs < ot->total_record_count && cs >= 0 ) {
		printf("Copying record to slot %" PRIu64 " in the table\n", ot->free_slots[cs]);

		slot = ot->free_slots[cs];

		s = &((order_t *)ot->data)[slot];
		memcpy(s, o, sizeof(order_t));

		ot->used_slots[slot] = cs;
		ot->free_slots[cs] = ot->total_record_count;
		ot->free_record_slot = cs == 0 ? UINT64_MAX : cs - 1;
	}
	return slot;
}

bool delete_order_record(table_t *ot, uint64_t slot, void *o) {
	bool rv = false;

	return rv;
}

void * read_order_record(table_t *ot, uint64_t slot) {
	order_t *o = NULL;
	if ( slot < ot->total_record_count && ot->used_slots[slot] < UINT64_MAX) {
		o = &(((order_t *)ot->data)[slot])	;
	}
	return o;
}

uint64_t add_order(table_t *ot, index_t *idx, order_t *o) {
	uint64_t rec_num = add_order_record(ot, o);

	idxordkey_t ordkey;
	bzero(&ordkey, sizeof(idxordkey_t));
	strcpy(ordkey.orderid, o->orderid);
	ordkey.record = (void *)rec_num;
	add_index_value(idx, &idx->root_node, &ordkey);

	return rec_num;
}

void init_order_id_index(index_t *idx) {
	idx->is_unique = true;
	init_index_node(&idx->root_node);
	idx->compare_key = &compare_order_id;
	idx->create_key = &create_order_key;
	idx->copy_key = &copy_order_key;

	idx->set_key_value = &set_order_key_value;
	idx->get_key_value = &get_order_key_value;

	idx->print_key = &print_order_key;
}

int main (int argc, char **argv) {
	index_t i;
	init_order_id_index(&i);

	table_t orders_table;
	table_t *ot;
	bzero(&orders_table, sizeof(table_t));
	orders_table.record_size = sizeof(order_t);
	orders_table.total_record_count = NUM_ORDERS;
	strcpy(orders_table.table_name, "orders");
	orders_table.add_record = &add_order_record;
	orders_table.delete_record = &delete_order_record;
	orders_table.read_record = &read_order_record;

	open_table(&orders_table, &ot);
	printf("Table %s opened\n", ot->table_name);

	order_t o;
	bzero(&o, sizeof(o));

	strcpy(o.orderid, "OR0000000000000000000001\0");
	strcpy(o.productid, "MSFT\0");
	o.quantity = 100;
	o.cost = 14000;
	o.direction = 'S';

	uint64_t sn = add_order(ot, &i, &o);
	printf("Record added to slot %" PRIu64 "\n", sn);
	//uint64_t sn = 0;
	order_t *rv = read_order_record(ot, sn);
	printf("Order read is %s (%s)\n", rv->orderid, rv->productid);
	close_table(ot);

	exit(EXIT_SUCCESS);
}
