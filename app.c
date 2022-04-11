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
	order_t *target;

	if ( slot < ot->total_record_count && ot->used_slots[slot] < UINT64_MAX) {
		target = &(((order_t *)ot->data)[slot]);
		if ( o != NULL )
			memcpy(o, target, sizeof(order_t));
		bzero(target, sizeof(order_t));

		ot->used_slots[slot] = UINT64_MAX;
		ot->free_record_slot++;
		ot->free_slots[ot->free_record_slot] = slot;
		rv = true;
	}

	return rv;
}

void * read_order_record(table_t *ot, uint64_t slot) {
	order_t *o = NULL;
	if ( slot < ot->total_record_count && ot->used_slots[slot] < UINT64_MAX) {
		o = &(((order_t *)ot->data)[slot]);
	}
	return o;
}

uint64_t add_order(table_t *ot, index_t *idx, order_t *o) {
	uint64_t rec_num = UINT64_MAX;
	idxordkey_t ordkey;
	bzero(&ordkey, sizeof(idxordkey_t));
	strcpy(ordkey.orderid, o->orderid);
	ordkey.record = (void *)-1;

	idxnode_t *idx_node = find_node(idx, &idx->root_node, &ordkey);
	if (find_record(idx, idx_node, &ordkey) != NULL ) {
		return rec_num;
	}

	rec_num = add_order_record(ot, o);
	ordkey.record = (void *)rec_num;
	add_index_value(idx, idx_node, &ordkey);

	return rec_num;
}

uint64_t del_order(table_t *ot, index_t *idx, order_t *o) {
	uint64_t rec_num = UINT64_MAX;

	idxordkey_t ordkey, *ov;
	bzero(&ordkey, sizeof(idxordkey_t));
	strcpy(ordkey.orderid, o->orderid);
	ordkey.record = (void *)-1;

	if ( (ov = (idxordkey_t *)find_record(idx, &idx->root_node, &ordkey)) == NULL )
		return rec_num;

	rec_num = (uint64_t)((*idx->get_key_value)(ov));
	delete_order_record(ot, rec_num, NULL);
	remove_index_value(idx, &idx->root_node, ov);

	return rec_num;
}

bool find_order(table_t *ot, index_t *idx, order_t *o) {
	idxordkey_t k, *kp;
	bool rv = false;
	bzero(&k, sizeof(k));
	strcpy(&k.orderid, o->orderid);
	k.record = (void *)-1;
	if ((kp = find_record(idx, &idx->root_node, &k)) != NULL) {
		uint64_t slot = (uint64_t)((*idx->get_key_value)(kp));
		order_t *fo = read_order_record(ot, slot);
		memcpy(o, fo, sizeof(order_t));
		rv = true;
	}
	return rv;
}

uint64_t find_order_slot(table_t *ot, index_t *idx, order_t *o) {
	uint64_t rv = UINT64_MAX;

	idxordkey_t k, *kp;
	bzero(&k, sizeof(k));
	strcpy(&k.orderid, o->orderid);
	k.record = (void *)-1;

	if ((kp = find_record(idx, &idx->root_node, &k)) != NULL)
		rv = (uint64_t)((*idx->get_key_value)(kp));

	return rv;
}

void init_order_id_index(index_t *idx) {
	bzero(idx, sizeof(index_t));

	strcpy(idx->index_name, "order_id_uq");
	idx->record_size = sizeof(idxordkey_t);

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
	int counter = 0;
	table_t orders_table;
	table_t *ot;

	init_order_id_index(&i);
	read_index_from_file(&i);

	bzero(&orders_table, sizeof(table_t));
	orders_table.header_size = sizeof(table_t);
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
	o.cost = 29697;
	o.direction = 'S';

	uint64_t sn = add_order(ot, &i, &o);
	printf("Record added to slot %" PRIu64 "\n", sn);
	//uint64_t sn = 0;
	order_t *rv = read_order_record(ot, sn);

	printf("Order read is %s (%s)\n", rv->orderid, rv->productid);
	bzero(&o, sizeof(o));
	strcpy(o.orderid, "OR0000000000000000000002\0");
	strcpy(o.productid, "IBM\0");
	o.quantity = 100;
	o.cost = 12773;
	o.direction = 'B';

	add_order(ot, &i, &o);

	bzero(&o, sizeof(o));
	strcpy(o.orderid, "OR0000000000000000000001\0");
	del_order(ot, &i, &o);

	counter = 0;
	print_tree(&i, &i.root_node, &counter);
	close_table(ot);
	write_file_from_index(&i);

	release_tree(&i, &i.root_node);
	exit(EXIT_SUCCESS);
}
