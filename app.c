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

//#define NUM_ORDERS 50000000
#define NUM_ORDERS 50

typedef struct IndexOrderKey {
	// requires -fms-extensions option for gcc
	struct IndexKey;
	char orderid[ORDER_ID_SIZE + 1];
} idxordkey_t;

typedef struct Order {
	char orderid[25];
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

order_t *add_order(table_t *ot, order_t *o, index_t *idx) {
	order_t *s = 0;
	uint64_t cs = ot->free_record_slot;
	uint64_t rec_num = 0;

	if ( cs < ot->total_record_count && cs >= 0 ) {
		printf("Copying record to slot %" PRIu64 " in the table\n", ot->free_slots[cs]);

		rec_num = ot->free_slots[cs];

		s = &ot->data[rec_num];
		memcpy(s, o, sizeof(order_t));

		ot->free_slots[cs] = ot->total_record_count;
		ot->free_record_slot = cs == 0 ? ot->total_record_count : cs - 1;

		idxordkey_t idx_key;
		bzero(&idx_key, sizeof(idxordkey_t));
		strcpy(idx_key.orderid, o->orderid);
		idx_key.record = (void *)rec_num;

		add_index_value(idx, &idx->root_node, &idx_key);
		return s;
	} else {
		return s;
	}
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

void test (void) {
	int rv, fd = 0;
	FILE *fp;
	void *order_file;
	size_t order_file_size;
	size_t order_data_size = sizeof(order_t) * NUM_ORDERS;
	size_t order_table_size = sizeof(table_t) + (sizeof(uint64_t) * NUM_ORDERS * 2);
	index_t id_index;
	int counter;

	table_t *ot;

	order_t o;
	struct stat fc;
	bzero(&o, sizeof(o));

	init_order_id_index(&id_index);

	strcpy(o.orderid, "OR0000000000000000000001\0");
	strcpy(o.productid, "MSFT\0");
	o.quantity = 100;
	o.cost = 14000;
	o.direction = 'S';

	//order_table.free_slots = malloc((sizeof(order_t *) * NUM_ORDERS));
	//bzero(order_table.free_slots, (sizeof(order_t *) * NUM_ORDERS));
	//printf("Order size %ld\nOrder table metadata %ld\n", order_size, order_table_size);

	if ( access("/dev/shm/orders.shm", F_OK) < 0 ) {
		printf("Creating file of %ld bytes\n", order_data_size + order_table_size);
		printf("Creating file of %ld bytes * %d + %ld bytes + %ld bytes  * %d\n", sizeof(order_t), NUM_ORDERS, sizeof(table_t), sizeof(uint64_t), NUM_ORDERS);
		if ( (fd = open("/dev/shm/orders.shm", O_CREAT | O_RDWR, 0640)) < 0 ) {
			fprintf(stderr, "Unable to open file\n");
			fprintf(stderr, "Error: %s\n", strerror(errno));
			exit(EXIT_FAILURE);
		}

		printf("Sizing file...\n");
		ftruncate(fd, order_data_size + order_table_size);

	} else if ( access("/dev/shm/orders.shm", W_OK) == 0 ) {
		if ( (fd = open("/dev/shm/orders.shm", O_RDWR)) < 0 ) {
			fprintf(stderr, "Unable to open file\n");
			fprintf(stderr, "Error: %s\n", strerror(errno));
			exit(EXIT_FAILURE);
		}

	} else {
		exit(EXIT_FAILURE);

	}

	order_file_size = lseek(fd, 0, SEEK_END);
	printf("%ld byte file opened\n", order_file_size);

	if ( (order_file = mmap(NULL, order_file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)) == MAP_FAILED ) {
		fprintf(stderr, "Unable to map file to memory\n");
		fprintf(stderr, "Error: %s\n", strerror(errno));
		exit(EXIT_FAILURE);

	}

	ot = order_file;

	if ( ot->total_record_count == 0 ) {
		ot->total_record_count = NUM_ORDERS;
		ot->free_record_slot = NUM_ORDERS - 1;
		ot->free_slots = (uint64_t *) (order_file + sizeof(table_t));
		ot->data = (order_t *) (order_file + sizeof(table_t) + (sizeof(uint64_t) * NUM_ORDERS));
		printf("%p is the starting point for data\n", ot->data);

		for(int i = 0; i < ot->total_record_count; i++)
			ot->free_slots[ot->free_record_slot - i] = i;

	} else if ( ot->total_record_count != NUM_ORDERS ) {
	} else {
		ot->free_slots = (uint64_t *) (order_file + sizeof(table_t));
		ot->data = (order_t *) (order_file + sizeof(table_t) + (sizeof(uint64_t) * ot->total_record_count));
	}


	printf("%" PRIu64 " records in the table\n", ot->total_record_count);
	printf("%" PRIu64 " is the current free record\n", ot->free_slots[ot->free_record_slot]);
	printf("%" PRIu64 " is the last free record\n", ot->free_slots[0]);

	//printf("int = %d\nchar = %d\n", sizeof(int), sizeof(char));
	//printf("allocating %ld bytes ( %d * %d )\n", order_size, sizeof(order_t), NUM_ORDERS);
	//printf("done.\n");

	/*
	printf("Order: %s\n", ot->data[0].orderid);
	printf("Product: %s\n", ot->data[0].productid);
	order_t *s = add_order(ot, &o, &id_index);
	*/

	counter = 0;
	print_tree(&id_index, &id_index.root_node, &counter);

	/*
	printf("Order: %s\n", ot->data[0].orderid);
	printf("Product: %s\n", ot->data[0].productid);
	printf("Order: %s\n", s->orderid);
	printf("Product: %s\n", s->productid);
	*/

	//printf("Order: %s\n", ot->data[0].orderid);
	//printf("Product: %s\n", ot->data[0].productid);

	//printf("Creating order 2\n");
	//strcpy(ot->data[0].orderid, "OR0000000000000000000002\0");
	//strcpy(ot->data[0].productid, "IBM\0");
	//ot->data[0].quantity = 100;
	//ot->data[0].cost = 9800;
	//ot->data[0].direction = 'B';

	//printf("Order: %s\n", ot->data[0].orderid);
	//printf("Product: %s\n", ot->data[0].productid);

	//printf("Copying order 1\n");
	//memcpy(&ot->data[1], &o, sizeof(order_t));

	//printf("Order: %s\n", ot->data[1].orderid);
	//printf("Product: %s\n", ot->data[1].productid);

	//printf("Setting direction on slot 3\n");
	//strcpy(ot->data[2].orderid, "orderid\0");
	//strcpy(ot->data[2].productid, "symbol\0");
	//ot->data[2].quantity = 0;
	//ot->data[2].cost = 0;
	//ot->data[2].direction = 'X';

	munmap(order_file, order_file_size);
	close(fd);
}

int main (int argc, char **argv) {
	index_t i;
	init_order_id_index(&i);

	idxordkey_t orders[10];

	for ( int i=0; i<10; i++ )
		bzero(&orders[i], sizeof(idxordkey_t));

	strcpy(orders[0].orderid, "banana");
	orders[0].record = 1;
	strcpy(orders[1].orderid, "mango");
	orders[1].record = 2;
	strcpy(orders[2].orderid, "apple");
	orders[2].record = 3;
	strcpy(orders[3].orderid, "orange");
	orders[3].record = 4;

	strcpy(orders[4].orderid, "pear");
	orders[4].record = 0;

	strcpy(orders[5].orderid, "apple");
	orders[5].record = -1;

	strcpy(orders[6].orderid, "orange");
	orders[6].record = 5;

	if ( !add_index_value(&i, &i.root_node, &orders[0]) )
		printf("Could not add key %s\n", orders[0].orderid);
	if ( !add_index_value(&i, &i.root_node, &orders[1]) )
		printf("Could not add key %s\n", orders[1].orderid);
	if ( !add_index_value(&i, &i.root_node, &orders[2]) )
		printf("Could not add key %s\n", orders[2].orderid);
	if ( !add_index_value(&i, &i.root_node, &orders[3]) )
		printf("Could not add key %s\n", orders[3].orderid);
	if ( !add_index_value(&i, &i.root_node, &orders[6]) )
		printf("Could not add key %s\n", orders[6].orderid);

	int counter = 0;
	print_tree(&i, &i.root_node, &counter);

	find_record(&i, &i.root_node, &orders[4]);
	find_record(&i, &i.root_node, &orders[5]);

	release_tree(&i, &i.root_node);

	table_t orders_table;
	table_t *ot;
	bzero(&orders_table, sizeof(table_t));
	orders_table.record_size = sizeof(order_t);
	orders_table.total_record_count = NUM_ORDERS;
	strcpy(orders_table.table_name, "orders");

	open_table(&orders_table, &ot);
	printf("Table %s opened\n", ot->table_name);
	close_table(ot);

	exit(EXIT_SUCCESS);
}
