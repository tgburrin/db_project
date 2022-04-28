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

#include <zlib.h>

#include <time.h>

#include "server_tools.h"
#include "table_tools.h"
#include "index_tools.h"
#include "journal_tools.h"

#define NUM_SUBSCRIPTIONS 15000000
//#define NUM_SUBSCRIPTIONS 1000
//#define NUM_SUBSCRIPTIONS 30

#define SUBSCRIPTION_ID_LENTH 18
#define CUSTOMER_ID_LENTH 18

typedef struct IndexSubIdKey {
	// requires -fms-extensions option for gcc
	struct IndexKey;
	char subscription_id[SUBSCRIPTION_ID_LENTH + 1];
} idxsubidkey_t;

typedef struct IndexCustIdKey {
	// requires -fms-extensions option for gcc
	struct IndexKey;
	char customer_id[CUSTOMER_ID_LENTH + 1];
} idxcustidkey_t;

typedef struct Subscription {
	char subscription_id[SUBSCRIPTION_ID_LENTH + 1];
	char customer_id[CUSTOMER_ID_LENTH + 1];
	char project_id[12];
	bool is_active;
	char product_type[16];
	char plan_id[128];
	char deferred_plan_id[128];
	char currency[4];
	uint32_t plan_price;
	uint16_t quantity;
	struct timespec term_start;
	struct timespec term_end;
	bool autorenew;
	struct timespec canceled_at;
	char status[24];
	char external_reference[96];
	char subscription_lifecycle[16];
	char churn_type[16];
} subscription_t;

bool admin_command (cJSON *obj, char *err, size_t errsz);

void print_subscription_key(void *vk, char *dst);
void print_customer_key(void *vk, char *dst);

int compare_subscription_id (void *a, void *b);
int compare_customer_id (void *a, void *b);

void * create_subid_key(void);
void * create_custid_key(void);

void * copy_subid_key(void *inkey, void *outkey);
void * copy_custid_key(void *inkey, void *outkey);

void set_subid_key_value(void *k, uint64_t v);
void set_custid_key_value(void *k, uint64_t v);

uint64_t get_subid_key_value(void *k);
uint64_t get_custid_key_value(void *k);

struct Server *app_server = NULL;

bool admin_command (cJSON *obj, char *err, size_t errsz) {
	bool rv = false;
	char *operation = NULL;
	char *action = NULL;

	cJSON *k = cJSON_GetObjectItemCaseSensitive(obj, "operation");
	if (!cJSON_IsString(k) || ((operation = k->valuestring) == NULL))
		return rv;

	k = cJSON_GetObjectItemCaseSensitive(obj, "data");
	if (!cJSON_IsObject(k))
		return rv;

	k = cJSON_GetObjectItemCaseSensitive(k, "action");
	if (!cJSON_IsString(k) || ((action = k->valuestring) == NULL))
		return rv;

	printf("Action: %s\nOperation: %s\n", action, operation);
	if ( strcmp(operation, "c") == 0 && strcmp(action, "shutdown") == 0 ) {
		app_server->running = false;
	}

	return rv;
}

void print_subscription_key(void *vk, char *dst) {
	sprintf(dst, "%s (%" PRIu64 ")", ((idxsubidkey_t *)vk)->subscription_id, ((idxsubidkey_t *)vk)->record);
}
void print_customer_key(void *vk, char *dst) {
	sprintf(dst, "%s (%" PRIu64 ")", ((idxcustidkey_t *)vk)->customer_id, ((idxcustidkey_t *)vk)->record);
}

int compare_subscription_id (void *va, void *vb) {
	idxsubidkey_t *a = (idxsubidkey_t *)va;
	idxsubidkey_t *b = (idxsubidkey_t *)vb;

	int rv = 0;

	// if the order ids aren't equal return immediately
	if ( (rv = strncmp(a->subscription_id, b->subscription_id, SUBSCRIPTION_ID_LENTH)) != 0 )
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

int compare_customer_id (void *va, void *vb) {
	idxcustidkey_t *a = (idxcustidkey_t *)va;
	idxcustidkey_t *b = (idxcustidkey_t *)vb;
	//printf("%s vs %s\n", a->orderid, b->orderid);
	int rv = 0;

	// if the order ids aren't equal return immediately
	if ( (rv = strncmp(a->customer_id, b->customer_id, CUSTOMER_ID_LENTH)) != 0 )
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

void * create_subid_key(void) {
	idxsubidkey_t *newkey = malloc(sizeof(idxsubidkey_t));
	bzero(newkey, sizeof(idxsubidkey_t));
	return (void *)newkey;
}

void * create_custid_key(void) {
	idxcustidkey_t *newkey = malloc(sizeof(idxcustidkey_t));
	bzero(newkey, sizeof(idxcustidkey_t));
	return (void *)newkey;
}

void *create_subid_key_from_record(void *rec) {
	subscription_t *s = (subscription_t *)rec;
	idxsubidkey_t *newkey = malloc(sizeof(idxsubidkey_t));
	bzero(newkey, sizeof(idxsubidkey_t));
	strcpy(newkey->subscription_id, s->subscription_id);

	return (void *)newkey;
}

void *create_custid_key_from_record(void *rec) {
	subscription_t *s = (subscription_t *)rec;
	idxcustidkey_t *newkey = malloc(sizeof(idxcustidkey_t));
	bzero(newkey, sizeof(idxcustidkey_t));
	strcpy(newkey->customer_id, s->customer_id);
	return (void *)newkey;
}

void * copy_subid_key(void *inkey, void *outkey) {
	memcpy(outkey, inkey, sizeof(idxsubidkey_t));
	((idxsubidkey_t *)outkey)->childnode = NULL;
	return outkey;

}

void * copy_custid_key(void *inkey, void *outkey) {
	memcpy(outkey, inkey, sizeof(idxcustidkey_t));
	((idxcustidkey_t *)outkey)->childnode = NULL;
	return outkey;

}

void set_subid_key_value(void *k, uint64_t v) {
	((idxsubidkey_t *)k)->record = v;
}

void set_custid_key_value(void *k, uint64_t v) {
	((idxcustidkey_t *)k)->record = v;
}

uint64_t get_subid_key_value(void *k) {
	return ((idxsubidkey_t *)k)->record;
}

uint64_t get_custid_key_value(void *k) {
	return ((idxcustidkey_t *)k)->record;
}

uint64_t add_subscription_record(table_t *tbl, void *newrec) {
	subscription_t *sr = 0;
	uint64_t slot = UINT64_MAX;
	uint64_t cs = tbl->free_record_slot;

	if ( cs < tbl->total_record_count && cs >= 0 ) {
		//printf("Copying record to slot %" PRIu64 " in the table\n", tbl->free_slots[cs]);

		slot = tbl->free_slots[cs];

		sr = &((subscription_t *)tbl->data)[slot];
		memcpy(sr, newrec, sizeof(subscription_t));

		tbl->used_slots[slot] = cs;
		tbl->free_slots[cs] = tbl->total_record_count;
		tbl->free_record_slot = cs == 0 ? UINT64_MAX : cs - 1;
	}
	return slot;
}

bool delete_subscription_record(table_t *tbl, uint64_t slot, void *delrec) {
	bool rv = false;
	subscription_t *target;

	if ( slot < tbl->total_record_count && tbl->used_slots[slot] < UINT64_MAX) {
		target = &(((subscription_t *)tbl->data)[slot]);
		if ( delrec != NULL )
			memcpy(delrec, target, sizeof(subscription_t));
		bzero(target, sizeof(subscription_t));

		tbl->used_slots[slot] = UINT64_MAX;
		tbl->free_record_slot++;
		tbl->free_slots[tbl->free_record_slot] = slot;
		rv = true;
	}

	return rv;
}

void * read_subscription_record(table_t *tbl, uint64_t slot) {
	subscription_t *rec = NULL;
	if ( slot < tbl->total_record_count && tbl->used_slots[slot] < UINT64_MAX) {
		rec = &(((subscription_t *)tbl->data)[slot]);
	}
	return rec;
}

uint64_t add_subscription(
		table_t *tbl,
		index_t *subidx,
		index_t *cusidx,
		subscription_t *subrec) {

	//printf("Adding subscription %s (%s) to the table\n", subrec->subscription_id, subrec->customer_id);
	uint64_t rec_num = UINT64_MAX;
	idxsubidkey_t subkey;
	bzero(&subkey, sizeof(idxsubidkey_t));
	strcpy(subkey.subscription_id, subrec->subscription_id);
	subkey.record = UINT64_MAX;

	//printf("Finding node where %s is or belongs\n", subkey.subscription_id);
	idxnode_t *idx_node = find_node(subidx, &subidx->root_node, &subkey);
	if (find_record(subidx, idx_node, &subkey) != NULL ) {
		return rec_num;
	}

	rec_num = (*tbl->add_record)(tbl, subrec);
	//printf("Allocated record %"PRIu64"to table\n", rec_num);
	subkey.record = rec_num;

	char keyrec[64];
	(*subidx->print_key)(&subkey, keyrec);
	//printf("Adding index key %s\n", keyrec);
	add_index_value(subidx, idx_node, &subkey);

	idxcustidkey_t custkey;
	bzero(&custkey, sizeof(idxcustidkey_t));
	strcpy(custkey.customer_id, subrec->customer_id);
	custkey.record = rec_num;

	(*cusidx->print_key)(&custkey, keyrec);
	//printf("Adding index key %s\n", keyrec);
	add_index_value(cusidx, &cusidx->root_node, &custkey);

	return rec_num;
}

uint64_t del_subscription(
		table_t *tbl,
		index_t *subidx,
		index_t *cusidx,
		subscription_t *subrec) {
	uint64_t rec_num = UINT64_MAX;

	idxsubidkey_t subkey, *sk;
	bzero(&subkey, sizeof(idxsubidkey_t));
	strcpy(subkey.subscription_id, subrec->subscription_id);
	subkey.record = UINT64_MAX;

	if ( (sk = (idxsubidkey_t *)find_record(subidx, &subidx->root_node, &subkey)) == NULL )
		return rec_num;

	subscription_t dr;
	bzero(&dr, sizeof(subscription_t));

	rec_num = (uint64_t)((*subidx->get_key_value)(sk));

	(*tbl->delete_record)(tbl, rec_num, &dr);

	idxcustidkey_t custkey;
	bzero(&custkey, sizeof(idxcustidkey_t));
	strcpy(custkey.customer_id, dr.customer_id);
	custkey.record = rec_num;

	remove_index_value(subidx, &subidx->root_node, sk);
	remove_index_value(cusidx, &cusidx->root_node, &custkey);

	return rec_num;
}

bool find_subscription_by_id(table_t *tbl, index_t *subidx, char *subscription_id, subscription_t *subrec) {
	idxsubidkey_t k, *kp;
	bool rv = false;
	bzero(&k, sizeof(k));
	strcpy(k.subscription_id, subscription_id);
	k.record = UINT64_MAX;

	if ((kp = find_record(subidx, &subidx->root_node, &k)) != NULL) {
		printf("Subscription %s found\n", subscription_id);
		uint64_t slot = (uint64_t)((*subidx->get_key_value)(kp));
		subscription_t *fs = (*tbl->read_record)(tbl, slot);
		memcpy(subrec, fs, sizeof(subscription_t));
		rv = true;
	} else {
		printf("Subscription %s not found\n", subscription_id);
	}
	return rv;
}

uint64_t find_subscription_slot(table_t *tbl, index_t *subidx, subscription_t *subrec) {
	uint64_t rv = UINT64_MAX;

	idxsubidkey_t k, *kp;
	bzero(&k, sizeof(k));
	strcpy(k.subscription_id, subrec->subscription_id);
	k.record = UINT64_MAX;

	if ((kp = find_record(subidx, &subidx->root_node, &k)) != NULL)
		rv = (uint64_t)((*subidx->get_key_value)(kp));

	return rv;
}

bool subscription_txn_handler(
		journal_t *j,
		table_t *tbl,
		index_t **idxs,
		uint8_t idxcnt,
		char action,
		subscription_t *subrec,
		char *keyname
	) {

	char msg[128];

	bool rv = false;
	index_t *uq_idx = NULL;
	journal_record_t jr;

	bzero(&jr, sizeof(journal_record_t));
	jr.msgsz = sizeof(journal_record_t) + sizeof(subscription_t);
	strcpy(jr.objname, tbl->table_name);
	jr.objsz = sizeof(subscription_t);
	jr.objdata = (void *)subrec;

	//printf("Operation %c for sub %s/%s\n", action, subrec->subscription_id, subrec->customer_id);
	//printf("Table %s\n", tbl->table_name);
	for(int i=0; i < idxcnt; i++) {
		//printf("Index %s%s\n", idxs[i]->index_name, idxs[i]->is_unique ? " (unique)" : "");
		if ( keyname != NULL && strcmp(keyname, idxs[i]->index_name) == 0 && idxs[i]->is_unique )
			uq_idx = idxs[i];
		else if ( keyname == NULL && idxs[i]->is_unique && uq_idx == NULL )
			uq_idx = idxs[i];
	}

	if ( uq_idx == NULL ) {
		fprintf(stderr, "Unique key could not be identified\n");
		return rv;
	} else {
		strcpy(jr.objkey, uq_idx->index_name);
	}


	void *key = NULL;
	if ( uq_idx != NULL ) {
		key = (*uq_idx->create_record_key)((void *)subrec);
		((indexkey_t *)key)->record = UINT64_MAX;
	} else {
		strcpy(jr.objkey, uq_idx->index_name);
	}

	switch (action) {
		case 'i':
			void *k = NULL;
			jr.objtype = action;
			bool txn_valid = false;

			if ( uq_idx != NULL ) {
				if ( (k = find_record(uq_idx, &uq_idx->root_node, key)) != NULL ) {
					(*uq_idx->print_key)(k, msg);
					fprintf(stderr, "ERR: Duplicate record %s\n", msg);
				} else
					txn_valid = true;
			} else
				txn_valid = true;

			if ( txn_valid ) {
				uint64_t recnum = UINT64_MAX;
				if ( (recnum = add_subscription_record(tbl, subrec)) < UINT64_MAX ) {

					for(int i=0; i < idxcnt; i++) {
						k = (*idxs[i]->create_record_key)((void *)subrec);
						(*idxs[i]->set_key_value)(k, recnum);
						add_index_value(idxs[i], &idxs[i]->root_node, k);
						free(k);
						k = NULL;
					}
					write_journal_record(j, &jr);

					rv = true;
				} else {
					if ( tbl->free_record_slot == UINT64_MAX)
						fprintf(stderr, "ERR: writing to table %s, table is full\n", tbl->table_name);
					else
						fprintf(stderr, "ERR: writing to table %s\n", tbl->table_name);
				}
			}
			break;
		case 'd':
			jr.objtype = action;
			break;
		case 'u':
			jr.objtype = action;
			break;
		default:
			fprintf(stderr, "Unknown txn action %c\n", action);
	}

	if( uq_idx != NULL )
		free(key);
	return rv;
}

void load_subs_from_file(
			char *filename,
			table_t *tbl,
			index_t **idxs,
			uint8_t idxcnt,
			journal_t *j
		) {
	uint64_t rec_count = 0;
	char line[1024], *l, *field, *t = NULL, *delim = "\t"; // tab
	bzero(line, sizeof(line));
	uint8_t field_num = 0;
	subscription_t addsub;

	journal_sync_off(j);

	gzFile gzfd = gzopen(filename, "r");
	while ( (l = gzgets(gzfd, line, sizeof(line))) != NULL ) {
		// chop the newline off of the end of the line
		if (l[strlen(l) - 1] == '\n')
			l[strlen(l) - 1] = '\0';

		bzero(&addsub, sizeof(subscription_t));
		field_num = 0;
		field = strtok_r(l, delim, &t);
		do {
			switch (field_num) {
				case 0: // subscription_id
					strcpy(addsub.subscription_id, field);
					break;
				case 1: // valid_from
					break;
				case 2: // customer_id
					strcpy(addsub.customer_id, field);
					break;
				case 3: // project_id
					break;
				case 4: // is_active
					addsub.is_active = strcmp(field, "t") == 0 ? true : false;
					break;
				case 5: // product
					strcpy(addsub.product_type, field);
					break;
				case 6: // plan
					if ( strcmp(field, "\\N") != 0 )
						strcpy(addsub.plan_id, field);
					break;
				case 7: // deferred plan
					if ( strcmp(field, "\\N") != 0 )
						strcpy(addsub.deferred_plan_id, field);
					break;
				case 8: // currency
					if ( strcmp(field, "\\N") != 0 )
						strcpy(addsub.currency, field);
					break;
				case 9: // plan_price
					addsub.plan_price = atoi(field);
					break;
				case 10: // quantity
					addsub.quantity = atoi(field);
					break;
				case 11: // term_start
				case 12: // term_end
					break;
				case 13: // autorenew - this can have nulls
					addsub.autorenew = strcmp(field, "t") == 0 ? true : false;
					break;
				case 14: // canceled at
					break;
				case 15: // status
					if ( strcmp(field, "\\N") != 0 )
						strcpy(addsub.status, field);
					break;
				case 16: // external ref
					if ( strcmp(field, "\\N") != 0 )
						strcpy(addsub.external_reference, field);
					break;
				case 17: // lifecycle
					if ( strcmp(field, "\\N") != 0 )
						strcpy(addsub.subscription_lifecycle, field);
					break;
				case 18: // churn
					if ( strcmp(field, "\\N") != 0 )
						strcpy(addsub.churn_type, field);
					break;
				//default:
				//	printf("%"PRIu8": %s\n", field_num, field);
			}

			field_num++;
			field = strtok_r(NULL, delim, &t);
		} while (field != NULL);
		bzero(line, sizeof(line));

		//printf("Adding sub %s...", addsub.subscription_id);
		subscription_txn_handler(j, tbl, idxs, 2, 'i', &addsub, NULL);
		rec_count++;
		//printf("successfully added\n");

		if ( rec_count % 10000 == 0 )
			printf("Added %" PRIu64 " records to the table\n", rec_count);
	}
	gzclose(gzfd);
	printf("Added %" PRIu64 " records to the table\n", rec_count);
	journal_sync_on(j);
}

int main (int argc, char **argv) {
	// https://stackoverflow.com/questions/6187908/is-it-possible-to-dynamically-define-a-struct-in-c

	//char *timestr = "2022-04-14 05:59:57.263+00";
	//char *timestr = "2022-04-14";
	//struct timespec out_time;
	struct timespec start_tm, end_tm, now;
	/*
	parse_timestamp(timestr, &out_time);

	clock_gettime(CLOCK_REALTIME, &now);
	char out_time_str[31];
	format_timestamp(&now, out_time_str);
	printf("Out time: %s\n", out_time_str);
	*/

	subscription_t s;

	journal_t jnl;
	bzero(&jnl, sizeof(journal_t));

	float time_diff;
	table_t subs_table;
	table_t *st;

	index_t subid_idx;
	index_t custid_idx;

	index_t *index_list[2];
	index_list[0] = &subid_idx;
	index_list[1] = &custid_idx;

	bzero(&subs_table, sizeof(table_t));
	subs_table.header_size = sizeof(table_t);
	subs_table.record_size = sizeof(subscription_t);
	subs_table.total_record_count = NUM_SUBSCRIPTIONS;
	strcpy(subs_table.table_name, "subscriptions");
	subs_table.add_record = &add_subscription_record;;
	subs_table.delete_record = &delete_subscription_record;
	subs_table.read_record = &read_subscription_record;

	bzero(&subid_idx, sizeof(index_t));
	strcpy(subid_idx.index_name, "subscription_id_idx_uq");
	subid_idx.record_size = sizeof(idxsubidkey_t);
	subid_idx.is_unique = true;
	init_index_node(&subid_idx.root_node);
	subid_idx.compare_key = &compare_subscription_id;
	subid_idx.create_key = &create_subid_key;
	subid_idx.create_record_key = &create_subid_key_from_record;
	subid_idx.copy_key = &copy_subid_key;
	subid_idx.set_key_value = &set_subid_key_value;
	subid_idx.get_key_value = &get_subid_key_value;
	subid_idx.print_key = &print_subscription_key;

	bzero(&custid_idx, sizeof(index_t));
	strcpy(custid_idx.index_name, "customer_id_idx");
	custid_idx.record_size = sizeof(idxcustidkey_t);
	custid_idx.is_unique = false;
	init_index_node(&custid_idx.root_node);
	custid_idx.compare_key = &compare_customer_id;
	custid_idx.create_key = &create_custid_key;
	custid_idx.create_record_key = &create_custid_key_from_record;
	custid_idx.copy_key = &copy_custid_key;
	custid_idx.set_key_value = &set_custid_key_value;
	custid_idx.get_key_value = &get_custid_key_value;
	custid_idx.print_key = &print_subscription_key;


	read_index_from_file(&subid_idx);
	read_index_from_file(&custid_idx);
	printf("Indexes loaded\n");

	open_table(&subs_table, &st);

	new_journal(&jnl);

	printf("Table opened\n");

	//del_subscription(st, &subid_idx, &custid_idx, &s);

	if ( argc == 2 )
		load_subs_from_file(argv[1], st, index_list, 2, &jnl);

	app_server = malloc(sizeof(struct Server));
	bzero(&app_server, sizeof(struct Server));

	message_handler_list_t handlers;
	bzero(&handlers, sizeof(message_handler_list_t));

	message_handler_t admin_handler;
	bzero(&admin_handler, sizeof(message_handler_t));

	strcpy(admin_handler.handler_name, "admin_functions");
	admin_handler.handler = &admin_command;

	handlers.num_handlers++;
	handlers.handlers = malloc(sizeof(void *) * handlers.num_handlers);
	handlers.handlers[0] = &admin_handler;

	start_application(app_server, &handlers);

	int counter;
	printf("Subscription Index:\n");
	counter = 0;
	print_tree_totals(&subid_idx, &subid_idx.root_node, &counter);
	counter = 0;
	//print_tree(&subid_idx, &subid_idx.root_node, &counter);
	printf("Customer Id Index:\n");
	counter = 0;
	print_tree_totals(&custid_idx, &custid_idx.root_node, &counter);
	counter = 0;
	//print_tree(&custid_idx, &custid_idx.root_node, &counter);

	idxcustidkey_t c;
	bzero(&c, sizeof(idxcustidkey_t));
	strcpy(c.customer_id, "cus_DLr1bFxFbQzbdS");
	c.record = UINT64_MAX;

	bzero(&s, sizeof(subscription_t));

	idxsubidkey_t k;
	bzero(&k, sizeof(idxsubidkey_t));
	//strcpy(k.subscription_id, "su_0jahiubOMk0cnB");
	strcpy(k.subscription_id, "sub_PcHdKYt3rmiBNl");
	k.record = UINT64_MAX;

	print_index_scan_lookup(&subid_idx, &k);

	clock_gettime(CLOCK_REALTIME, &start_tm);
	//if ( find_subscription_by_id(st, &subid_idx, "su_0jahiubOMk0cnB", &s) ) {
	if ( find_subscription_by_id(st, &subid_idx, "su_1PNT9d6ahvCu8Z", &s) ) {
		clock_gettime(CLOCK_REALTIME, &end_tm);
		time_diff = end_tm.tv_sec - start_tm.tv_sec;
		time_diff += (end_tm.tv_nsec / 1000000000.0) - (start_tm.tv_nsec / 1000000000.0);
		printf("Lookup was %fus\n", time_diff * 1000000.0);

		printf("subscription_id: %s\n", s.subscription_id);
		printf("customer_id: %s\n", s.customer_id);
		printf("product_type: %s\n", s.product_type);
		printf("plan_id: %s\n", s.plan_id);
		printf("currency: %s\n", s.currency);
		printf("plan_price: %d\n", s.plan_price);
		printf("quantity: %d\n", s.quantity);
		printf("status: %s\n", s.status);
		printf("external_reference: %s\n", s.external_reference);
		printf("lifecycle: %s\n", s.subscription_lifecycle);
		printf("churn_type: %s\n", s.churn_type);
	}

	bzero(&s, sizeof(subscription_t));
	clock_gettime(CLOCK_REALTIME, &start_tm);
	if ( find_subscription_by_id(st, &subid_idx, "sub_Gaeu53lByWsOWk", &s) ) {
		clock_gettime(CLOCK_REALTIME, &end_tm);
		time_diff = end_tm.tv_sec - start_tm.tv_sec;
		time_diff += (end_tm.tv_nsec / 1000000000.0) - (start_tm.tv_nsec / 1000000000.0);
		printf("Lookup was %fus\n", time_diff * 1000000.0);

		printf("subscription_id: %s\n", s.subscription_id);
		printf("customer_id: %s\n", s.customer_id);
		printf("product_type: %s\n", s.product_type);
		printf("plan_id: %s\n", s.plan_id);
		printf("currency: %s\n", s.currency);
		printf("plan_price: %d\n", s.plan_price);
		printf("quantity: %d\n", s.quantity);
		printf("status: %s\n", s.status);
		printf("external_reference: %s\n", s.external_reference);
		printf("lifecycle: %s\n", s.subscription_lifecycle);
		printf("churn_type: %s\n", s.churn_type);
	}

	bzero(&s, sizeof(subscription_t));
	clock_gettime(CLOCK_REALTIME, &start_tm);
	if ( find_subscription_by_id(st, &subid_idx, "su_2gjAMOQfjVI58E", &s) ) {
		clock_gettime(CLOCK_REALTIME, &end_tm);
		time_diff = end_tm.tv_sec - start_tm.tv_sec;
		time_diff += (end_tm.tv_nsec / 1000000000.0) - (start_tm.tv_nsec / 1000000000.0);
		printf("Lookup was %fus\n", time_diff * 1000000.0);

		printf("subscription_id: %s\n", s.subscription_id);
		printf("customer_id: %s\n", s.customer_id);
		printf("product_type: %s\n", s.product_type);
		printf("plan_id: %s\n", s.plan_id);
		printf("currency: %s\n", s.currency);
		printf("plan_price: %d\n", s.plan_price);
		printf("quantity: %d\n", s.quantity);
		printf("status: %s\n", s.status);
	}

	printf("Closing table\n");
	close_table(st);

	printf("Flushing indexes\n");
	for(counter = 0; counter < 2; counter++)
		write_file_from_index(index_list[counter]);

	printf("Releasing indexes\n");
	for(counter = 0; counter < 2; counter++)
		release_tree(index_list[counter], &(index_list[counter])->root_node);

	close_journal(&jnl);

	printf("Done\n");
	exit(EXIT_SUCCESS);
}
