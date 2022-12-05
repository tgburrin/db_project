/*
 ============================================================================
 Name        : db_server.c
 Author      : Tim Burrington
 Version     :
 Copyright   : 
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <unistd.h>
#include <arpa/inet.h>

#include <zlib.h>

#include <table_tools.h>
#include <index_tools.h>
#include <journal_tools.h>
#include <server_tools.h>
#include <data_dictionary.h>
#include <db_interface.h>
#include <utils.h>

bool admin_command (cJSON *obj, cJSON **resp, uint16_t argc, void **argv, char *err, size_t errsz);
bool subscription_command (cJSON *obj, cJSON **resp, uint16_t argc, void **argv, char *err, size_t errsz);

bool subscription_txn_handler(journal_t *, db_table_t *, char, char *, char *, char *, size_t);

bool admin_command (cJSON *obj, cJSON **resp, uint16_t argc, void **argv, char *err, size_t errsz) {
	char *action = NULL, *operation = NULL;
	cJSON *k = NULL;

	struct Server *app_server = NULL;
	if ( argc >= 1 )
		app_server = (struct Server *)argv[0];

	if ( app_server == NULL )
		return false;

	k = cJSON_GetObjectItemCaseSensitive(obj, "operation");
	if (!cJSON_IsString(k) || ((operation = k->valuestring) == NULL))
		return false;

	k = cJSON_GetObjectItemCaseSensitive(obj, "data");
	if (!cJSON_IsObject(k))
		return false;


	k = cJSON_GetObjectItemCaseSensitive(k, "action");
	if (!cJSON_IsString(k) || ((action = k->valuestring) == NULL))
		return false;

	if ( strcmp(operation, "c") == 0 ) {
		if ( strcmp(action, "shutdown") == 0 ) {
			app_server->running = false;
			cJSON *r = cJSON_CreateObject();
			cJSON_AddStringToObject(r, "message", "server shutting down");
			*resp = r;
		}
	}

	return true;
}


bool subscription_command (cJSON *obj, cJSON **resp, uint16_t argc, void **argv, char *err, size_t errsz) {
	bool rv = false;
	char *operation = NULL, *lookup_index = NULL;
	char op, errmsg[129];

	journal_t *j = (journal_t *)(argv[0]);
	db_table_t *tbl = (db_table_t *)(argv[1]);

	bzero(&errmsg, sizeof(errmsg));
	cJSON *k = cJSON_GetObjectItemCaseSensitive(obj, "operation");
	if (!cJSON_IsString(k) || ((operation = k->valuestring) == NULL))
		return rv;
	else
		operation = k->valuestring;

	if ( strlen(operation) == 1 )
		op = operation[0];

	k = cJSON_GetObjectItemCaseSensitive(obj, "lookup_index");
	if (!cJSON_IsString(k) || ((lookup_index = k->valuestring) == NULL))
		return rv;
	else
		lookup_index = k->valuestring;

	cJSON *data = cJSON_GetObjectItemCaseSensitive(obj, "data");
	if (!cJSON_IsObject(data))
		return rv;

	printf("Operation: %s (%c)\n", operation, op);
	printf("Lookup Index: %s\n", lookup_index);

	cJSON *r = cJSON_CreateObject();
	switch (op) {
		case 'i': ;
			printf("Running insert on %s\n", tbl->table_name);
			for(uint8_t i = 0; i < tbl->num_indexes; i++)
				printf("\tIndex -> %s\n", tbl->indexes[i]->index_name);
			char *subscription_id, *s = new_db_table_record(tbl->schema);

			k = cJSON_GetObjectItemCaseSensitive(data, "subscription_id");
			if ( cJSON_IsString(k) && ((operation = k->valuestring) != NULL)) {
				set_db_table_record_field(tbl->schema, "subscription_id", k->valuestring, s);
				subscription_id = k->valuestring;
			}

			k = cJSON_GetObjectItemCaseSensitive(data, "customer_id");
			if ( cJSON_IsString(k) && ((operation = k->valuestring) != NULL))
				set_db_table_record_field(tbl->schema, "customer_id", k->valuestring, s);

			k = cJSON_GetObjectItemCaseSensitive(data, "product_type");
			if ( cJSON_IsString(k) && ((operation = k->valuestring) != NULL))
				set_db_table_record_field(tbl->schema, "product_type", k->valuestring, s);

			rv = subscription_txn_handler(j, tbl, 'i', s, NULL, errmsg, sizeof(errmsg) - 1);

			if ( rv ) {
				cJSON_AddStringToObject(r, "id", subscription_id);
			} else {
				cJSON_AddStringToObject(r, "message", errmsg);
			}

			release_table_record(tbl->schema, s);
			break;
		case 'u': ;
			break;
		case 'd': ;
			break;
		case 'q': ;
			break;
		default: ;
			// error message
	}

	*resp = r;

	return rv;
}

bool find_subscription_by_id(
		db_table_t *tbl,
		db_index_t *subidx,
		char *subscription_id,
		char **subrec) {

	bool rv = false;
	char *subid = NULL;
	dd_datafield_t *subfield = NULL;
	db_indexkey_t *kp = NULL, *subkey = dbidx_allocate_key_with_data(subidx->idx_schema);
	subkey->record = UINT64_MAX;

	for(uint8_t i = 0; i < subidx->idx_schema->num_fields; i++) {
		if ( strcmp(subidx->idx_schema->fields[i]->field_name, "subscription_id") == 0 ) {
			subfield = subidx->idx_schema->fields[i];
			break;
		}
	}
	if ( subfield == NULL )
		return rv;

	subid = malloc(subfield->field_sz);
	bzero(subid, subfield->field_sz);
	strcpy(subid, subscription_id);
	dbidx_set_key_field_value(subidx->idx_schema, "subscription_id", subkey, subid);
	if ((kp = dbidx_find_record(subidx->idx_schema, subidx->root_node, subkey)) != NULL) {
		if ( (*subrec = read_db_table_record(tbl->mapped_table, kp->record)) == NULL )
			fprintf(stderr, "Error while reading %s from slot %" PRIu64 "\n", subscription_id, kp->record);
		else
			rv = true;
	} else {
		printf("Subscription %s not found\n", subscription_id);
	}
	free(subid);
	free(subkey);
	return rv;
}

db_index_t *find_unique_key(db_table_t *tbl, char *candidate_key) {
	db_index_t *uq_idx = find_db_index(tbl, candidate_key);
	if ( uq_idx != NULL && !uq_idx->idx_schema->is_unique ) {
		uq_idx = NULL;
		fprintf(stderr, "Key %s is not a candidate for a unique search\n", candidate_key);
	}

	if ( uq_idx == NULL )
		for(uint8_t i = 0; i < tbl->num_indexes; i++)
			if ( uq_idx == NULL && tbl->indexes[i]->idx_schema->is_unique )
				uq_idx = tbl->indexes[i];
	return uq_idx;
}

bool subscription_txn_handler(
		journal_t *j,
		db_table_t *tbl,
		char action,
		char *subrec,
		char *keyname,
		char *errmsg,
		size_t errmsgsz
	) {

	char msg[128];
	bool jnlwrite = false;
	db_index_t *uq_idx = NULL, *idx = NULL;
	db_indexkey_t *k = NULL, *key = NULL;

	switch (action) {
		case 'i': ;
			if ( tbl->free_record_slot == UINT64_MAX) {
				fprintf(stderr, "ERR: writing to table %s, table is full\n", tbl->table_name);
				if ( errmsg != NULL )
					snprintf(errmsg, errmsgsz, "error writing to table %s, table is full", tbl->table_name);
				return false;
			}

			for(uint8_t i = 0; i < tbl->num_indexes; i++) {
				idx = tbl->indexes[i];
				if ( !idx->idx_schema->is_unique )
					continue;

				key = dbidx_allocate_key_with_data(uq_idx->idx_schema);
				key = create_key_from_record_data(tbl->schema, uq_idx->idx_schema, subrec);
				key->record = UINT64_MAX;
				k = dbidx_find_record(uq_idx->idx_schema, uq_idx->root_node, key);
				free(key);

				if ( k != NULL ) {
					idx_key_to_str(uq_idx->idx_schema, k, msg);
					fprintf(stderr, "ERR: Duplicate record %s\n", msg);
					if ( errmsg != NULL )
						snprintf(errmsg, errmsgsz, "ERR: Duplicate record %s", msg);
					return false;
				}
			}

			uint64_t recnum = UINT64_MAX;
			if ( (recnum = add_db_table_record(tbl->mapped_table, subrec)) == UINT64_MAX ) {
				fprintf(stderr, "ERR: writing to table %s\n", tbl->table_name);
				if ( errmsg != NULL )
					snprintf(errmsg, errmsgsz, "ERR: writing to table %s", tbl->table_name);
				return false;
			}

			char *newrec = read_db_table_record(tbl->mapped_table, recnum);
			for(uint8_t i = 0; i < tbl->num_indexes; i++) {
				db_indexkey_t *newkey = create_key_from_record_data(tbl->schema, tbl->indexes[i]->idx_schema, newrec);
				newkey->record = recnum;
				dbidx_add_index_value(tbl->indexes[i], NULL, newkey);
				free(newkey);
			}
			jnlwrite = true;
			break;
		case 'd':
			break;
		case 'u':
			break;
		case 'q':

			break;
		default:
			fprintf(stderr, "Unknown txn action %c\n", action);
			if ( errmsg != NULL )
				snprintf(errmsg, errmsgsz, "Unknown txn action %c", action);
			return false;
	}

	if ( jnlwrite ) {
		journal_record_t jr;
		bzero(&jr, sizeof(journal_record_t));

		jr.objtype = action;
		jr.msgsz = sizeof(journal_record_t) + tbl->schema->record_size;
		strcpy(jr.objname, tbl->table_name);
		jr.objsz = sizeof(tbl->schema->record_size);
		jr.objdata = subrec;
		if ( uq_idx != NULL && (action == 'u' || action == 'd') )
			strcpy(jr.objkey, uq_idx->index_name);
		write_journal_record(j, &jr);
	}

	return true;
}

void load_subs_from_file(
			char *filename,
			db_table_t *tbl,
			journal_t *j
		) {
	uint64_t rec_count = 0;
	char subid[32], line[1024], *l, *field, *t = NULL, *delim = "\t"; // tab
	unsigned int projectid[3];
	bzero(&subid, sizeof(subid));
	bzero(line, sizeof(line));
	uint8_t field_num = 0;

	bool sub_bool;
	uint32_t sub_u32;
	uint16_t sub_u16;
	struct timespec sub_timestamp;

	char *addsub = new_db_table_record(tbl->schema);

	journal_sync_off(j);

	gzFile gzfd = gzopen(filename, "r");
	while ( (l = gzgets(gzfd, line, sizeof(line))) != NULL ) {
		// chop the newline off of the end of the line
		if (l[strlen(l) - 1] == '\n')
			l[strlen(l) - 1] = '\0';

		bzero(addsub, tbl->schema->record_size);
		field_num = 0;
		field = strtok_r(l, delim, &t);
		do {
			switch (field_num) {
				case 0: // subscription_id
					strcpy(subid, field);
					set_db_table_record_field(tbl->schema, "subscription_id", field, addsub);
					break;
				case 1: // valid_from
					break;
				case 2: // customer_id
					set_db_table_record_field(tbl->schema, "customer_id", field, addsub);
					break;
				case 3: // project_id
					if ( strncmp(field, "\\\\x", sizeof(char)*3) == 0 && strlen(field+3) == 24) {
						char *projectidstr = field + 3;

						bzero(&projectid, sizeof(projectid));
						sscanf(projectidstr, "%08X%08X%08X",
								&projectid[0], &projectid[1], &projectid[2]
						);
						for(int i = 0; i < 3; i++)
							projectid[i] = htonl(projectid[i]);
						set_db_table_record_field(tbl->schema, "project_id", (char *)&projectid, addsub);
					}
					break;
				case 4: // is_active
					sub_bool = strcmp(field, "t") == 0 ? true : false;
					set_db_table_record_field(tbl->schema, "is_active", (char *)&sub_bool, addsub);
					break;
				case 5: // product
					set_db_table_record_field(tbl->schema, "product_type", field, addsub);
					break;
				case 6: // plan
					if ( strcmp(field, "\\N") != 0 )
						set_db_table_record_field(tbl->schema, "plan_id", field, addsub);
					break;
				case 7: // deferred plan
					if ( strcmp(field, "\\N") != 0 )
						set_db_table_record_field(tbl->schema, "deferred_plan_id", field, addsub);
					break;
				case 8: // currency
					if ( strcmp(field, "\\N") != 0 )
						set_db_table_record_field(tbl->schema, "currency", field, addsub);
					break;
				case 9: // plan_price
					sub_u32 = atoi(field);
					set_db_table_record_field(tbl->schema, "plan_price", (char *)&sub_u32, addsub);
					break;
				case 10: // quantity
					sub_u16 = atoi(field);
					set_db_table_record_field(tbl->schema, "quantity", (char *)&sub_u16, addsub);
					break;
				case 11: // term_start
					if ( strlen(field) > 0 ) {
						parse_utc_timestamp(field, &sub_timestamp);
						//parse_timestamp(field, &sub_timestamp);
						set_db_table_record_field(tbl->schema, "term_start", (char *)&sub_timestamp, addsub);
					}
					break;
				case 12: // term_end
					if ( strlen(field) > 0 ) {
						parse_utc_timestamp(field, &sub_timestamp);
						//parse_timestamp(field, &sub_timestamp);
						set_db_table_record_field(tbl->schema, "term_end", (char *)&sub_timestamp, addsub);
					}
					break;
				case 13: // autorenew - this can have nulls
					sub_bool = strcmp(field, "t") == 0 ? true : false;
					set_db_table_record_field(tbl->schema, "autorenew", (char *)&sub_bool, addsub);
					break;
				case 14: // canceled at
					if ( strlen(field) > 0 ) {
						parse_utc_timestamp(field, &sub_timestamp);
						//parse_timestamp(field, &sub_timestamp);
						set_db_table_record_field(tbl->schema, "canceled_at", (char *)&sub_timestamp, addsub);
					}
					break;
				case 15: // status
					if ( strcmp(field, "\\N") != 0 )
						set_db_table_record_field(tbl->schema, "status", field, addsub);
					break;
				case 16: // external ref
					if ( strcmp(field, "\\N") != 0 )
						set_db_table_record_field(tbl->schema, "external_reference", field, addsub);
					break;
				case 17: // lifecycle
					if ( strcmp(field, "\\N") != 0 )
						set_db_table_record_field(tbl->schema, "subscription_lifecycle", field, addsub);
					break;
				case 18: // churn
					if ( strcmp(field, "\\N") != 0 )
						set_db_table_record_field(tbl->schema, "churn_type", field, addsub);
					break;
				//default:
				//	printf("%"PRIu8": %s\n", field_num, field);
			}

			field_num++;
			field = strtok_r(NULL, delim, &t);
		} while (field != NULL);
		bzero(line, sizeof(line));

		//printf("Adding sub %s...", subid);
		subscription_txn_handler(j, tbl, 'i', addsub, NULL, NULL, 0);
		rec_count++;
		//printf("successfully added\n");

		if ( rec_count % 10000 == 0 )
			printf("Added %" PRIu64 " records to the table\n", rec_count);
	}
	gzclose(gzfd);
	free(addsub);
	printf("Added %" PRIu64 " records to the table\n", rec_count);
	journal_sync_on(j);
}

int main (int argc, char **argv) {
	int errs = 0;
	uint64_t counter = 0;
	char c;
	char *dd_filename = NULL, *datafile = NULL;
	data_dictionary_t **data_dictionary = NULL;

	struct timespec start_tm, end_tm;
	struct Server app_server;

	while ((c = getopt(argc, argv, "d:f:")) != -1) {
		switch(c) {
		case 'd': ;
		dd_filename = optarg;
			break;
		case 'f': ;
		datafile = optarg;
			break;
		case ':': ;
			fprintf(stderr, "Option -%c requires an option\n", optopt);
			errs++;
			break;
		case '?': ;
			fprintf(stderr, "Unknown option '-%c'\n", optopt);
		}
	}

	if ( dd_filename != NULL ) {
		if ( (data_dictionary = build_dd_from_json(dd_filename)) == NULL ) {
			fprintf(stderr, "Error while building data dictionary from file %s\n", dd_filename);
			errs++;
		}
	} else {
		errs++;
		fprintf(stderr, "Option -d <data dictionary> is required\n");
	}

	if ( errs > 0 )
		exit(EXIT_FAILURE);

	print_data_dictionary(*data_dictionary);

	if ( !load_all_dd_tables(*data_dictionary) )
		exit(EXIT_FAILURE);
	printf("All lables opened\n");

	/* sets up regular expressions for parsing time */
	init_common();

	journal_t jnl;
	bzero(&jnl, sizeof(journal_t));

	float time_diff;

	db_table_t *tbl = find_db_table(data_dictionary, "subscriptions");
	load_dd_index_from_table(tbl);
	printf("Indexes loaded\n");

	new_journal(&jnl);

	//del_subscription(st, &subid_idx, &custid_idx, &s);

	if ( datafile != NULL )
		load_subs_from_file(datafile, tbl, &jnl);

	bzero(&app_server, sizeof(struct Server));

	message_handler_list_t handlers;
	bzero(&handlers, sizeof(message_handler_list_t));

	// Admin handler
	message_handler_t admin_handler;
	bzero(&admin_handler, sizeof(message_handler_t));

	strcpy(admin_handler.handler_name, "admin_functions");
	admin_handler.handler = &admin_command;
	admin_handler.handler_argc = 1;
	admin_handler.handler_argv = malloc(sizeof(void *) * admin_handler.handler_argc);
	admin_handler.handler_argv[0] = &app_server;

	handlers.num_handlers++;
	handlers.handlers = malloc(sizeof(void *) * handlers.num_handlers);
	handlers.handlers[handlers.num_handlers - 1] = &admin_handler;

	// Subscription handler
	message_handler_t subscription_handler;
	bzero(&subscription_handler, sizeof(message_handler_t));

	strcpy(subscription_handler.handler_name, "subscriptions");
	subscription_handler.handler = &subscription_command;
	subscription_handler.handler_argc = 4;
	subscription_handler.handler_argv = malloc(sizeof(void *) * subscription_handler.handler_argc);
	subscription_handler.handler_argv[0] = &jnl;
	subscription_handler.handler_argv[1] = tbl;

	handlers.num_handlers++;
	handlers.handlers = realloc(handlers.handlers, sizeof(void *) * handlers.num_handlers);
	handlers.handlers[handlers.num_handlers - 1] = &subscription_handler;

	start_application(&handlers);

	printf("Subscription Index:\n");
	db_index_t *idx = NULL;
	for(uint8_t i = 0; i < tbl->num_indexes; i++) {
		idx = tbl->indexes[i];
		printf("Index: %s\n", idx->index_name);
		counter = 0;
		dbidx_print_tree_totals(idx, NULL, &counter);
		//counter = 0;
		//dbidx_print_tree(idx, NULL, &counter);
	}

	db_index_t *subidx = find_db_index(tbl, "subscription_id_idx_uq");
	db_indexkey_t *subkey = dbidx_allocate_key_with_data(subidx->idx_schema);
	dbidx_set_key_field_value(subidx->idx_schema, "subscription_id", subkey, "su_0k77ufeRLJyzXp");
	subkey->record = UINT64_MAX;

	db_index_t *custidx = find_db_index(tbl, "customer_id_idx");
	db_indexkey_t *custkey = dbidx_allocate_key_with_data(custidx->idx_schema);
	dbidx_set_key_field_value(custidx->idx_schema, "customer_id", custkey, "cus_DLr1bFxFbQzbdS");
	custkey->record = UINT64_MAX;

	dbidx_print_index_scan_lookup(subidx, subkey);

	clock_gettime(CLOCK_REALTIME, &start_tm);

	char *s = NULL;

	if ( find_subscription_by_id(tbl, subidx, "su_0k4aUCpOeZhysU", &s) ) {
		clock_gettime(CLOCK_REALTIME, &end_tm);
		time_diff = end_tm.tv_sec - start_tm.tv_sec;
		time_diff += (end_tm.tv_nsec / 1000000000.0) - (start_tm.tv_nsec / 1000000000.0);
		printf("Lookup was %fus\n", time_diff * 1000000.0);

		db_table_record_print(tbl->schema, s);
	}

	clock_gettime(CLOCK_REALTIME, &start_tm);
	if ( find_subscription_by_id(tbl, subidx, "sub_Gaeu53lByWsOWk", &s) ) {
		clock_gettime(CLOCK_REALTIME, &end_tm);
		time_diff = end_tm.tv_sec - start_tm.tv_sec;
		time_diff += (end_tm.tv_nsec / 1000000000.0) - (start_tm.tv_nsec / 1000000000.0);
		printf("Lookup was %fus\n", time_diff * 1000000.0);

		db_table_record_print(tbl->schema, s);
	}

	clock_gettime(CLOCK_REALTIME, &start_tm);
	if ( find_subscription_by_id(tbl, subidx, "su_2gjAMOQfjVI58E", &s) ) {
		clock_gettime(CLOCK_REALTIME, &end_tm);
		time_diff = end_tm.tv_sec - start_tm.tv_sec;
		time_diff += (end_tm.tv_nsec / 1000000000.0) - (start_tm.tv_nsec / 1000000000.0);
		printf("Lookup was %fus\n", time_diff * 1000000.0);

		db_table_record_print(tbl->schema, s);
	}

	free(subkey);
	free(custkey);

	printf("Closing tables\n");
	close_all_dd_tables(*data_dictionary);

	close_journal(&jnl);
	printf("Releasing common resources\n");
	cleanup_common();

	for(counter = 0; counter < handlers.num_handlers; counter++) {
		message_handler_t *h = handlers.handlers[counter];
		free(h->handler_argv);
	}
	free(handlers.handlers);

	release_data_dictionary(data_dictionary);
	printf("Done\n");
	exit(EXIT_SUCCESS);
}
