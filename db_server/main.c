/*
 ============================================================================
 Name        : db_server.c
 Author      : Tim Burrington
 Version     :
 Copyright   : 
 Description : db_server
 ============================================================================
 */

#include <unistd.h>
#include <arpa/inet.h>
#include <sys/personality.h>

#include <zlib.h>

#include <data_dictionary.h>
#include <journal_tools.h>
#include <server_tools.h>
#include <db_interface.h>
#include <utils.h>

bool admin_command (cJSON *obj, cJSON **resp, uint16_t argc, void **argv, char *err, size_t errsz);

void remove_mem_table(data_dictionary_t **data_dictionary, char *tblname) {
	db_table_t *tbl = find_db_table(data_dictionary, tblname);
	if ( tbl != NULL ) {
		char *tpth = "/dev/shm";
		size_t tnsz = sizeof(char) * (strlen(tpth) + 1 + strlen(tbl->table_name) + 5);
		char *tn = malloc(tnsz);
		bzero(tn, tnsz);
		sprintf(tn, "%s/%s.shm", tpth, tbl->table_name);
		printf("removing %s\n", tn);
		unlink(tn);
		free(tn);
	}
}

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

void build_record_from_json(dd_table_schema_t *tbl, cJSON *data, char *result_record) {
	cJSON *k = NULL;
	char *fielddata = NULL;

	for(uint8_t i = 0; i < tbl->num_fields; i++) {
		k = cJSON_GetObjectItemCaseSensitive(data, tbl->fields[i]->field_name);
		if ( k != NULL ) {
			if (cJSON_IsNull(k))
				continue;

			if ( tbl->fields[i]->fieldtype == DD_TYPE_STR ||
					tbl->fields[i]->fieldtype == DD_TYPE_TIMESTAMP ||
					tbl->fields[i]->fieldtype == DD_TYPE_UUID ||
					tbl->fields[i]->fieldtype == DD_TYPE_BYTES ) {
				fielddata = malloc(tbl->fields[i]->field_sz);
				bzero(fielddata, tbl->fields[i]->field_sz);
				str_to_dd_type(tbl->fields[i], k->valuestring, fielddata);
				set_db_table_record_field_num(tbl, i, fielddata, result_record);
				free(fielddata);
				fielddata = NULL;
			} else if ( tbl->fields[i]->fieldtype >= DD_TYPE_I8 && tbl->fields[i]->fieldtype <= DD_TYPE_UI64 ) {
				if ( cJSON_IsNumber(k) ) {
					double d = cJSON_GetNumberValue(k);
					printf("Parsing %f\n", d);
					fielddata = malloc(tbl->fields[i]->field_sz);
					bzero(fielddata, tbl->fields[i]->field_sz);
					if ( tbl->fields[i]->fieldtype == DD_TYPE_I8 && d >= INT8_MIN && d <= INT8_MAX) {
						*(int8_t *)fielddata = (int8_t)(d);
					} else if ( tbl->fields[i]->fieldtype == DD_TYPE_UI8 && d >= 0 && d <= UINT8_MAX) {
						*(uint8_t *)fielddata = (uint8_t)(d);
					} else if ( tbl->fields[i]->fieldtype == DD_TYPE_I16 && d >= INT16_MIN && d <= INT16_MAX) {
						*(int16_t *)fielddata = (int16_t)(d);
					} else if ( tbl->fields[i]->fieldtype == DD_TYPE_UI16 && d >= 0 && d <= UINT16_MAX) {
						*(uint16_t *)fielddata = (uint16_t)(d);
					} else if ( tbl->fields[i]->fieldtype == DD_TYPE_I32 && d >= INT32_MIN && d <= INT32_MAX) {
						*(int32_t *)fielddata = (int32_t)(d);
					} else if ( tbl->fields[i]->fieldtype == DD_TYPE_UI32 && d >= 0 && d <= UINT32_MAX) {
						*(uint32_t *)fielddata = (uint32_t)(d);
					} else if ( tbl->fields[i]->fieldtype == DD_TYPE_I64 && d >= INT64_MIN && d <= INT64_MAX) {
						*(int64_t *)fielddata = (int64_t)(d);
					} else if ( tbl->fields[i]->fieldtype == DD_TYPE_UI64 && d >= 0 && d <= UINT64_MAX) {
						*(uint64_t *)fielddata = (uint64_t)(d);
					}
					set_db_table_record_field_num(tbl, i, fielddata, result_record);
					free(fielddata);
					fielddata = NULL;
				}
			} else if ( tbl->fields[i]->fieldtype == DD_TYPE_BOOL ) {
				bool tf = cJSON_IsBool(k) && cJSON_IsTrue(k) ? true : false;
				if ( cJSON_IsBool(k) )
					set_db_table_record_field_num(tbl, i, (char *)&tf, result_record);
			}
		}
	}
	printf("Built record:\n");
	db_table_record_print(tbl, result_record);
}

bool client_command (cJSON *obj, cJSON **resp, uint16_t argc, void **argv, char *err, size_t errsz) {
	bool rv = false;
	char *operation = NULL, *lookup_index = NULL, *client_record = NULL; //*client_id = NULL
	char op = 0, errmsg[129];

	struct timespec start_tm, end_tm;
	float time_diff;

	//journal_t *j = (journal_t *)(argv[0]);
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

			//client_record = new_db_table_record(tbl->schema);
			//client_id = malloc(subfield->field_sz * 2 + 1);
			//bzero(client_id, subfield->field_sz * 2 + 1);
			break;
		case 'u': ;
			break;
		case 'd': ;
			break;
		case 'q': ;
			cJSON *records = cJSON_AddArrayToObject(r, "records");
			printf("Running query on %s\n", tbl->table_name);
			client_record = new_db_table_record(tbl->schema);
			build_record_from_json(tbl->schema, data, client_record);
			//db_table_record_print(tbl->schema, client_record);

			db_index_t *lookupidx = NULL;
			for(uint8_t i = 0; i < tbl->num_indexes; i++) {
				printf("\tIndex -> %s\n", tbl->indexes[i]->index_name);
				if ( strcmp(tbl->indexes[i]->index_name, lookup_index) == 0 )
					lookupidx = tbl->indexes[i];
			}

			if ( lookupidx != NULL && lookupidx->idx_schema->is_unique ) {
				printf("Running unique index lookup using %s\n", lookupidx->index_name);
				clock_gettime(CLOCK_REALTIME, &start_tm);
				db_indexkey_t *findkey = create_key_from_record_data(tbl->schema, lookupidx->idx_schema, client_record);
				findkey->record = RECORD_NUM_MAX;
				dbidx_key_print(lookupidx->idx_schema, findkey);

				db_indexkey_t *foundkey = dbidx_find_record(lookupidx, findkey);

				if ( foundkey != NULL ) {
					char *foundsub = read_db_table_record(tbl->mapped_table, foundkey->record);
					if ( foundsub != NULL ) {
						clock_gettime(CLOCK_REALTIME, &end_tm);

						time_diff = end_tm.tv_sec - start_tm.tv_sec;
						time_diff += (end_tm.tv_nsec / 1000000000.0) - (start_tm.tv_nsec / 1000000000.0);
						printf("Lookup was %fus\n", time_diff * 1000000.0);

						db_table_record_print(tbl->schema, foundsub);
						char *fieldvalue = NULL;
						cJSON *recobj = cJSON_CreateObject();
						size_t offset = 0;
						for(uint8_t i = 0; i < tbl->schema->num_fields; i++) {
							dd_datafield_t *recfield = tbl->schema->fields[i];
							if ( recfield->fieldtype == DD_TYPE_STR ||
									recfield->fieldtype == DD_TYPE_TIMESTAMP ||
									recfield->fieldtype == DD_TYPE_UUID ||
									recfield->fieldtype == DD_TYPE_BYTES ) {
								dd_type_to_allocstr(recfield, foundsub + offset, &fieldvalue);
								cJSON_AddStringToObject(recobj, recfield->field_name, fieldvalue);
							} else if ( (recfield->fieldtype >= DD_TYPE_I8 && recfield->fieldtype <= DD_TYPE_UI64) || recfield->fieldtype == DD_TYPE_BOOL ) {
								dd_type_to_allocstr(recfield, foundsub + offset, &fieldvalue);
								cJSON_AddRawToObject(recobj, recfield->field_name, fieldvalue);
							}
							offset += recfield->field_sz;
							free(fieldvalue);
						}
						cJSON_AddItemToArray(records, recobj);
					}

				} else {
					printf("The following record could not be found:\n");
					dbidx_key_print(lookupidx->idx_schema, findkey);

				}

				free(findkey);
			} else if ( lookupidx == NULL )
				printf("Index %s could not be located\n", lookup_index);

			release_table_record(tbl->schema, client_record);
			break;
		default: ;
			// error message
	}

	*resp = r;

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

bool client_txn_handler(
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

			printf("Checking unique indexes\n");
			for(uint8_t i = 0; i < tbl->num_indexes; i++) {
				idx = tbl->indexes[i];
				if ( !idx->idx_schema->is_unique )
					continue;

				k = NULL;
				printf("Checking index %s\n", idx->index_name);
				key = dbidx_allocate_key_with_data(idx->idx_schema);
				key = create_key_from_record_data(tbl->schema, idx->idx_schema, subrec);
				key->record = RECORD_NUM_MAX;
				k = dbidx_find_record(idx, key);
				free(key);

				if ( k != NULL ) {
					idx_key_to_str(idx->idx_schema, k, msg);
					fprintf(stderr, "ERR: Duplicate record %s\n", msg);
					if ( errmsg != NULL )
						snprintf(errmsg, errmsgsz, "ERR: Duplicate record %s", msg);
					return false;
				}
			}

			printf("Adding table record\n");
			record_num_t recnum = RECORD_NUM_MAX;
			if ( (recnum = add_db_table_record(tbl->mapped_table, subrec)) == RECORD_NUM_MAX ) {
				fprintf(stderr, "ERR: writing to table %s\n", tbl->table_name);
				if ( errmsg != NULL )
					snprintf(errmsg, errmsgsz, "ERR: writing to table %s", tbl->table_name);
				return false;
			}

			char *newrec = read_db_table_record(tbl->mapped_table, recnum);
			for(uint8_t i = 0; i < tbl->num_indexes; i++) {
				printf("Adding index record for %s\n", tbl->indexes[i]->index_name);
				db_indexkey_t *newkey = create_key_from_record_data(tbl->schema, tbl->indexes[i]->idx_schema, newrec);
				newkey->record = recnum;
				dbidx_key_print(tbl->indexes[i]->idx_schema, newkey);
				dbidx_add_index_value(tbl->indexes[i], newkey);
				free(newkey);
			}
			jnlwrite = true;
			break;
		case 'd':
			break;
		case 'u':
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

void load_clients_from_file(data_dictionary_t *dd, char *filename) {
	char line[1024], *l, *field, *token = NULL;
	db_table_t *tbl = find_db_table(&dd, "test_table");

	printf("Total Record Count: %" PRIu64 "\n", (uint64_t)tbl->mapped_table->total_record_count);
	printf("Free Record Slot: %" PRIu64 "\n", (uint64_t)tbl->mapped_table->free_record_slot);
	if ( tbl->mapped_table->total_record_count - tbl->mapped_table->free_record_slot > 1 ) {
		fprintf(stderr, "The table is already populated\n");
		return;
	}

	char *client_record = new_db_table_record(tbl->schema);
	// struct timespec create_tm;

	uint64_t recordcount = 0;

	dd_datafield_t *tablefield = NULL;
	uuid_t clientid;
	tablefield = find_dd_field(&dd, "client_username");
	size_t client_username_sz = tablefield->field_sz + 1;
	char *client_username = malloc(client_username_sz);
	tablefield = find_dd_field(&dd, "client_name");
	size_t client_name_sz = tablefield->field_sz + 1;
	char *client_name = malloc(client_name_sz);

	uint32_t client_id = 0;
	gzFile gzfd = gzopen(filename, "r");
	while ( (l = gzgets(gzfd, line, sizeof(line))) != NULL ) {
		client_id++;
		// chop the newline off of the end of the line
		if (l[strlen(l) - 1] == '\n')
			l[strlen(l) - 1] = '\0';
		uint8_t field_num = 0;
		field = strtok_r(l, ",", &token);
		do {
			switch (field_num) {
			case 0: // client_id
				uuid_clear(clientid);
				uuid_parse(field, clientid);
				set_db_table_record_field(tbl->schema, "client_uid", (char *)&clientid, client_record);
				//set_db_table_record_field(tbl->schema, "client_id", (char *)&client_id, client_record);
			break;
			case 1: // client username
				bzero(client_username, client_username_sz);
				strcpy(client_username, field);
				set_db_table_record_field(tbl->schema, "client_username", client_username, client_record);
			break;
			case 2: // client name
				//bzero(client_name, client_name_sz);
				//strcpy(client_name, field);
				//set_db_table_record_field(tbl->schema, "client_name", client_name, client_record);
			break;
			default:
				printf("%" PRIu8": %s\n", field_num, field);
			}
			field_num++;
			field = strtok_r(NULL, ",", &token);
		} while (field != NULL);
		//bzero(line, sizeof(line));
		//clock_gettime(CLOCK_REALTIME, &create_tm);
		//set_db_table_record_field(tbl->schema, "created_dt", (char  *)&create_tm, client_record);

		record_num_t recnum = RECORD_NUM_MAX;
		if ( (recnum = add_db_table_record(tbl->mapped_table, client_record)) == RECORD_NUM_MAX ) {
			fprintf(stderr, "ERR: writing to table %s\n", tbl->table_name);
			return;
		}

		recordcount++;
		if ( recordcount % 100000 == 0 ) {
			printf("%" PRIu64 " client records loaded\n", recordcount);
			//db_table_record_print(tbl->schema, client_record);
		}

		reset_db_table_record(tbl->schema, client_record);
	}
	gzclose(gzfd);

	printf("Finished loading %" PRIu64 " client records\n", recordcount);

	free(client_username);
	free(client_name);
	release_table_record(tbl->schema, client_record);

	printf("Building indexes\n");
	load_dd_index_from_table(tbl);
}

int main (int argc, char **argv) {
	int errs = 0, c;
	bool remove_files = false;
	char *dd_filename = NULL, *datafile = NULL;
	data_dictionary_t **data_dictionary = NULL;

	struct Server app_server;

	/*
	 * https://rigtorp.se/virtual-memory/
	 */
	if ( personality(ADDR_NO_RANDOMIZE) < 0 ) {
		fprintf(stderr, "Could not set personality to compact mem\n");
		return EXIT_FAILURE;
	}

	while ((c = getopt(argc, argv, "d:f:r")) != -1) {
		switch(c) {
		case 'd': ;
			dd_filename = optarg;
			break;
		case 'f': ;
			datafile = optarg;
			break;
		case 'r': ;
			remove_files = true;
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

	//if ( !load_all_dd_disk_tables(*data_dictionary) )
	//	exit(EXIT_FAILURE);
	if ( !load_all_dd_tables(*data_dictionary) )
		exit(EXIT_FAILURE);
	printf("All lables opened\n");

	/* sets up regular expressions for parsing time */
	init_common();

	journal_t jnl;
	bzero(&jnl, sizeof(journal_t));

	db_table_t *tbl = NULL;
	for(uint32_t i = 0; i < (*data_dictionary)->num_tables; i++) {
		tbl = &(*data_dictionary)->tables[i];
		printf("Loading indexes for table %s\n", tbl->table_name);
		load_dd_indexes(tbl);
	}
	printf("Indexes loaded\n");

	new_journal(&jnl);

	//del_subscription(st, &subid_idx, &custid_idx, &s);

	if ( datafile != NULL )
		load_clients_from_file(*data_dictionary, datafile);
		//load_subs_from_file(datafile, tbl, &jnl);

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
	/*
	message_handler_t subscription_handler;
	bzero(&subscription_handler, sizeof(message_handler_t));

	strcpy(subscription_handler.handler_name, "subscriptions");
	subscription_handler.handler = &subscription_command;
	subscription_handler.handler_argc = 4;
	subscription_handler.handler_argv = malloc(sizeof(void *) * subscription_handler.handler_argc);
	subscription_handler.handler_argv[0] = &jnl;
	subscription_handler.handler_argv[1] = find_db_table(data_dictionary, "subscriptions");

	handlers.num_handlers++;
	handlers.handlers = realloc(handlers.handlers, sizeof(void *) * handlers.num_handlers);
	handlers.handlers[handlers.num_handlers - 1] = &subscription_handler;
	*/

	message_handler_t client_handler;
	bzero(&client_handler, sizeof(message_handler_t));

	strcpy(client_handler.handler_name, "clients");
	client_handler.handler = &client_command;
	client_handler.handler_argc = 2;
	client_handler.handler_argv = malloc(sizeof(void *) * client_handler.handler_argc);
	client_handler.handler_argv[0] = &jnl;
	client_handler.handler_argv[1] = find_db_table(data_dictionary, "test_table");;

	handlers.num_handlers++;
	handlers.handlers = realloc(handlers.handlers, sizeof(void *) * handlers.num_handlers);
	handlers.handlers[handlers.num_handlers - 1] = &client_handler;

	start_application(&handlers);

	db_index_t *idx = NULL;
	uint64_t recordcount = 0;
	for(uint32_t t = 0; t < (*data_dictionary)->num_tables; t++) {
		tbl = &(*data_dictionary)->tables[t];
		printf("Table %s\n", tbl->table_name);
		for(uint8_t i = 0; i < tbl->num_indexes; i++) {
			idx = tbl->indexes[i];
			printf("Index: %s\n", idx->index_name);
			recordcount = 0;
			dbidx_print_tree_totals(idx, NULL, &recordcount);
		}
	}

	for(uint32_t t = 0; t < (*data_dictionary)->num_tables; t++) {
		tbl = &(*data_dictionary)->tables[t];
		for(uint8_t i = 0; i < tbl->num_indexes; i++) {
			idx = tbl->indexes[i];
			printf("Writing index %s to disk\n", idx->index_name);
			dbidx_write_file_records(idx);
		}
	}
	printf("Closing tables\n");
	//close_all_dd_disk_tables(*data_dictionary);
	close_all_dd_tables(*data_dictionary);

	/*
	for(uint32_t t = 0; t < (*data_dictionary)->num_tables; t++)
		remove_mem_table(data_dictionary, (*data_dictionary)->tables[t].table_name);
	*/

	close_journal(&jnl);
	printf("Releasing common resources\n");
	cleanup_common();

	for(uint16_t counter = 0; counter < handlers.num_handlers; counter++) {
		message_handler_t *h = handlers.handlers[counter];
		free(h->handler_argv);
	}
	free(handlers.handlers);

	printf("Releasing the data dictionary + indexes\n");
	release_data_dictionary(data_dictionary);
	printf("Done\n");
	exit(EXIT_SUCCESS);
}
