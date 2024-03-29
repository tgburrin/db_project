/*
 * server_tools.c
 *
 *  Created on: Apr 25, 2022
 *      Author: tgburrin
 */

#include "server_tools.h"

bool create_api_message(cJSON **msg) {
	bool rv = false;
	cJSON *obj = cJSON_CreateObject();

	cJSON_AddStringToObject(obj, "handler", "");
	cJSON_AddStringToObject(obj, "operation", "r"); // default to response op
	cJSON_AddArrayToObject(obj, "operation_flags");
	cJSON_AddObjectToObject(obj, "data");

	*msg = obj;
	rv = true;

	return rv;
}

bool format_error_reponse(char *msg, char *msgout, size_t msgoutsz) {
	bool rv = false;

	cJSON *obj = NULL;
	create_api_message(&obj);

	cJSON *op_flags = cJSON_GetObjectItem(obj, "operation_flags");
	cJSON *data = cJSON_GetObjectItem(obj, "data");

	cJSON *errstatus = cJSON_CreateString("error");
	cJSON_AddItemToArray(op_flags, errstatus);


	if ( cJSON_AddStringToObject(data, "message", msg) == NULL ) {
		cJSON_Delete(obj);
		return rv;
	}

	char *jsonmsg = cJSON_PrintUnformatted(obj);

	if (strlen(jsonmsg) < msgoutsz) {
		strcpy(msgout, jsonmsg);
		rv = true;
		free(jsonmsg);
	}

	cJSON_Delete(obj);
	return rv;
}

int init_server_socket (void) {
	uint16_t port = 0;
	int svr_sd = -1, enabled_op = 1;

	struct addrinfo hint;
	struct addrinfo *res = NULL;

	bzero(&hint, sizeof(struct addrinfo));

	hint.ai_family = AF_INET;
	hint.ai_socktype = SOCK_STREAM;

	struct sockaddr_in addr;

	if ( getaddrinfo(NULL, DEFAULT_SERVER_NAME, &hint, &res) == 0 ) {
		port = ntohs(((struct sockaddr_in *)res->ai_addr)->sin_port);
	} else {
		port = DEFAULT_SERVER_PORT;
	}

	if ( res != NULL )
		freeaddrinfo(res);

	if ( (svr_sd = socket(AF_INET, SOCK_STREAM, 0)) < 0 ) {
		fprintf(stderr, "Error openeing socket: %s\n", strerror(errno));
		return -1;

	}

	if ( setsockopt(svr_sd, SOL_SOCKET, SO_REUSEADDR, &enabled_op, sizeof(enabled_op)) < 0 ) {
		fprintf(stderr, "Error setting socket option: %s\n", strerror(errno));
		close(svr_sd);
		return -1;

	}

	if ( fcntl(svr_sd, F_SETFL, O_NONBLOCK) < 0 ) {
		fprintf(stderr, "Error setting socket to be non-blocking: %s\n", strerror(errno));
		close(svr_sd);
		return -1;

	}

	bzero(&addr, sizeof(struct sockaddr_in));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	addr.sin_port = htons(port);

	if ( bind(svr_sd, (struct sockaddr *)&addr, sizeof(struct sockaddr)) < 0 ) {
		fprintf(stderr, "Error binding socket to address: %s\n", strerror(errno));
		close(svr_sd);
		return -1;

	}

	if ( listen(svr_sd, 32) < 0 ) {
		fprintf(stderr, "Error listening to socket to address: %s\n", strerror(errno));
		close(svr_sd);
		return -1;
	}

	return svr_sd;
}

bool start_application (message_handler_list_t *h) {
	bool rv = false;
	uint16_t ac = 0;
	int svr_sd = -1, cli_sd = -1, i;
	char stx = 2;

	struct Server *app_server = NULL;

	struct pollfd conns[MAX_CONNECTIONS];
	message_t buffers[MAX_CONNECTIONS];

	for(i = 0; i < h->num_handlers && app_server == NULL; i++) {
		message_handler_t *hdl = (h->handlers)[i];
		if (strcmp(hdl->handler_name, "admin_functions") == 0 && hdl->handler_argc >= 1)
			app_server = (struct Server *)((hdl->handler_argv)[0]);
	}

	if ( app_server == NULL )
		return rv;

	if ( (svr_sd = init_server_socket()) < 0 )
		return rv;

	for(i = 0; i < MAX_CONNECTIONS; i++)
		bzero(&buffers[i], sizeof(message_t));

	bzero(conns, sizeof(conns));
	conns[ac].fd = svr_sd;
	conns[ac].events = POLLIN;
	ac++;

	app_server->running = true;
	printf("Server started, accepting connections\n");
	while (app_server->running) {
		printf("Polling....");
		fflush(stdout);
		if ( (i = poll(conns, ac, (60 * 1000))) < 0 ) {
			fprintf(stderr, "Error polling for connections: %s\n", strerror(errno));
			close(svr_sd);
			break;
		} else if ( i == 0 ) {
			printf("timed out. Looping.\n");
			fflush(stdout);
			continue;
		}
		printf("handling updates.\n");
		for(int cnt = 0; cnt < ac; cnt++) {
			if ( conns[cnt].revents == 0 ) {
				continue;
			} else if ( conns[cnt].revents != POLLIN ) {
				fprintf(stderr, "Event state on conn #%d = %d\n", cnt, conns[cnt].revents);
				continue;
			} else if ( conns[cnt].fd == svr_sd ) {
				int new_conn = 0;
				printf("Accepting new connection(s)\n");
				do {
					cli_sd = accept(svr_sd, NULL, NULL);
					if ( cli_sd < 0 ) {
						if (errno != EWOULDBLOCK)
							fprintf(stderr, "Error accepting new connection: %s\n", strerror(errno));
						continue;
					}
					if ( ac + 1 >= MAX_CONNECTIONS ) {
						uint16_t msgsz = 4096;
						char *errmsg = malloc(msgsz);
						bzero(errmsg, msgsz);
						((char *)errmsg)[0] = stx;
						format_error_reponse("Too many connections", errmsg + sizeof(char) + sizeof(uint16_t), msgsz - 3);
						size_t msglen = strlen(errmsg+3);
						/*
						printf("Sending %ld bytes: %s\n", strlen(errmsg + sizeof(char) + sizeof(uint16_t)),
														(char *)(errmsg + sizeof(char) + sizeof(uint16_t)));
						*/
						uint16_t nmsglen = htons(msglen);
						memcpy(errmsg + 1, &nmsglen, sizeof(uint16_t));
						send(cli_sd, errmsg, msglen + sizeof(char) + sizeof(uint16_t), 0);

						free(errmsg);
						close(cli_sd);
					} else {
						// if this puts us above our max connections, we need to reject with a response
						new_conn++;
						conns[ac].fd = cli_sd;
						conns[ac].events = POLLIN;
						ac++;
					}
				} while ( cli_sd != -1 );
			} else {
				bool close_conn = false;
				uint16_t buffsz = 4096;
				int recv_flag = MSG_PEEK;
				char *buffer;

				int rb = 0;
				if ( buffers[cnt].bytes_remaining > 0 &&
					 buffers[cnt].bytes_remaining < buffsz ) {
					buffsz = buffers[cnt].bytes_remaining;
					recv_flag = 0;
					printf("Existing buffer found with %d bytes remaining\n", buffsz);
				}

				buffer = malloc(buffsz);
				bzero(buffer, buffsz);

				rb = recv(conns[cnt].fd, buffer, buffsz, recv_flag);
				printf("Read %d bytes PEEKd from the network\n", rb);
				if ( rb < 0 ) {
					fprintf(stderr, "Error reading from socket %d: %s\n", conns[cnt].fd, strerror(errno));
					close_conn = true;
				} else if ( rb == 0 ) {
					printf("closing connection with no bytes\n");
					close_conn = true;
				} else {
					int stx_cnt = 0;
					for(i = 0; i < rb; i++) {
						if ( ((char *)buffer)[i] == stx ) {
							// network short may contain 0x02 as part of the number
							stx_cnt++;
							if ( stx_cnt >= 2) {
								// There are 2 messages in the buffer, walk it back to 1
								i--;
								break;
							} else if ( stx_cnt == 1 && i + sizeof(uint16_t) >= (unsigned long int)rb) {
								// There is 1 message at the end of the buffer and the size would
								// be split into 2 iterations
								i--;
								break;
							} else
								i += sizeof(uint16_t); // a network short may contain an STX byte
						}
					}

					bzero(buffer, buffsz);
					printf("Attempting to consume %d bytes out of %d from buffer\n", i, rb);
					rb = recv(conns[cnt].fd, buffer, i, 0);
					printf("Read %d bytes from the network\n", rb);

					if ( buffers[cnt].bytes_remaining == 2 && buffers[cnt].msg_sz == 0 ) {
						printf("Finding message size for boundary message\n");

						uint16_t msgsz = ntohs(*((uint16_t *)buffer));
						buffers[cnt].msg_sz = msgsz;
						buffers[cnt].bytes_remaining = msgsz;
						buffers[cnt].msgbuf = malloc(msgsz+1);
						bzero(buffers[cnt].msgbuf, msgsz+1);

					} else if ( buffers[cnt].bytes_remaining > 0 ) {
						printf("Collecting the remainder of a message\n");

						size_t offset = buffers[cnt].msg_sz - buffers[cnt].bytes_remaining;
						memcpy(buffers[cnt].msgbuf + offset, buffer, rb);
						buffers[cnt].bytes_remaining -= rb;

					} else {
						for(i = 0; i < rb; i++)
							if ( ((char *)buffer)[i] == stx )
								break;

						if ( i < rb)
							printf("STX found in postition %d\n", i);

						if ( i < rb - 2) {
							i++;
							uint16_t msgsz = ntohs(*((uint16_t *)(buffer + i)));
							printf("STX byte found with message of %d bytes\n", msgsz);
							i += 2;
							int ab = rb - i; // available bytes
							if ( ab > msgsz )
								ab = msgsz;

							buffers[cnt].msg_sz = msgsz;
							buffers[cnt].bytes_remaining = msgsz - ab;
							buffers[cnt].msgbuf = malloc(msgsz+1);
							bzero(buffers[cnt].msgbuf, msgsz+1);
							memcpy(buffers[cnt].msgbuf, buffer + i, ab);

						} else if ( i == rb - 1 ) {
							printf("Boundary message detected\n");
							buffers[cnt].msg_sz = 0;
							buffers[cnt].bytes_remaining = 2;

						} else {
							printf("No STX byte found - ignoring\n");

						}
					}
				}
				free(buffer);

				if ( buffers[cnt].msg_sz > 0 && buffers[cnt].bytes_remaining == 0 ) {
					//printf("processing message '%s' (%d bytes)\n", buffers[cnt].msgbuf, buffers[cnt].msg_sz);
					char *response = NULL, *errors = NULL;

					process_message(h, buffers[cnt].msgbuf, (char **)&response, NULL);

					if ( response != NULL ) {
						printf("Sending response %s\n", (char *)response);
						uint16_t msglen = strlen(response);
						response = realloc(response, msglen + 4); // header bytes and terminal \0
						bzero(response + msglen + 1, 3);
						memmove(response + sizeof(char) + sizeof(uint16_t), response, msglen + 1);
						((char *)response)[0] = stx;
						uint16_t nmsglen = htons(msglen + 1);
						memcpy(response + 1, &nmsglen, sizeof(uint16_t));
						send(conns[cnt].fd, response, msglen + (sizeof(char)*2) + sizeof(uint16_t), 0);
						free(response);
					}

					if ( errors != NULL )
						free(errors);

					free(buffers[cnt].msgbuf);
					bzero(&buffers[cnt], sizeof(message_t));
				}

				if ( close_conn ) {
					printf("Closing connection %d\n", cnt);
					close(conns[cnt].fd);
					ac--;
					//printf("Slot %d closed %d connections left active\n", cnt, ac);
					if ( ac - cnt > 0 ) {
						uint16_t s = ac - cnt;
						memmove(&conns[cnt], &conns[cnt+1], sizeof(struct pollfd)*s);
					}
					bzero(&conns[ac], sizeof(struct pollfd));
				}
			}
		}
	}

	printf("Shutting down...\n");
	for(i = 0; i < MAX_CONNECTIONS; i++) {
		if ( buffers[i].msgbuf != NULL )
			free(buffers[i].msgbuf);
	}

	for(i = ac - 1; i >= 0; i--) {
		close(conns[i].fd);
		bzero(&conns[i], sizeof(struct pollfd));
	}

	return true;
}

uint16_t process_message (message_handler_list_t *h, char *payload, char **response, char **errors) {
	uint16_t rv = 0;
	cJSON *doc = NULL;


	if ( (doc = cJSON_Parse(payload)) == NULL ) {
		rv++;
		char *err = NULL;
		if ( (err = (char *)cJSON_GetErrorPtr()) != NULL ) {
			fprintf(stderr, "Error parsing json: %s\n", err);
			if ( errors != NULL ) {
				size_t msglen = sizeof(char) * strlen(err) + 21;
				*errors = malloc(msglen);
				bzero(*errors, msglen);
				sprintf(*errors, "Error parsing json: %s", err);
			}
		}
		return rv;
	}

	char *str = cJSON_Print(doc);
	printf("Handling\n%s\n", str);

	if (str != NULL)
		free(str);

	if ( doc != NULL ) {
		if ( !cJSON_IsObject(doc) ) {
			rv++;
			cJSON_Delete(doc);
			return rv;
		}
		cJSON *resp = NULL;
		cJSON *respmsg = NULL;
		cJSON *handler = cJSON_GetObjectItemCaseSensitive(doc, "handler");
		if ( cJSON_IsString(handler) && (handler->valuestring != NULL) ) {
			printf("Searching for handler %s\n", handler->valuestring);
			int i = 0;
			for(i = 0; i < h->num_handlers; i++) {
				if ( strcmp(handler->valuestring, (h->handlers[i])->handler_name) == 0 ) {
					create_api_message(&respmsg);
					printf("Found handler for %s\n", handler->valuestring);
					message_handler_t *run = h->handlers[i];
					(*run->handler)(doc, &resp, run->handler_argc, run->handler_argv, NULL, 0);
					if ( resp != NULL ) {
						cJSON *op_flags = cJSON_GetObjectItem(respmsg, "operation_flags");
						cJSON *status = cJSON_CreateString("success");
						cJSON_AddItemToArray(op_flags, status);
						cJSON_ReplaceItemInObjectCaseSensitive(respmsg, "data", resp);
					}
					*response = cJSON_PrintUnformatted(respmsg);
					cJSON_Delete(respmsg);
					break;
				}
			}
			if ( i == h->num_handlers )
				fprintf(stderr, "handler not found for message type %s\n", handler->valuestring);
		}
		cJSON_Delete(doc);
	}

	return rv;
}
