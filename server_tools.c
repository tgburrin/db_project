/*
 * server_tools.c
 *
 *  Created on: Apr 25, 2022
 *      Author: tgburrin
 */

#include "server_tools.h"

bool format_error_reponse(char *msg, char *msgout, size_t msgoutsz) {
	bool rv = false;
	cJSON *obj = cJSON_CreateObject();

	if ( cJSON_AddStringToObject(obj, "error", msg) == NULL ) {
		cJSON_Delete(obj);
		return rv;
	}

	char *jsonmsg = cJSON_PrintUnformatted(obj);

	if (strlen(jsonmsg) < msgoutsz) {
		strcpy(msgout, jsonmsg);
		rv = true;
		free(jsonmsg);
		cJSON_Delete(obj);
	}

	return rv;
}

int init_server_socket (void) {
	uint16_t port = 0;
	int svr_sd = -1, enabled_op = 1;

	struct servent *se = NULL;
	struct sockaddr_in addr;

	if ( (se = getservbyname(DEFAULT_SERVER_NAME, "tcp")) == NULL ) {
		port = DEFAULT_SERVER_PORT;

	} else
		port = ntohs(se->s_port);

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

bool start_application (void) {
	bool rv = false;
	uint16_t ac = 0;
	int svr_sd = -1, cli_sd = -1, i;
	char stx = 2;
	char errmsg[1024];

	struct pollfd conns[MAX_CONNECTIONS];
	message_t buffers[MAX_CONNECTIONS];

	if ( (svr_sd = init_server_socket()) < 0 )
		return rv;

	for(i = 0; i < MAX_CONNECTIONS; i++)
		bzero(&buffers[i], sizeof(message_t));

	bzero(conns, sizeof(conns));
	conns[ac].fd = svr_sd;
	conns[ac].events = POLLIN;
	ac++;

	printf("Server started, accepting connections\n");
	for (;;) {
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
						bzero(errmsg, sizeof(errmsg));
						errmsg[0] = stx;
						format_error_reponse("Too many connections", errmsg + 2, sizeof(errmsg) - 3);
						size_t msglen = strlen(errmsg+2);
						errmsg[1] = htons(msglen);
						send(cli_sd, errmsg, msglen, 0);
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
				uint16_t buffsz = 2048;
				int recv_flag = MSG_PEEK;
				char *buffer;

				int rb = 0;
				if ( buffers[cnt].bytes_remaining > 0 &&
					 buffers[cnt].bytes_remaining < buffsz ) {
					buffsz = buffers[cnt].bytes_remaining;
					recv_flag = 0;
				}

				buffer = malloc(buffsz + 1);
				bzero(buffer, buffsz + 1);

				if ( (rb = recv(conns[cnt].fd, buffer, buffsz, recv_flag)) < 0 ) {
					free(buffer);
					fprintf(stderr, "Error reading from socket %d: %s\n", conns[cnt].fd, strerror(errno));
					close_conn = true;
				} else if ( rb == 0 ) {
					free(buffer);
					close_conn = true;
				} else {
					if ( buffers[cnt].bytes_remaining > 0 ) {


					} else {
						for(i = 0; i < buffsz; i++)
							if ( buffer[i] == stx )
								break;

						if ( i > 0 ) {
							// consume junk data
						} else if ( i == buffsz ) {
							// ran out of buffer - either discard or append
						}
					}

					free(buffer);
				}

				if ( close_conn ) {
					close(conns[cnt].fd);
					ac--;
					printf("Slot %d closed %d connections left active\n", cnt, ac);
					if ( ac - cnt > 0 ) {
						uint16_t s = ac - cnt;
						memcpy(&conns[cnt], &conns[cnt+1], sizeof(struct pollfd)*s);
					}
					bzero(&conns[ac], sizeof(struct pollfd));
				}
			}
		}
	}

	return rv;
}

bool process_message (char *payload) {
	bool rv = false;

	return rv;
}
