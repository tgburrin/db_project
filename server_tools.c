/*
 * server_tools.c
 *
 *  Created on: Apr 25, 2022
 *      Author: tgburrin
 */

#include "server_tools.h"

bool start_application (void) {
	bool rv = false;
	uint16_t port = 0, active_connections = 0;
	int svr_sd = -1, cli_sd = -1, i, enabled_op = 1;

	struct servent *se = NULL;
	struct sockaddr_in addr;
	struct pollfd conns[MAX_CONNECTIONS];

	if ( (se = getservbyname(DEFAULT_SERVER_NAME, "tcp")) == NULL ) {
		port = DEFAULT_SERVER_PORT;
	} else
		port = ntohs(se->s_port);

	if ( (svr_sd = socket(AF_INET, SOCK_STREAM, 0)) < 0 ) {
		fprintf(stderr, "Error openeing socket: %s\n", strerror(errno));
		return rv;
	}

	if ( setsockopt(svr_sd, SOL_SOCKET, SO_REUSEADDR, &enabled_op, sizeof(enabled_op)) < 0 ) {
		fprintf(stderr, "Error setting socket option: %s\n", strerror(errno));
		close(svr_sd);
		return rv;
	}

	if ( fcntl(svr_sd, F_SETFL, O_NONBLOCK) < 0 ) {
		fprintf(stderr, "Error setting socket to be non-blocking: %s\n", strerror(errno));
		close(svr_sd);
		return rv;
	}

	bzero(&addr, sizeof(struct sockaddr_in));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	addr.sin_port = htons(port);

	if ( bind(svr_sd, (struct sockaddr *)&addr, sizeof(struct sockaddr)) < 0 ) {
		fprintf(stderr, "Error binding socket to address: %s\n", strerror(errno));
		close(svr_sd);
		return rv;
	}

	if ( listen(svr_sd, 32) < 0 ) {
		fprintf(stderr, "Error listening to socket to address: %s\n", strerror(errno));
		close(svr_sd);
		return rv;
	}

	bzero(conns, sizeof(conns));
	conns[0].fd = svr_sd;
	conns[0].events = POLLIN;
	active_connections++;

	printf("Server started, accepting connections\n");
	for (;;) {
		printf("Polling....");
		fflush(stdout);
		if ( (i = poll(conns, active_connections, (60 * 1000))) < 0 ) {
			fprintf(stderr, "Error polling for connections: %s\n", strerror(errno));
			close(svr_sd);
			break;
		} else if ( i == 0 ) {
			printf("timed out. Looping.\n");
			fflush(stdout);
			continue;
		} else {
			printf("handling updates.\n");
		}

		for(int cnt = 0; cnt < active_connections; cnt++) {
			if ( conns[cnt].revents == 0 ) {
				continue;
			} else if ( conns[cnt].revents != POLLIN ) {
				fprintf(stderr, "event state on conn #%d = %d\n", cnt, conns[cnt].revents);
				continue;
			} else if ( conns[cnt].fd == svr_sd ) {
				int new_conn = 0;
				printf("Accepting new connection(s)\n");
				do {
					new_conn++;
					cli_sd = accept(svr_sd, NULL, NULL);
				} while ( cli_sd != -1 );

			}
		}
	}

	return rv;
}
