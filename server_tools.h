/*
 * server_tools.h
 *
 *  Created on: Apr 25, 2022
 *      Author: tgburrin
 */

#ifndef SERVER_TOOLS_H_
#define SERVER_TOOLS_H_

#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <poll.h>
#include <fcntl.h>

#include <cjson/cJSON.h>

#include "utils.h"

#define DEFAULT_SERVER_PORT 4933
#define DEFAULT_SERVER_NAME "shm-table"

#define MAX_CONNECTIONS 5 // 32768 would be the max with ephemeral ports

typedef struct Message {
	uint16_t msg_sz;
	uint16_t bytes_remaining;
	char *msgbuf;
} message_t;

bool format_error_reponse(char *msg, char *msgout, size_t msgoutsz);
int init_server_socket (void);
bool start_application (void);

#endif /* SERVER_TOOLS_H_ */
