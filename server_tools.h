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

typedef bool (*message_handler_f)(cJSON *, uint16_t, void **, char *, size_t); // in key, out key

struct Server {
	bool running;
};

typedef struct Message {
	uint16_t msg_sz;
	uint16_t bytes_remaining;
	char *msgbuf;
} message_t;

typedef struct MessageHandler {
	char handler_name[DB_OBJECT_NAME_SZ];
	message_handler_f handler;
	uint16_t handler_argc;
	void **handler_argv;
} message_handler_t;

typedef struct MessageHandlerList {
	uint16_t num_handlers;
	message_handler_t **handlers;
} message_handler_list_t;

bool format_error_reponse(char *, char *, size_t);
int init_server_socket (void);
bool start_application (message_handler_list_t *);
uint16_t process_message (message_handler_list_t *, char *, char **);

#endif /* SERVER_TOOLS_H_ */
