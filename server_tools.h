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

#define MAX_CONNECTIONS 2048

bool start_application (void);

#endif /* SERVER_TOOLS_H_ */
