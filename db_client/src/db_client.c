/*
 ============================================================================
 Name        : db_client.c
 Author      : Tim Burrington
 Version     :
 Copyright   : 
 Description : db client to be paired with db_project
 ============================================================================
 */
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <inttypes.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <cjson/cJSON.h>

#define DEFAULT_SERVER_PORT 4933
#define DEFAULT_SERVER_HOST "localhost"
#define DEFAULT_SERVER_NAME "shm-table"

extern char *optarg;
extern int opterr, optind, optopt;

int init_client_socket (char *hostname, uint16_t port) {
	int cli_sd = -1, enabled_op = 1, i;
	char *servicename = NULL;

	struct addrinfo hint;
	struct addrinfo *res;

	bzero(&hint, sizeof(struct addrinfo));

	hint.ai_family = AF_INET;
	hint.ai_socktype = SOCK_STREAM;

	struct sockaddr_in addr;
	bzero(&addr, sizeof(struct sockaddr_in));

	if ( hostname == NULL )
		hostname = DEFAULT_SERVER_HOST;

	if ( port == 0 )
		servicename = DEFAULT_SERVER_NAME;

	if ( (i = getaddrinfo(hostname, NULL, &hint, &res)) != 0 ) {
		fprintf(stderr, "Error while looking up host '%s': %s\n", hostname, gai_strerror(i));
		return cli_sd;
	} else
		memcpy(&addr, res->ai_addr, sizeof(struct sockaddr_in));

	if ( servicename != NULL ) {
		if ( (i = getaddrinfo(NULL, servicename, &hint, &res)) == 0 ) {
			port = ntohs(((struct sockaddr_in *)res->ai_addr)->sin_port);
		} else {
			port = DEFAULT_SERVER_PORT;
		}
	}

	printf("Attempting connect to %s:%"PRIu16"\n", hostname, port);

	freeaddrinfo(res);

	if ( (cli_sd = socket(AF_INET, SOCK_STREAM, 0)) < 0 ) {
		fprintf(stderr, "Error openeing socket: %s\n", strerror(errno));
		return -1;

	}

	addr.sin_port = htons(port);

	if ( (i = connect(cli_sd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in))) < 0 ) {
		close(cli_sd);
		fprintf(stderr, "Error while connecting to host: %s\n", strerror(errno));
		return -1;
	}

	return cli_sd;
}

char *read_json_file(char *filename) {
	char *rv = NULL;
	FILE *fdf = NULL;

	if ( filename == NULL )
		return rv;

	if ( strcmp(filename, "-") == 0 ) {
		fdf = stdin;
		printf("Opened stdin\n");
	} else 	if ( (fdf = fopen(filename, "r")) == NULL ) {
		fprintf(stderr, "Unable open file pointer for %s: %s\n", filename, strerror(errno));
		return rv;
	} else
		printf("Opened file '%s'\n", filename);

	size_t i = 0, buffsz = 4096;
	uint32_t rb = 0, memsz = buffsz;
	char *buff = malloc(buffsz);
	bzero(buff, buffsz);

	for (;;) {
		printf("Reading up to %ld bytes from file\n", buffsz);
		if ( (i = fread(buff +  rb, buffsz, 1, fdf)) == 0 && !feof(fdf)) {
			fprintf(stderr, "Unable to read from file\n");
			free(buff);
			fclose(fdf);
			return rv;

		} else if ( feof(fdf) ) {
			// this was the last read
			rv = buff;
			break;

		} else {
			rb += i * buffsz;
			printf("Read %ld bytes...allocating more space\n", i * buffsz);
			memsz += buffsz;
			if ( (buff = realloc(buff, memsz)) == NULL ) {
				fprintf(stderr, "Unable to allocate more memory while reading file\n");
				free(buff);
				fclose(fdf);
				return rv;
			}
			bzero(buff+rb, buffsz);
		}
	}

	fclose(fdf);
	printf("File of %ld bytes loaded\n", strlen(rv));

	return rv;
}

int main(int argc, char **argv) {
	int client_sd, rv = EXIT_FAILURE;
	char *hostname = NULL, *filename = NULL;
	uint16_t port = 0, errs = 0, msglen = 0;
	char stx = 2, c;

	while ((c = getopt(argc, argv, "h:p:f:")) != -1) {
		switch(c) {
		case 'h': ;
			hostname = optarg;
			break;
		case 'p': ;
			int i = 0;
			if ( ( i = atoi(optarg) ) <= 0 ) {
				fprintf(stderr, "Invalid option for port provided: %s\n", optarg);
				errs++;
			} else {
				port = i;
			}
			break;
		case 'f': ;
			filename = optarg;
			break;
		case ':': ;
			fprintf(stderr, "Option -%c requires an option\n", optopt);
			errs++;
			break;
		case '?': ;
			fprintf(stderr, "Unknown option '-%c'\n", optopt);
		}
	}

	if ( errs > 0 )
		exit(rv);

	if ( filename == NULL ) {
		fprintf(stderr, "Filename (-f) is required\n");
		exit(rv);
	}

	char *filedata = NULL;
	if ( (filedata = read_json_file(filename)) == NULL ) {
		fprintf(stderr, "Error reading from %s\n", filename);
		exit(EXIT_FAILURE);
	}

	cJSON *doc = NULL;
	if ( (doc = cJSON_Parse(filedata)) == NULL ) {
		char *err = NULL;
		if ( (err = cJSON_GetErrorPtr()) != NULL )
			fprintf(stderr, "Error parsing json: %s\n", err);
		exit(EXIT_FAILURE);
	} else
		free(filedata);

	filedata = cJSON_PrintUnformatted(doc);

	msglen = strlen(filedata);
	size_t buffsz = sizeof(char) * msglen + sizeof(char) + sizeof(uint16_t) + 1;
	void *msg = malloc(buffsz);
	bzero(msg, buffsz);

	memcpy(msg, &stx, sizeof(char));
	uint16_t nmsglen = htons(msglen);
	memcpy(msg + sizeof(char), &nmsglen, sizeof(uint16_t));
	memcpy(msg + sizeof(char) + sizeof(uint16_t), filedata, strlen(filedata));
	cJSON_Delete(doc);
	free(filedata);

	// Everything is verified, lets do the work

	if ( (client_sd = init_client_socket(hostname, port)) >= 0 ) {
		printf("Connected!\n");
		size_t wrb = send(client_sd, msg, msglen + sizeof(char) + sizeof(uint16_t), 0);
		printf("Wrote %ld bytes to socket\n", wrb);
		free(msg);

		void *resp = malloc(4096);
		size_t rrb = recv(client_sd, resp, 4096, 0);
		if ( ((char *)resp)[0] == stx ) {
			uint16_t respsz = ntohs(*((uint16_t *)(resp+1)));
			char *msg = malloc(respsz+1);
			bzero(msg, respsz+1);

			memcpy(msg, resp + sizeof(char) + sizeof(uint16_t), respsz);
			printf("Response Message (%d bytes): %s\n", respsz, msg);
			free(msg);
		}
		free(resp);
		close(client_sd);
		rv = EXIT_SUCCESS;
	}
	exit(rv);
}
