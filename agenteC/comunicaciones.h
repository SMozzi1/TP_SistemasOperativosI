#ifndef COMUNICACIONES_H
#define COMUNICACIONES_H

#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <sys/timerfd.h>
#include "job_table.h"


int socket_server;
int socket_erlang;
int socket_UDP;
int epollfd;
int erlangfd;

//MAX_FDS debe ser igual o mayor al número máximo de file descriptors que tu proceso puede tener abiertos simultáneamente
#define MAX_FDS 1024


#define LENG 1024
// Max capacity for the stream reconstruction buffer
#define BUFFER_MAX 2048

//Es el buffer acumulador por socket. Lo necesitás porque TCP no garantiza que un 
//mensaje llegue completo en un solo recv()
typedef struct {
    char buffer[512];
    int accumulated_bytes;
} ConnectionState;



/*
 *  Reads from a non-blocking socket piece by piece until a newline character is found.
 *  fd The socket file descriptor to read from (e.g., erlangfd).
 *  output_line Buffer where the complete line will be copied once fully assembled.
 *  1 if the line is complete, 0 if more data is needed, -1 on error/disconnection.
 */
int read_until_newline(int fd, char* output_line);

/**
 *  Parses instructions coming from Erlang and triggers the appropriate action.
 *  erlangfd The socket connected to the Erlang node.
 *  instruction The raw null-terminated string received from the socket.
 */
void erlang_to_C(int erlangfd, char* instruction);

/**
  Sends a formatted response back to Erlang safely using MSG_NOSIGNAL.
  The socket connected to the Erlang node.
  estado The status string (e.g., "granted", "rejected", "waiting").
  job_id The identifier of the job being processed.
 */
void C_to_erlang(int erlangfd, char* estado, char* job_id);

/*
 communication between C agents (peer-to-peer logic).
 */
void C_to_C(void);






#endif /* NETWORK_UTILS_H */