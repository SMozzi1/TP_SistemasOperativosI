#ifndef AGENTE_H
#define AGENTE_H
//#define _GNU_SOURCE //This is needed to use accept4

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <sys/socket.h>

#include "comunicaciones.h"
#include "utils.h"
#include "../ResourceManager/job_table.h"
#include "globals.h"


/* Function prototypes */

/* Initializes and configures non-blocking TCP and UDP listening sockets */
static void initialize_listen_sockets(void);


/* Core event loop that waits for and handles network events using epoll */
static void *event_loop(void *arg);


/* Creates the epoll instance, registers listening sockets, and starts the event loop */
void setup_epoll(void);


#endif /* AGENTE_CORE_H */
