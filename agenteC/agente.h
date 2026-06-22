#ifndef AGENTE_H
#define AGENTE_H
#define _GNU_SOURCE //This is needed to use accept4

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

#include "comunicaciones.h"


typedef struct {
    int broadcast_timer_fd;   /* UDP broadcast timer  (fires every 5 s) */
    int timeout_timer_fd;     /* Job timeout checker  (fires every 5 s) */
} worker_args_t;

typedef struct {
    char buffer[BUFFER_LEN];
    int accumulated_bytes;
} ConnectionState;


/* Function prototypes */

/* Initializes and configures non-blocking TCP and UDP listening sockets */
static void initialize_listen_sockets(void);


static int make_timer(int initial_sec, int interval_sec);


static void check_job_timeouts(void);


/* Core event loop that waits for and handles network events using epoll */
void *event_loop(void *arg);


/* Creates the epoll instance, registers listening sockets, and starts the event loop */
void setup_epoll(void);


#endif /* AGENTE_CORE_H */
