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


/*
    Initializes the listening sockets for TCP (Erlang/Nodes) and UDP (Broadcast).
    Configures the sockets to be non-blocking and enables SO_REUSEADDR and SO_REUSEPORT
    to allow immediate binding on service restarts. Sets up distinct socket addresses:
    - socket_server: listens on 0.0.0.0 for external node connections.
    - socket_erlang: restricted to localhost (127.0.0.1) for local communication.
    - socket_UDP: configured for broadcasting node announcements.
 */
static void initialize_listen_sockets(void);


/*
    Main worker thread loop that waits for events from the epoll instance.
    Each thread blocks on epoll_wait and handles events such as:
    - New incoming connections (TCP).
    - Broadcast timer expiration (UDP announcements).
    - Job timeout management (local job table maintenance).
    - Incoming datagrams from remote nodes (UDP).
    - Inter-process communication with the Erlang scheduler (TCP).
        Uses EPOLLONESHOT to ensure thread safety when accessing shared file descriptors
        without requiring global per-fd mutexes.
 */
static void* event_loop(void *arg);



/**
    Sets up the epoll event monitoring system and spawns the thread pool.
    1. Configures signal handling.
    2. Initializes the epoll instance.
    3. Registers base listening sockets into the epoll watch list.
    4. Initializes high-resolution timers for broadcasting and timeout checks.
    5. Spawns NUM_WORKERS worker threads to process events concurrently.
*/
void setup_epoll(void);




#endif /* AGENTE_CORE_H */
