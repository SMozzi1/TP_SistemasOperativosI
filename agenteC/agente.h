#ifndef AGENTE_H
#define AGENTE_H
//#define _GNU_SOURCE //This is needed to use accept4

#include "comunicaciones.h"
#include "utils.h"
#include "globals.h"
#include "loopfunc.h"


/*
    Initializes the listening sockets for TCP (Erlang/Nodes) and UDP (Broadcast).
    Configures the sockets to be non-blocking and enables SO_REUSEADDR and SO_REUSEPORT
    to allow immediate binding on service restarts. Sets up distinct socket addresses:
    - socket_server: listens on 0.0.0.0 for external node connections.
    - socket_erlang: restricted to localhost (127.0.0.1) for local communication.
    - socket_UDP: configured for broadcasting node announcements.

    (initialize_listen_sockets and event_loop are file-local statics defined in
    agente.c; they are intentionally not declared here.)
 */

/**
    Sets up the epoll event monitoring system and spawns the thread pool.
    1. Configures signal handling.
    2. Initializes the epoll instance.
    3. Registers base listening sockets into the epoll watch list.
    4. Initializes high-resolution timers for broadcasting and timeout checks.
    5. Spawns NUM_WORKERS worker threads to process events concurrently.
*/
void setup_epoll(int port);




#endif /* AGENTE_CORE_H */
