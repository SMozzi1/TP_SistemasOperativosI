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
    char buffer[BUFFER_LEN];
    int accumulated_bytes;
} ConnectionState;


/* Function prototypes */

/* Initializes and configures non-blocking TCP and UDP listening sockets */
void inicializar_sockets_escucha(int *socket_escucha, int *socket_erlang, int *socket_UDP);

/* Core event loop that waits for and handles network events using epoll */
void aceptar_eventos(int epollfd, struct epoll_event events[]);

/* Creates the epoll instance, registers listening sockets, and starts the event loop */
void crear_epoll(void);

/* Prints system errors and terminates execution when a critical failure occurs */
void handler(const char *msg);

#endif /* AGENTE_CORE_H */
