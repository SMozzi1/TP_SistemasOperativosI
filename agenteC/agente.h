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



/* Creates the epoll instance, registers listening sockets, and starts the event loop */
void setup_epoll(void);


#endif /* AGENTE_CORE_H */
