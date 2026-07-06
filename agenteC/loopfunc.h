#ifndef LOOPFUNC_H
#define LOOPFUNC_H

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


int connect_erlang(int fd, int epollfd);

void connect_client(int fd, int epollfd);

void udp_broadcast(int socket_UDP, int port, int cpu, int mem, int gpu);





#endif /* LOOPFUNC_H */