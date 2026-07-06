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

/*
    Function for handling a new connection from the Erlang scheduler.
    Accepts the connection, sets it to non-blocking mode, and registers it with epoll for monitoring incoming data. 
    Returns the new file descriptor or -1 on error. 
*/
int connect_erlang(int fd, int epollfd);

/*
    Function for handling a new connection from a remote node.
    Accepts the connection, sets it to non-blocking mode, and registers it with epoll for monitoring incoming data. 
    Logs the remote client's IP address and file descriptor. 
*/
void connect_client(int fd, int epollfd);

/*
    Function for broadcasting the node's availability and resources over UDP.
    Constructs a message with the node's port and available resources, then sends it to the broadcast address.
    Uses a mutex to safely access shared resource variables. 
*/
void udp_broadcast(int socket_UDP, int port, int cpu, int mem, int gpu);

void udp_datagram_from_remote(int fd);

/*
    Function for handling incoming messages from the Erlang scheduler.
    Reads a line of input, processes it, and handles disconnections. 
*/
void message_from_erlang(int fd, int epollfd);

void send_request(int fd, int epollfd);

void recive_message_from_remote(int fd, int epollfd);




#endif /* LOOPFUNC_H */