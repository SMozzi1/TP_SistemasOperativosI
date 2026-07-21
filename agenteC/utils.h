#ifndef UTILS_H
#define UTILS_H

#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <string.h>
#include <sys/types.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include "comunicaciones.h"




#


#include "../ResourceManager/job_table.h"
#include "globals.h"


void log_error(const char *msg);
void fatal_error(const char *msg);


/*Functions to work with global variables */
void update_local_resources(job_entry* job);
void release_resources(job_entry* job);
void original_socket(job_entry* job, int fd);


void enqueue_jobs(const char* resource, int job_id, int amount, int fd_actual);
void reserve_elements();


char* obtener_string_nodos(job_entry* job_table[]);
void release_client_by_fd(int fd);
void drain_queue(p_queue_t* q, int* avail, const char* type);
void handle_outbound_disconnect(int fd);




#endif /* UTILS_H */