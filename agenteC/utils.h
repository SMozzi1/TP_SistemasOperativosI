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



#include "globals.h"


void log_error(const char *msg);
void fatal_error(const char *msg);


/* Client side: send RELEASE to every provider that granted a resource to this
 * local job and close those connections. Does NOT free the job (its owner
 * table frees it on removal). */
void release_resources(local_job_t* job);


/* Server side: a remote RESERVE arrived; enqueue it and try to grant. */
void enqueue_jobs(const char* resource, int job_id, int amount, int fd_actual);
void reserve_elements(void);
void drain_queue(request_queue* q, const char* type);


/* Server side: return to the local pool everything reserved on this fd
 * (RELEASE or unexpected disconnect) and re-drain the queues. */
void release_client_by_fd(int fd);

/* Client side: a provider disconnected before answering our RESERVE; reject
 * the affected local job instead of waiting for the timeout. */
void handle_outbound_disconnect(int fd);




#endif /* UTILS_H */