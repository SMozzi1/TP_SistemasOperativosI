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




#


#include "../ResourceManager/job_table.h"
#include "globals.h"


void log_error(const char *msg)   { perror(msg); }
void fatal_error(const char *msg) { perror(msg); exit(EXIT_FAILURE); }


/*Estructura y funciones para trabajar con timerfd*/
typedef struct times{
    int broadcast_timer_fd;   /* UDP broadcast timer  (fires every 5 s) */
    int timeout_timer_fd;     /* Job timeout checker  (fires every 5 s) */
} worker_args_t;

int make_timer(int initial_sec, int interval_sec);
void check_job_timeouts(active_jobs* tabla, int timeout_sec);


/*Funciones para trabajar con las variables globales */
void update_local_resources(job_entry* job);
void release_resources(job_entry* job);
void original_socket(job_entry* job, int fd);


//Funciones de la implementacion tomi
void enqueue_jobs(const char* resource, int job_id, int amount, int fd_actual);
void reserve_elements();

char* obtener_string_nodos(job_entry* job_table[]);

#endif /* UTILS_H */