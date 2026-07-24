#ifndef _TIMER_H_
#define _TIMER_H_


#include <sys/timerfd.h>
#include <sys/epoll.h>
#include <time.h>
#include "../ResourceManager/hash.h"



typedef struct times{
    int broadcast_timer_fd;   /* UDP broadcast timer  (fires every 5 s) */
    int timeout_timer_fd;     /* Job timeout checker  (fires every 5 s) */
    int port;
} worker_args_t;



/*
 * Creates a periodic timerfd and registers it in epoll.
 * initial_sec:  seconds until the first expiration.
 * interval_sec: repeat period in seconds.
 */
int make_timer(int initial_sec, int interval_sec, int epollfd);


/* Monotonic seconds since an unspecified epoch (safe for measuring durations). */
time_t get_monotonic_time(void);


/* Evicts peers from the nodes table that have not announced within NODE_TIMEOUT_SEC. */
void check_nodes_timeouts(TablaHash table_nodes);

/* No-Preemption deadlock recovery: times out local jobs still waiting for a
 * resource after JOB_TIMEOUT_SEC, releases their partial reservations, tells
 * Erlang JOB_TIMEOUT and drops them. 'table' must be the owner (table_localjobs). */
void check_ourjob_timeouts(TablaHash table);


#endif /*_TIMER_H_*/
