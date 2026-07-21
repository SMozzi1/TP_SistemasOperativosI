#ifndef _TIMER_H_
#define _TIMER_H_


#include <sys/timerfd.h>
#include <sys/epoll.h>
#include <time.h>
#include "../ResourceManager/job_table.h"



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



void check_job_timeouts(active_jobs* tabla, int timeout_sec);







#endif /*_TIMER_H_*/