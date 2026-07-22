
#include "timer.h"


#define JOB_TIMEOUT_SEC 30
#define NODE_TIMEOUT_SEC 15



int make_timer(int initial_sec, int interval_sec, int epollfd) {
    // timerfd_create()  creates  a new timer object, and returns a file descriptor that refers to that timer.  The clockid argument specifies the clock
    // that is used to mark the progress of the timer.

    //CLOCK_MONOTONICA nonsettable monotonically increasing clock that measures 
    //time from some unspecified point in the past that does not change after system startup.

    //TFD_NONBLOCK  Set the O_NONBLOCK file status flag on the open file description (see open(2)) referred to 
    //by the new file descriptor.  Using  this flag saves extra calls to fcntl(2) to achieve the same result.
    int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (tfd < 0) fatal_error("timerfd_create failed");


    //struct itimerspec {
        //   struct timespec it_interval;  /* Interval for periodic timer */
        //   struct timespec it_value;     /* Initial expiration */
    //};

    struct itimerspec ts;
    ts.it_value.tv_sec     = initial_sec;
    ts.it_value.tv_nsec    = 0;
    ts.it_interval.tv_sec  = interval_sec;
    ts.it_interval.tv_nsec = 0;

    if (timerfd_settime(tfd, 0, &ts, NULL) < 0) {
        close(tfd);
        fatal_error("timerfd_settime failed");
    }

    struct epoll_event ev;
    ev.events  = EPOLLIN;
    ev.data.fd = tfd;

    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, tfd, &ev) < 0) {
        close(tfd);
        fatal_error("epoll_ctl ADD timer failed");
    }
    return tfd;
}





void check_job_timeouts( , int timeout_sec) {
    if (tabla == NULL) return;
    time_t now = time(NULL);

    pthread_mutex_lock(&tabla->mutexTable);

    for (int i = 0; i < TABLE_SIZE; i++) {
        job_entry **pp = &tabla->job_table[i];

        while (*pp) {
            job_entry *j = *pp;

            if (j->next_req != NULL && difftime(now, j->timestamp) >= timeout_sec) {
                fprintf(stderr, "[TIMEOUT] Job %d expired.\n", j->job_id);

                if (tabla == &table_ourjobs) {

                    char id_str[16];
                    snprintf(id_str, sizeof(id_str), "%d", j->job_id);
                    C_to_erlang("timeout", id_str);
                    // 1) Frees (notifying the supplier) ONLY what was given
                    //    (everything before next_req).
                    granted_t *res = j->resources;
                    while (res != NULL && res != j->next_req) {
                        granted_t *next_res = res->next;
                        if (res->provider_fd >= 0) {
                            char msg[256];
                            snprintf(msg, sizeof(msg), "RELEASE %d %s %d\n",
                                     j->job_id, res->type, res->amount);
                            send(res->provider_fd, msg, strlen(msg), MSG_NOSIGNAL);
                            close(res->provider_fd);
                        }
                        free(res);
                        res = next_res;
                    }
                    // 2) What was never given: DOESNT have a valid provider_fd yet
                    //   (trash without being initialized), we only free the memory.
                    while (res != NULL) {
                        granted_t *next_res = res->next;
                        free(res);
                        res = next_res;
                    }

                    // 3) Close the leaving connection "on fly" (the one waiting GRANTED/DENIED
                    //    from the next asked resource). If the job never asked anything,
                    //    origin_socket keeps being erlangfd: do not touch.
                    if (j->origin_socket >= 0 && j->origin_socket != erlangfd) {
                        epoll_ctl(epollfd, EPOLL_CTL_DEL, j->origin_socket, NULL);
                        close(j->origin_socket);
                    }

                } else if (tabla == &table_nodes) {
                    // The node stop announcing itself: we only remove it from the table.
                    // BEWARE: this resources are the ones announced by HIM, NOT OURS.
                    // Never should be added to cpu_available/mem_available/gpu_available.
                    DestroyGrantedList(j->resources);
                }

                j->resources = NULL;
                *pp = j->next_job;
                tabla->active_count--;
                ReleaseJob(j);

            } else {
                pp = &(*pp)->next_job;
            }
        }
    }

    pthread_mutex_unlock(&tabla->mutexTable);
}



time_t get_monotonic_time(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec;
}