static int make_timer(int initial_sec, int interval_sec) {
    int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (tfd < 0) fatal_error("timerfd_create failed");

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


static void check_job_timeouts(void) {
    time_t now = time(NULL);

    /* ── tabla_propia: our jobs waiting for a reply from remote nodes ── */
    pthread_mutex_lock(&tabla_propia.mutexTable);
    for (int i = 0; i < HASH_SIZE; i++) {
        job_entry **pp = &tabla_propia.buckets[i];
        while (*pp) {
            job_entry *j = *pp;
            if (difftime(now, j->created_at) >= JOB_TIMEOUT_SEC) {
                fprintf(stderr, "[TIMEOUT] job %d in tabla_propia expired\n", j->job_id);

                char job_id_str[32];
                snprintf(job_id_str, sizeof(job_id_str), "%d", j->job_id);

                /* Notify Erlang only if the connection is still alive */
                if (erlangfd >= 0) {
                    C_to_erlang(erlangfd, "timeout", job_id_str);
                }

                *pp = j->next;
                FreeJob(j);
                /* Do not advance pp: the next entry is already at *pp */
            } else {
                pp = &(*pp)->next;
            }
        }
    }
    pthread_mutex_unlock(&tabla_propia.mutexTable);

    /* ── tabla_clientes: pending reservations from remote nodes ──────── */
    pthread_mutex_lock(&tabla_clientes.lock);
    for (int i = 0; i < HASH_SIZE; i++) {
        job_entry **pp = &tabla_clientes.buckets[i];
        while (*pp) {
            job_entry *j = *pp;
            if (difftime(now, j->created_at) >= JOB_TIMEOUT_SEC) {
                /*
                 * TODO (resource management teammate):
                 *   release_resources_for_job(j);
                 */
                *pp = j->next;
                FreeJob(j);
            } else {
                pp = &(*pp)->next;
            }
        }
    }
    pthread_mutex_unlock(&tabla_clientes.lock);
}