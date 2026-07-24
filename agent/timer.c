
#include "timer.h"
#include "communications.h"

#include <stdio.h>
#include <unistd.h>


#define JOB_TIMEOUT_SEC 30
#define NODE_TIMEOUT_SEC 15

/* Bounded number of jobs processed per timeout tick (avoids an unbounded stack
 * array; leftover expired jobs are handled on the next tick). */
#define MAX_TIMEOUTS_PER_TICK 256



int make_timer(int initial_sec, int interval_sec, int epollfd) {
    // timerfd_create() creates a new timer object and returns a file descriptor
    // referring to it. CLOCK_MONOTONIC is a nonsettable, monotonically
    // increasing clock, the correct choice for measuring durations.
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

    /* EPOLLONESHOT: the kernel delivers each expiration to EXACTLY ONE worker
     * and then disables the fd until it is re-armed. This is a hard guarantee
     * (unlike best-effort EPOLLEXCLUSIVE, which valgrind's thread serialization
     * defeats), so the ANNOUNCE fires once per tick. The handler in event_loop
     * must re-arm the fd (EPOLL_CTL_MOD) after draining it, or it never fires
     * again. NOTE: EPOLLONESHOT and EPOLLEXCLUSIVE cannot be combined (EINVAL). */
    struct epoll_event ev;
    ev.events  = EPOLLIN | EPOLLONESHOT;
    ev.data.fd = tfd;

    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, tfd, &ev) < 0) {
        close(tfd);
        fatal_error("epoll_ctl ADD timer failed");
    }
    return tfd;
}


time_t get_monotonic_time(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec;
}


/*
 * Node liveness: removes peers whose last ANNOUNCE is older than
 * NODE_TIMEOUT_SEC (enunciado 5.3). Safe single-pass deletion using a
 * pointer-to-pointer walk so we never advance through a freed node.
 */
void check_nodes_timeouts(HashTable table) {
    if (table == NULL) return;

    time_t now = get_monotonic_time();

    pthread_mutex_lock(&table->table_mutex);
    for (unsigned i = 0; i < table->capacity; i++) {
        ListNode** pp = &table->elems[i].list;
        while (*pp != NULL) {
            received_node* node = (received_node*)(*pp)->data;
            if (difftime(now, node->time) >= NODE_TIMEOUT_SEC) {
                printf("[TIMEOUT] Node ip=%d removed due to inactivity\n", node->ip);
                ListNode* to_remove = *pp;
                *pp = to_remove->next;
                table->destr(to_remove->data);
                free(to_remove);
                table->numElems--;
            } else {
                pp = &(*pp)->next;
            }
        }
    }
    pthread_mutex_unlock(&table->table_mutex);
}


/*
 * No-Preemption deadlock recovery (enunciado 3/6): a local job still waiting on
 * a resource after JOB_TIMEOUT_SEC is assumed stuck. We release its partial
 * reservations, tell Erlang JOB_TIMEOUT (so it relaunches later) and drop it.
 *
 * We collect the expired ids UNDER the table lock, then process them AFTER
 * releasing it, because the per-job cleanup (release_resources, C_to_erlang and
 * tablahash_remove_lock) takes locks that would otherwise deadlock or violate
 * the lock order.
 */
void check_ourjob_timeouts(HashTable table) {
    if (table == NULL) return;

    time_t now = get_monotonic_time();

    int expired[MAX_TIMEOUTS_PER_TICK];
    int count = 0;

    pthread_mutex_lock(&table->table_mutex);
    for (unsigned i = 0; i < table->capacity && count < MAX_TIMEOUTS_PER_TICK; i++) {
        ListNode* current = table->elems[i].list;
        while (current != NULL && count < MAX_TIMEOUTS_PER_TICK) {
            local_job_t* job = (local_job_t*)current->data;
            // Only jobs still waiting for a resource are deadlock candidates;
            // a fully granted job (next_req == NULL) is done, not stuck.
            if (job->next_req != NULL && difftime(now, job->timer) >= JOB_TIMEOUT_SEC) {
                expired[count++] = job->job_id;
            }
            current = current->next;
        }
    }
    pthread_mutex_unlock(&table->table_mutex);

    for (int k = 0; k < count; k++) {
        local_job_t job_key;
        job_key.job_id = expired[k];
        local_job_t* job = (local_job_t*) tablahash_find(table, &job_key);
        if (job == NULL) continue;   // already resolved between passes

        char id_str[16];
        snprintf(id_str, sizeof(id_str), "%d", job->job_id);
        printf("[TIMEOUT] Job %d timed out waiting for resources\n", job->job_id);

        /* Tear down the in-flight outbound socket, if any. */
        if (job->origin_socket >= 0) {
            fd_job_entry fkey;
            fkey.fd = job->origin_socket;
            if (tablahash_find(table_ourjobs, &fkey) != NULL) {
                tablahash_remove_lock(table_ourjobs, &fkey);
                epoll_ctl(epollfd, EPOLL_CTL_DEL, job->origin_socket, NULL);
                close(job->origin_socket);
            }
        }

        release_resources(job);                        // release partial reservations
        C_to_erlang("timeout", id_str);                // ask Erlang to relaunch
        tablahash_remove_lock(table_localjobs, job); // frees job + lists
    }
}
