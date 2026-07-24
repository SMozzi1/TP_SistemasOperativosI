
#include "timer.h"
#include "globals.h"
#include "comunicaciones.h"
#include "utils.h"

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

    struct epoll_event ev;
    ev.events  = EPOLLIN;
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
void check_nodes_timeouts(TablaHash table) {
    if (table == NULL) return;

    time_t now = get_monotonic_time();

    pthread_mutex_lock(&table->table_mutex);
    for (unsigned i = 0; i < table->capacidad; i++) {
        NodoLista** pp = &table->elems[i].lista;
        while (*pp != NULL) {
            received_node* nodo = (received_node*)(*pp)->dato;
            if (difftime(now, nodo->time) >= NODE_TIMEOUT_SEC) {
                printf("[TIMEOUT] Nodo ip=%d eliminado por inactividad\n", nodo->ip);
                NodoLista* victim = *pp;
                *pp = victim->next;
                table->destr(victim->dato);
                free(victim);
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
 * tablahash_eliminar_lock) takes locks that would otherwise deadlock or violate
 * the lock order.
 */
void check_ourjob_timeouts(TablaHash table) {
    if (table == NULL) return;

    time_t now = get_monotonic_time();

    int expired[MAX_TIMEOUTS_PER_TICK];
    int count = 0;

    pthread_mutex_lock(&table->table_mutex);
    for (unsigned i = 0; i < table->capacidad && count < MAX_TIMEOUTS_PER_TICK; i++) {
        NodoLista* actual = table->elems[i].lista;
        while (actual != NULL && count < MAX_TIMEOUTS_PER_TICK) {
            local_job_t* job = (local_job_t*)actual->dato;
            // Only jobs still waiting for a resource are deadlock candidates;
            // a fully granted job (next_req == NULL) is done, not stuck.
            if (job->next_req != NULL && difftime(now, job->timer) >= JOB_TIMEOUT_SEC) {
                expired[count++] = job->job_id;
            }
            actual = actual->next;
        }
    }
    pthread_mutex_unlock(&table->table_mutex);

    for (int k = 0; k < count; k++) {
        local_job_t job_key;
        job_key.job_id = expired[k];
        local_job_t* job = (local_job_t*) tablahash_buscar(table, &job_key);
        if (job == NULL) continue;   // already resolved between passes

        char id_str[16];
        snprintf(id_str, sizeof(id_str), "%d", job->job_id);
        printf("[TIMEOUT] Job %d expiró esperando recursos\n", job->job_id);

        /* Tear down the in-flight outbound socket, if any. */
        if (job->origin_socket >= 0) {
            fd_job_entry fkey;
            fkey.fd = job->origin_socket;
            if (tablahash_buscar(table_ourjobs, &fkey) != NULL) {
                tablahash_eliminar_lock(table_ourjobs, &fkey);
                epoll_ctl(epollfd, EPOLL_CTL_DEL, job->origin_socket, NULL);
                close(job->origin_socket);
            }
        }

        release_resources(job);                        // release partial reservations
        C_to_erlang("timeout", id_str);                // ask Erlang to relaunch
        tablahash_eliminar_lock(table_localjobs, job); // frees job + lists
    }
}
