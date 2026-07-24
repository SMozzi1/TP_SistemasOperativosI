
#include "utils.h"



void log_error(const char *msg)   { perror(msg); }
void fatal_error(const char *msg) { perror(msg); exit(EXIT_FAILURE); }



/* ─────────────────────────── Client side ─────────────────────────── */

/*
 * Sends RELEASE to every provider that granted a resource to this local job
 * and closes those provider connections. The job struct itself is owned by
 * table_localjobs and is freed there when the job is removed.
 * The caller is responsible for removing the job from the tables afterwards.
 */
void release_resources(local_job_t* job)
{
    if (job == NULL) return;

    pending_resource_t* granted = job->granted_reqs;
    while (granted != NULL) {
        if (granted->provider_fd >= 0) {
            char msg[128];
            snprintf(msg, sizeof(msg), "RELEASE %d %s %d\n",
                     job->job_id, granted->type, granted->amount);
            send(granted->provider_fd, msg, strlen(msg), MSG_NOSIGNAL);

            printf("[INFO] RELEASE del recurso %s enviado al fd=%d (job %d)\n",
                   granted->type, granted->provider_fd, job->job_id);

            epoll_ctl(epollfd, EPOLL_CTL_DEL, granted->provider_fd, NULL);
            close(granted->provider_fd);
            granted->provider_fd = -1;
        }
        granted = granted->next;
    }
}



/* ─────────────────────────── Server side ─────────────────────────── */

/*
 * A remote RESERVE arrived. Queue it into the matching resource FIFO and try
 * to satisfy pending requests. If there is enough available it is granted
 * immediately by drain_queue; otherwise it stays queued (enunciado 5.1).
 */
void enqueue_jobs(const char* resource, int job_id, int amount, int fd_actual) {
    request* rq = make_request(job_id, fd_actual, amount);

    if (!strcmp(resource, "cpu")) {
        enqueue_request(cpu_queue, rq);
    } else if (!strcmp(resource, "mem")) {
        enqueue_request(mem_queue, rq);
    } else {
        enqueue_request(gpu_queue, rq);
    }

    reserve_elements();
}


/*
 * Records a granted server-side reservation in table_nodejobs so it can be
 * reclaimed on RELEASE or on the peer's disconnect (enunciado 5.1: "tabla de
 * jobs activos con los recursos concedidos"). Keyed by (id, ip, port); the
 * peer address is taken from the origin socket.
 */
static void record_granted(int job_id, int origin_socket, const char* type, int amount) {
    struct sockaddr_in peer;
    socklen_t plen = sizeof(peer);
    int ip = 0, port = 0;
    if (getpeername(origin_socket, (struct sockaddr*)&peer, &plen) == 0) {
        ip   = (int)peer.sin_addr.s_addr;
        port = ntohs(peer.sin_port);
    }

    received_job key;
    memset(&key, 0, sizeof(key));
    key.id = job_id;
    key.ip = ip;
    key.port = port;

    received_job rj;
    received_job* existing = (received_job*) tablahash_buscar(table_nodejobs, &key);
    if (existing != NULL) {
        rj = *existing;
    } else {
        memset(&rj, 0, sizeof(rj));
        rj.id = job_id;
        rj.ip = ip;
        rj.port = port;
    }
    rj.original_socket = origin_socket;

    if (!strcmp(type, "cpu"))      rj.cpu_granted += amount;
    else if (!strcmp(type, "mem")) rj.mem_granted += amount;
    else                           rj.gpu_granted += amount;

    tablahash_insertar(table_nodejobs, &rj); // deep-copies (received_job is a POD)
}


/*
 * Drains one resource FIFO: grants as many head requests as *avail allows.
 *
 * Concurrency:
 *  - The check (*avail >= head->amount) and the two mutations (dequeue and
 *    avail -=) run with BOTH locks held, so the decision is atomic.
 *  - Lock order is always mutex_resources -> mutexQueue; nothing takes them in
 *    reverse, so there is no deadlock.
 *  - The slow work (table insert + send) happens AFTER both locks are released;
 *    we never hold mutex_resources while touching a table mutex.
 */
void drain_queue(request_queue* q, int* avail, const char* type) {
    request* ready = NULL;   /* stash of granted requests, linked by next_req */

    pthread_mutex_lock(&mutex_resources);
    pthread_mutex_lock(&q->mutexQueue);
    while (q->first != NULL && *avail >= q->first->amount_requested) {
        request* req = dequeue_request_locked(q);
        *avail -= req->amount_requested;
        req->next_req = ready;
        ready = req;
    }
    pthread_mutex_unlock(&q->mutexQueue);
    pthread_mutex_unlock(&mutex_resources);

    /* ---- outside the locks ---- */
    while (ready != NULL) {
        request* req = ready;
        ready = ready->next_req;

        record_granted(req->job_id, req->origin_socket, type, req->amount_requested);

        char msg[64];
        int n = snprintf(msg, sizeof(msg), "GRANTED %d\n", req->job_id);
        send(req->origin_socket, msg, n, MSG_NOSIGNAL);

        printf("[SERVER] Job %d: %s otorgada (registrada en table_nodejobs)\n",
               req->job_id, type);
        destr_request(req);
    }
}


void reserve_elements(void) {
    drain_queue(cpu_queue, &cpu_available, "cpu");
    drain_queue(mem_queue, &mem_available, "mem");
    drain_queue(gpu_queue, &gpu_available, "gpu");
}


/*
 * Returns to the local pool every resource reserved on 'fd' (a peer's RELEASE
 * or an unexpected disconnect), removes those entries from table_nodejobs, and
 * re-drains the queues. Idempotent: if nothing is reserved on 'fd', it is a
 * no-op.
 */
void release_client_by_fd(int fd) {
    int found_any = 0;

    pthread_mutex_lock(&table_nodejobs->table_mutex);
    for (unsigned i = 0; i < table_nodejobs->capacidad; i++) {
        NodoLista** pp = &table_nodejobs->elems[i].lista;
        while (*pp != NULL) {
            received_job* job = (received_job*)(*pp)->dato;
            if (job->original_socket == fd) {
                found_any = 1;

                pthread_mutex_lock(&mutex_resources);
                cpu_available += job->cpu_granted;
                mem_available += job->mem_granted;
                gpu_available += job->gpu_granted;
                pthread_mutex_unlock(&mutex_resources);

                NodoLista* victim = *pp;
                *pp = victim->next;
                table_nodejobs->destr(victim->dato);
                free(victim);
                table_nodejobs->numElems--;
            } else {
                pp = &(*pp)->next;
            }
        }
    }
    pthread_mutex_unlock(&table_nodejobs->table_mutex);

    if (found_any) reserve_elements();
}


/*
 * A provider disconnected before answering our RESERVE. Look the local job up
 * by its current outbound fd (the fd index), reject it, release whatever other
 * providers had already granted, and drop it from both tables.
 */
void handle_outbound_disconnect(int fd) {
    fd_job_entry key;
    key.fd = fd;
    fd_job_entry* entry = (fd_job_entry*) tablahash_buscar(table_ourjobs, &key);
    if (entry == NULL || entry->job == NULL) return;

    local_job_t* job = entry->job;
    int job_id = job->job_id;

    fprintf(stderr, "[WARN] Job %d: proveedor remoto se desconectó sin responder. Rechazado.\n",
            job_id);

    /* drop the fd index entry (does not free the job) */
    tablahash_eliminar_lock(table_ourjobs, &key);

    /* release what other providers already granted */
    release_resources(job);

    char id_str[16];
    snprintf(id_str, sizeof(id_str), "%d", job_id);
    C_to_erlang("rejected", id_str);

    /* finally free the job via its owner table */
    tablahash_eliminar_lock(table_localjobs, job);
}
