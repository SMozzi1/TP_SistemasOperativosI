#include "comunicaciones.h"
#include "utils.h"
#include "read_instructions.h"


/*
 * Client-side state machine. Requests the next resource in the job's list, one
 * at a time, in the fixed CPU -> MEM -> GPU order (circular-wait prevention).
 * When there are no resources left the job is fully granted and Erlang is told.
 */
void ask_for_next_resource(local_job_t* job)
{
    if (job->next_req == NULL) {
        char id_str[16];
        snprintf(id_str, sizeof(id_str), "%d", job->job_id);
        C_to_erlang("granted", id_str);
        return;
    }

    /* Non-blocking socket; the RESERVE is actually sent later, from
     * send_request(), when epoll reports the socket is writable (EPOLLOUT). */
    int remote_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (remote_fd < 0) {
        perror("[ERROR] ask_for_next_resource: socket()");
        return;
    }

    struct sockaddr_in remote_addr;
    memset(&remote_addr, 0, sizeof(remote_addr));
    remote_addr.sin_family = AF_INET;
    remote_addr.sin_port   = htons(job->next_req->dest_port);
    inet_pton(AF_INET, job->next_req->dest_ip, &remote_addr.sin_addr);

    int conn_res = connect(remote_fd, (struct sockaddr *)&remote_addr, sizeof(remote_addr));
    if (conn_res < 0 && errno != EINPROGRESS) {
        perror("[ERROR] ask_for_next_resource: connect()");
        close(remote_fd);
        return;
    }

    job->origin_socket = remote_fd;

    /* fd -> job index, so send_request()/GRANTED can find the job by socket. */
    fd_job_entry entry;
    entry.fd  = remote_fd;
    entry.job = job;
    tablahash_insert(table_ourjobs, &entry);

    struct epoll_event ev;
    ev.events  = EPOLLOUT | EPOLLET | EPOLLONESHOT;
    ev.data.fd = remote_fd;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, remote_fd, &ev) < 0) {
        perror("[ERROR] epoll_ctl ADD");
        tablahash_remove_lock(table_ourjobs, &entry);
        close(remote_fd);
        return;
    }
}


/*
 * Sends a response to the Erlang scheduler process.
 * instruction: "granted" | "rejected" | "waiting" | "timeout"
 */
void C_to_erlang(const char *instruction, const char *job_id) {
    char msg[BUFFER_LEN];
    int  n;

    if      (!strcmp(instruction, "granted"))  n = snprintf(msg, sizeof(msg), "JOB_GRANTED %s\n",  job_id);
    else if (!strcmp(instruction, "rejected")) n = snprintf(msg, sizeof(msg), "JOB_DENIED %s\n",   job_id);
    else if (!strcmp(instruction, "waiting"))  n = snprintf(msg, sizeof(msg), "WAITING %s\n",      job_id);
    else                                       n = snprintf(msg, sizeof(msg), "JOB_TIMEOUT %s\n",  job_id);

    if (n < 0 || n >= (int)sizeof(msg)) {
        fprintf(stderr, "[ERROR] C_to_erlang: message truncated\n");
        return;
    }

    if (send(erlangfd, msg, n, MSG_DONTWAIT) < 0) {
        perror("[ERROR] C_to_erlang: send");
    }
}


/*
 * Handles a message that arrived from a remote node on 'current_fd':
 *  - RESERVE / RELEASE : the peer requests or frees one of OUR local resources.
 *  - GRANTED           : the peer answered a RESERVE WE sent.
 *  - anything else / DENIED : treated as a rejection of our job.
 */
void client_to_myserver(int current_fd, char *instruction) {
    /* Work on a copy to avoid destroying the original buffer */
    char copy[BUFFER_LEN];
    strncpy(copy, instruction, sizeof(copy) - 1);
    copy[sizeof(copy) - 1] = '\0';
    char *tokens[10];
    int   num = get_token(copy, tokens, 10);
    if (num < 1) return;

    /* ── RESERVE: a remote node is requesting a local resource ──────── */
    if (!strcmp(tokens[0], "RESERVE")) {
        if (num < 4) {
            fprintf(stderr, "[WARN] Malformed RESERVE: %s\n", instruction);
            return;
        }
        int amount = atoi(tokens[3]);
        int job_id = atoi(tokens[1]);

        /* enqueue and try to grant immediately (drain_queue decides) */
        enqueue_jobs(tokens[2], job_id, amount, current_fd);
    }
    /* ── RELEASE: the remote node frees resources we granted it ──────── */
    else if (!strcmp(tokens[0], "RELEASE")) {
        if (num >= 2) printf("[SERVER] RELEASE job %s en fd=%d\n", tokens[1], current_fd);
        release_client_by_fd(current_fd);
    }
    /* ── GRANTED: response to a RESERVE we sent ─────────────────────── */
    else if (!strcmp(tokens[0], "GRANTED")) {
        if (num < 2) return;
        int received_job_id = atoi(tokens[1]);

        fd_job_entry key;
        key.fd = current_fd;
        fd_job_entry* found = (fd_job_entry*) tablahash_find(table_ourjobs, &key);

        if (found != NULL && found->job != NULL && found->job->job_id == received_job_id) {
            local_job_t* job = found->job;

            pending_resource_t* req = job->next_req;
            if (req != NULL) {
                job->next_req = req->next;
                req->provider_fd = current_fd;      // keep this connection open for RELEASE
                req->next = job->granted_reqs;
                job->granted_reqs = req;
            }
            /* The job just made progress: restart its timeout clock so it counts
             * time waiting for the NEXT resource, not the whole lifetime (B2). */
            job->timer = get_monotonic_time();
            printf("[SERVER] Job %d: recurso otorgado por fd=%d\n", job->job_id, current_fd);

            /* This fd is now a held provider connection kept ONLY to send RELEASE
             * later. Drop it from the fd index AND stop monitoring it in epoll, so
             * its lifetime is owned solely by release_resources(): no disconnect-path
             * close, no fd-number reuse, no cross-talk (B1). It stays open. */
            tablahash_remove_lock(table_ourjobs, &key);
            epoll_ctl(epollfd, EPOLL_CTL_DEL, current_fd, NULL);
            ask_for_next_resource(job);
        }
    }
    /* ── Unknown / DENIED: reject the job ───────────────────────────── */
    else {
        if (num < 2) {
            fprintf(stderr, "[WARN] Mensaje desconocido o malformado: %s\n", instruction);
            return;
        }
        fd_job_entry key;
        key.fd = current_fd;
        fd_job_entry* found = (fd_job_entry*) tablahash_find(table_ourjobs, &key);
        if (found != NULL && found->job != NULL) {
            local_job_t* job = found->job;

            tablahash_remove_lock(table_ourjobs, &key);
            release_resources(job);

            char id_str[16];
            snprintf(id_str, sizeof(id_str), "%d", job->job_id);
            C_to_erlang("rejected", id_str);

            tablahash_remove_lock(table_localjobs, job);
        }
    }
}


/*
 * Handles a command received from the local Erlang scheduler.
 */
void erlang_to_C(char *instruction, time_t timer) {

    char copy[BUFFER_LEN];
    strncpy(copy, instruction, sizeof(copy) - 1);
    copy[sizeof(copy) - 1] = '\0';

    char *tokens[32];
    int   num = get_token(copy, tokens, 32);
    if (num < 1) return;

    /* ── JOB_REQUEST ─────────────────────────────────────────────── */
    if (!strcmp(tokens[0], "JOB_REQUEST")) {

        if (num < 2) {
            fprintf(stderr, "[WARN] JOB_REQUEST missing job_id\n");
            return;
        }

        char *job_id_str = tokens[1];
        int   job_id     = atoi(job_id_str);

        if (num < 3) {
            fprintf(stderr, "[WARN] JOB_REQUEST has no destinations for job %s\n", job_id_str);
            C_to_erlang("rejected", job_id_str);
            return;
        }

        local_job_t* newjob = malloc(sizeof(local_job_t));
        newjob->job_id        = job_id;
        newjob->erlang_socket = erlangfd;
        newjob->next_req      = NULL;
        newjob->granted_reqs  = NULL;
        newjob->timer         = timer;
        newjob->origin_socket = -1;

        pending_resource_t* tail = NULL;

        /* Each token is "@ip:res:amount" */
        for (int i = 2; i < num; i++) {
            char dest_copy[256];
            strncpy(dest_copy, tokens[i], sizeof(dest_copy) - 1);
            dest_copy[sizeof(dest_copy) - 1] = '\0';

            char *p = dest_copy;
            if (*p == '@') p++;

            char *dest_ip  = strtok(p,    ":");
            char *dest_res = strtok(NULL, ":");
            char *dest_amt = strtok(NULL, " ");

            if (!dest_ip || !dest_res || !dest_amt) {
                fprintf(stderr, "[WARN] Formato inválido: %s\n", tokens[i]);
                continue;
            }

            pending_resource_t* req = malloc(sizeof(pending_resource_t));
            strncpy(req->dest_ip, dest_ip, sizeof(req->dest_ip) - 1);
            req->dest_ip[sizeof(req->dest_ip) - 1] = '\0';
            strncpy(req->type, dest_res, sizeof(req->type) - 1);
            req->type[sizeof(req->type) - 1] = '\0';
            req->amount      = atoi(dest_amt);
            req->provider_fd = -1;
            req->next        = NULL;

            /* Resolve the destination port from table_nodes. The key is the raw
             * s_addr int, matching how udp_datagram_from_remote() stores nodes. */
            received_node node_key;
            memset(&node_key, 0, sizeof(node_key));
            node_key.ip = (int)inet_addr(dest_ip);
            received_node* remote_node = (received_node*) tablahash_find(table_nodes, &node_key);
            req->dest_port = (remote_node != NULL) ? remote_node->port : 4200;

            if (newjob->next_req == NULL) newjob->next_req = req;
            else                          tail->next = req;
            tail = req;
        }

        /* The owner table takes ownership of newjob (pointer-owning copy),
         * so we must NOT free it here. */
        tablahash_insert(table_localjobs, newjob);
        ask_for_next_resource(newjob);
    }
    /* ── JOB_RELEASE ─────────────────────────────────────────────── */
    else if (!strcmp(tokens[0], "JOB_RELEASE")) {
        if (num < 2) return;
        int job_id = atoi(tokens[1]);

        local_job_t job_key;
        job_key.job_id = job_id;
        local_job_t* job = (local_job_t*) tablahash_find(table_localjobs, &job_key);
        if (job == NULL) return;

        /* If a request is still in flight, tear down its outbound socket. */
        if (job->origin_socket >= 0) {
            fd_job_entry fkey;
            fkey.fd = job->origin_socket;
            if (tablahash_find(table_ourjobs, &fkey) != NULL) {
                tablahash_remove_lock(table_ourjobs, &fkey);
                epoll_ctl(epollfd, EPOLL_CTL_DEL, job->origin_socket, NULL);
                close(job->origin_socket);
            }
        }

        release_resources(job);                     // RELEASE to each provider
        tablahash_remove_lock(table_localjobs, job); // frees job + lists
    }
    /* ── JOB_STATUS ──────────────────────────────────────────────── */
    else if (!strcmp(tokens[0], "JOB_STATUS")) {
        if (num < 2) return;
        int job_id = atoi(tokens[1]);

        local_job_t job_key;
        job_key.job_id = job_id;
        local_job_t* job = (local_job_t*) tablahash_find(table_localjobs, &job_key);

        if (job == NULL) {
            /* Completed, rejected or expired: no "unknown" state in the protocol,
             * so report timeout (worst case Erlang relaunches it). */
            C_to_erlang("timeout", tokens[1]);
        } else if (job->next_req == NULL) {
            C_to_erlang("granted", tokens[1]);
        } else {
            C_to_erlang("waiting", tokens[1]);
        }
    }
    /* ── GET_NODES ───────────────────────────────────────────────── */
    else if (!strcmp(tokens[0], "GET_NODES")) {
        char* nodedata = get_nodes_instance();  // locks table_nodes internally
        if (nodedata != NULL) {
            if (send(erlangfd, nodedata, strlen(nodedata), MSG_DONTWAIT) < 0) {
                perror("[ERROR] erlang_to_C: send GET_NODES response");
            }
            free(nodedata);
        }
    }
    else {
        fprintf(stderr, "[WARN] erlang_to_C: unknown command '%s'\n", tokens[0]);
    }
}
