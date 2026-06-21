#define _GNU_SOURCE
/*
 * comunicaciones.c
 *
 * Communications module for the Resource Manager Agent.
 * Handles all networking logic: reading from non-blocking sockets,
 * message parsing, and dispatching to Erlang or remote nodes.
 *
 * Fixes applied over the original draft:
 *  - desglosar_instrucciones: now operates on a local copy so the
 *    original buffer is never destroyed. Callers no longer reference
 *    the undeclared variable `mensaje` or the undeclared array `token[]`.
 *  - C_to_erlang: signature corrected to (int, const char*, const char*).
 *  - myserver_to_client: signature corrected; now receives erlangfd and epollfd.
 *  - erlang_to_C: call to myserver_to_client fixed; the @host:res:amount
 *    list in JOB_REQUEST is now parsed correctly.
 *  - client_to_myserver: token[] -> tokens[], mensaje -> instruction (local copy).
 *  - ANNOUNCE format without IP (image 2): IP is extracted from recvfrom().
 *  - Job timeouts implemented with a dedicated timerfd.
 *  - Consistent naming: socket_UDP instead of udp_socket.
 */

#include "comunicaciones.h"

/* ═══════════════════════════════════════════════════════════════════
 *  Global variables
 * ═══════════════════════════════════════════════════════════════════ */

fd_connection_state connections[MAX_FDS];

int socket_server;
int socket_erlang;
int socket_UDP;
int epollfd;
int erlangfd;




/*
 * Splits `instruction` into tokens delimited by space, '\n', or '\r'.
 * WARNING: modifies the string in place (uses strtok_r internally).
 * Returns the number of tokens found (at most max_tokens).
 */
int desglosar_instrucciones(char *instruction, char **token_array, int max_tokens) {
    int i = 0;
    char *saveptr;

    char *t = strtok_r(instruction, " \n\r", &saveptr);
    while (t != NULL && i < max_tokens) {
        token_array[i++] = t;
        t = strtok_r(NULL, " \n\r", &saveptr);
    }
    return i;
}




void clear_connection_buffer(int fd) {
    if (fd >= 0 && fd < MAX_FDS) {
        connections[fd].accumulated_bytes = 0;
    }
}

/*
 * Reads bytes from a non-blocking socket and accumulates them until
 * a newline character '\n' is found.
 *
 * Returns:
 *   1  -> complete line ready in output_line
 *   0  -> partial data received, keep waiting
 *  -1  -> peer disconnected or critical error
 */
int read_until_newline(int fd, char *output_line) {
    if (fd < 0 || fd >= MAX_FDS) return -1;

    char  *storage = connections[fd].buffer;
    int   *acc     = &connections[fd].accumulated_bytes;

    char temp[256];
    int  n = recv(fd, temp, sizeof(temp) - 1, 0);

    if (n == 0) return -1;   /* Peer closed the connection */
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        return -1;
    }

    /* Guard against accumulator buffer overflow */
    if (*acc + n >= BUFFER_LEN) {
        fprintf(stderr, "[WARN] read_until_newline: buffer overflow on fd=%d, resetting\n", fd);
        *acc = 0;
        return -1;
    }

    memcpy(&storage[*acc], temp, n);
    *acc += n;
    storage[*acc] = '\0';

    char *nl = strchr(storage, '\n');
    if (nl != NULL) {
        int line_len = (nl - storage) + 1;
        memcpy(output_line, storage, line_len);
        output_line[line_len] = '\0';

        int remaining = *acc - line_len;
        if (remaining > 0) {
            memmove(storage, &storage[line_len], remaining);
        }
        *acc = remaining;
        return 1;
    }
    return 0;
}



/*
 * Sends a response to the Erlang scheduler process.
 * instruction: "granted" | "rejected" | "waiting" | "timeout"
 */
void C_to_erlang(int fd, const char *instruction, const char *job_id) {
    char msg[LENG];
    int  n;

    if      (!strcmp(instruction, "granted"))  n = snprintf(msg, sizeof(msg), "JOB_GRANTED %s\n",  job_id);
    else if (!strcmp(instruction, "rejected")) n = snprintf(msg, sizeof(msg), "JOB_DENIED %s\n",   job_id);
    else if (!strcmp(instruction, "waiting"))  n = snprintf(msg, sizeof(msg), "WAITING %s\n",      job_id);
    else                                       n = snprintf(msg, sizeof(msg), "JOB_TIMEOUT %s\n",  job_id);

    if (n < 0 || n >= (int)sizeof(msg)) {
        fprintf(stderr, "[ERROR] C_to_erlang: message truncated\n");
        return;
    }

    if (send(fd, msg, n, MSG_DONTWAIT) < 0) {
        perror("[ERROR] C_to_erlang: send");
    }
}

/* ═══════════════════════════════════════════════════════════════════
 *  Incoming message from a remote node -> respond or update tables
 * ═══════════════════════════════════════════════════════════════════ */

/*
 * Processes an incoming TCP message from another remote C agent.
 *
 * Expected messages (node-to-node protocol §4.1):
 *   RESERVE <job_id> <resource> <amount>
 *   RELEASE <job_id> <resource> <amount>
 *   GRANTED <job_id>
 *   DENIED  <job_id>
 *
 * fd            -> FD of the remote node that sent the message
 * erlangfd_local -> FD of the local Erlang connection (for JOB_GRANTED/DENIED)
 * instruction   -> the already-read line (will be modified by strtok_r)
 */
void client_to_myserver(int erlangfd_local, int fd, char *instruction) {
    /* Work on a copy to avoid destroying the original buffer */
    char copy[BUFFER_LEN];
    strncpy(copy, instruction, sizeof(copy) - 1);
    copy[sizeof(copy) - 1] = '\0';

    char *tokens[10];
    int   num = desglosar_instrucciones(copy, tokens, 10);
    if (num < 1) return;

    /* ── RESERVE: a remote node is requesting a local resource ──────── */
    if (!strcmp(tokens[0], "RESERVE")) {
        if (num < 4) {
            fprintf(stderr, "[WARN] Malformed RESERVE: %s\n", instruction);
            return;
        }

        char *job_id_str = tokens[1];
        char *resource   = tokens[2];
        int   amount     = atoi(tokens[3]);
        int   job_id     = atoi(job_id_str);

        /*
         * TODO (resource management teammate):
         *   int ok = try_reserve(resource, amount);
         * Placeholder for now: always returns 0 (denied).
         */
        int ok = 0; /* placeholder */

        if (ok) {
            /* Register in tabla_clientes so we can release on disconnection */
            job_entry *job = FindJob(&tabla_clientes, job_id);
            if (job == NULL) {
                job = MakeJob(job_id, fd, time(NULL));
                strncpy(job->resource_req, resource, sizeof(job->resource_req) - 1);
                job->amount_req = amount;
                JobsTableInsert(&tabla_clientes, job);
            }
            granted_t *res = MakeGranted(resource, amount);
            if (res) AddResource(job, res);
        }

        /* Send immediate reply to the remote node */
        char resp[LENG];
        int  n = snprintf(resp, sizeof(resp),
                          ok ? "GRANTED %s\n" : "DENIED %s\n", job_id_str);
        send(fd, resp, n, MSG_DONTWAIT);
    }

    /* ── RELEASE: the remote node is freeing a resource we granted it ── */
    else if (!strcmp(tokens[0], "RELEASE")) {
        if (num < 2) return;

        int job_id = atoi(tokens[1]);

        /*
         * TODO (resource management teammate):
         *   release_resource(resource, amount);
         */

        job_entry *job = FindJob(&tabla_clientes, job_id);
        if (job != NULL) {
            RemoveJob(&tabla_clientes, job_id);
        }
    }

    /* ── GRANTED / DENIED: response to a RESERVE we sent ─────────────── */
    else if (!strcmp(tokens[0], "GRANTED") || !strcmp(tokens[0], "DENIED")) {
        if (num < 2) return;

        char *job_id_str = tokens[1];
        int   job_id     = atoi(job_id_str);

        job_entry *job = FindJob(&tabla_propia, job_id);
        if (job != NULL) {
            const char *result = !strcmp(tokens[0], "GRANTED") ? "granted" : "rejected";
            C_to_erlang(erlangfd_local, result, job_id_str);

            /*
             * This outgoing fd has served its purpose.
             * Remove it from epoll and close it.
             * (We have access to the global epollfd here.)
             */
            epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, NULL);
            close(fd);
            clear_connection_buffer(fd);
            RemoveJob(&tabla_propia, job_id);
        }
    }

    else {
        fprintf(stderr, "[WARN] client_to_myserver: unknown command '%s'\n", tokens[0]);
    }
}



/*
 * Opens a non-blocking TCP connection to a remote node, registers it
 * in epoll, and sends the given instruction (RESERVE or RELEASE).
 *
 * erlangfd_local -> used to forward errors to Erlang if the job is missing
 * epollfd_local  -> shared epoll instance
 * instruction    -> "reserve <job_id>" or "release <job_id>"
 *
 * The destination IP, port, resource name, and amount are read from the
 * job_entry that must have been inserted into tabla_propia by erlang_to_C().
 */
void myserver_to_client(int erlangfd_local __attribute__((unused)), int epollfd_local, char *instruction) {
    char copy[BUFFER_LEN];
    strncpy(copy, instruction, sizeof(copy) - 1);
    copy[sizeof(copy) - 1] = '\0';

    char *tokens[10];
    int   num = desglosar_instrucciones(copy, tokens, 10);

    if (num < 2) {
        fprintf(stderr, "[ERROR] myserver_to_client: missing arguments in '%s'\n", instruction);
        return;
    }

    const char *command    = tokens[0];
    int         job_id_int = atoi(tokens[1]);

    //TODO
    // job_entry *job = FindJob(&tabla_propia, job_id_int);
    // if (job == NULL) {
    //     fprintf(stderr, "[ERROR] myserver_to_client: job %d not found in tabla_propia\n", job_id_int);
    //     return;
    // }

    /* 1. Create a non-blocking TCP socket */
    int remote_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (remote_fd < 0) {
        perror("[ERROR] myserver_to_client: socket()");
        return;
    }

    /* 2. Build the destination address from the job_entry */
    struct sockaddr_in remote_addr;
    memset(&remote_addr, 0, sizeof(remote_addr));
    remote_addr.sin_family = AF_INET;
    remote_addr.sin_port   = htons(job->dest_port);
    inet_pton(AF_INET, job->dest_ip, &remote_addr.sin_addr);

    /* 3. Non-blocking connect (EINPROGRESS is expected and safe) */
    int conn_res = connect(remote_fd, (struct sockaddr *)&remote_addr, sizeof(remote_addr));
    if (conn_res < 0 && errno != EINPROGRESS) {
        perror("[ERROR] myserver_to_client: connect()");
        close(remote_fd);
        return;
    }

    /* 4. Register in epoll:
     *    - EPOLLIN     -> to read the incoming GRANTED/DENIED reply
     *    - EPOLLOUT    -> to detect when the async connect completes
     *    - EPOLLET     -> edge-triggered (consistent with the overall design)
     *    - EPOLLONESHOT -> only one thread processes this fd at a time     */
    struct epoll_event ev;
    ev.events  = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLONESHOT;
    ev.data.fd = remote_fd;

    if (epoll_ctl(epollfd_local, EPOLL_CTL_ADD, remote_fd, &ev) < 0) {
        perror("[ERROR] myserver_to_client: epoll_ctl ADD");
        close(remote_fd);
        return;
    }

    /* 5. Format and send the message based on the command */
    char payload[512];
    int  plen = 0;

    if (strcasecmp(command, "reserve") == 0) {
        plen = snprintf(payload, sizeof(payload),
                        "RESERVE %d %s %d\n",
                        job->job_id, job->resource_req, job->amount_req);
    } else if (strcasecmp(command, "release") == 0) {
        plen = snprintf(payload, sizeof(payload),
                        "RELEASE %d %s %d\n",
                        job->job_id, job->resource_req, job->amount_req);
    } else {
        fprintf(stderr, "[WARN] myserver_to_client: unknown command '%s'\n", command);
        epoll_ctl(epollfd_local, EPOLL_CTL_DEL, remote_fd, NULL);
        close(remote_fd);
        return;
    }

    if (plen > 0) {
        ssize_t sent = send(remote_fd, payload, plen, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (sent < 0 && errno != EAGAIN) {
            /* Do not close: the kernel may still buffer the message for delivery */
            perror("[WARN] myserver_to_client: partial or failed send");
        }
        printf("[P2P OUT] %s", payload);
    }
}

/* ═══════════════════════════════════════════════════════════════════
 *  Erlang -> C  (local command from the scheduler)
 * ═══════════════════════════════════════════════════════════════════ */

/*
 * Processes a command received from the Erlang scheduler (§4.2):
 *
 *   JOB_REQUEST <job_id> [@host:res:amount ...]
 *   JOB_RELEASE <job_id>
 *   JOB_STATUS  <job_id>   (responds WAITING for now)
 */
void erlang_to_C(int erlangfd_local, char *instruction) {
    char copy[BUFFER_LEN];
    strncpy(copy, instruction, sizeof(copy) - 1);
    copy[sizeof(copy) - 1] = '\0';

    char *tokens[32];
    int   num = desglosar_instrucciones(copy, tokens, 32);
    if (num < 1) return;

    /* ── JOB_REQUEST ─────────────────────────────────────────────── */
    if (!strcmp(tokens[0], "JOB_REQUEST")) {
        if (num < 2) {
            fprintf(stderr, "[WARN] JOB_REQUEST missing job_id\n");
            return;
        }

        char *job_id_str = tokens[1];
        int   job_id     = atoi(job_id_str);

        /*
         * Parse destination tokens: @host:resource:amount
         * Example: @192.168.1.2:cpu:2 @192.168.1.3:gpu:1
         *
         * For each destination we create a job_entry in tabla_propia
         * and fire an async RESERVE.
         *
         * Current simplification: only the first destination is processed.
         * Handling multiple destinations (waiting for all GRANTEDs before
         * replying to Erlang) requires a state machine or Erlang-side logic.
         */
        if (num < 3) {
            /* No remote destinations: resource must be local; delegate to resource manager */
            fprintf(stderr, "[WARN] JOB_REQUEST has no remote destinations for job %s\n", job_id_str);
            C_to_erlang(erlangfd_local, "rejected", job_id_str);
            return;
        }

        /* Parse the first destination: @ip:resource:amount */
        char dest_copy[256];
        strncpy(dest_copy, tokens[2], sizeof(dest_copy) - 1);
        dest_copy[sizeof(dest_copy) - 1] = '\0';

        char *p = dest_copy;
        if (*p == '@') p++;   /* skip the leading '@' */

        /* Split ip : resource : amount */
        char *dest_ip  = strtok(p,    ":");
        char *dest_res = strtok(NULL, ":");
        char *dest_amt = strtok(NULL, ":");

        if (!dest_ip || !dest_res || !dest_amt) {
            fprintf(stderr, "[WARN] Invalid destination format: %s\n", tokens[2]);
            C_to_erlang(erlangfd_local, "rejected", job_id_str);
            return;
        }

        /* Insert job into tabla_propia before opening the outgoing connection */
        job_entry *job = MakeJob(job_id, erlangfd_local, time(NULL));
        if (!job) { C_to_erlang(erlangfd_local, "rejected", job_id_str); return; }

        strncpy(job->dest_ip,      dest_ip,  sizeof(job->dest_ip)      - 1);
        job->dest_port = PORT;   /* All agents listen on the same PORT */
        strncpy(job->resource_req, dest_res, sizeof(job->resource_req) - 1);
        job->amount_req = atoi(dest_amt);

        JobsTableInsert(&tabla_propia, job);

        /* Fire the outgoing connection and send RESERVE */
        char reserve_cmd[128];
        snprintf(reserve_cmd, sizeof(reserve_cmd), "reserve %d", job_id);
        myserver_to_client(erlangfd_local, epollfd, reserve_cmd);

        /* The GRANTED/DENIED reply arrives asynchronously in
         * client_to_myserver(), which then calls C_to_erlang(). */
    }

    /* ── JOB_RELEASE ─────────────────────────────────────────────── */
    else if (!strcmp(tokens[0], "JOB_RELEASE")) {
        if (num < 2) return;

        int job_id = atoi(tokens[1]);
        char release_cmd[128];
        snprintf(release_cmd, sizeof(release_cmd), "release %d", job_id);
        myserver_to_client(erlangfd_local, epollfd, release_cmd);

        RemoveJob(&tabla_propia, job_id);
    }

    /* ── JOB_STATUS ──────────────────────────────────────────────── */
    else if (!strcmp(tokens[0], "JOB_STATUS")) {
        if (num < 2) return;
        C_to_erlang(erlangfd_local, "waiting", tokens[1]);
    }
    

    else {
        fprintf(stderr, "[WARN] erlang_to_C: unknown command '%s'\n", tokens[0]);
    }
}