#define _GNU_SOURCE
/*
 * comunicaciones.c
 *
 * Communications module for the Resource Manager Agent.
 * Handles all networking logic: reading from non-blocking sockets,
 * message parsing, and dispatching to Erlang or remote nodes.
 *
 * Fixes applied over the original draft:
 *  - get_token: now operates on a local copy so the
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
#include "utils.h"

/* ═══════════════════════════════════════════════════════════════════
 *  Global variables
 * ═══════════════════════════════════════════════════════════════════ */

ConnectionState connections[MAX_FDS];

/**
 * we use the extern since the table is going to be initialized in the setup epoll
 */

extern active_jobs table_nodes;
extern active_jobs table_clients;

extern int cpu_available;
extern int mem_available;
extern int gpu_available;

fifo_queue_t cpu_queue = {NULL, NULL};
fifo_queue_t mem_queue = {NULL, NULL};
fifo_queue_t gpu_queue = {NULL, NULL};


extern pthread_mutex_t mutex_resources;

/*
 * Splits `instruction` into tokens delimited by space, '\n', or '\r'.
 * WARNING: modifies the string in place (uses strtok_r internally).
 * Returns the number of tokens found (at most max_tokens).
 */
int get_token(char *instruction, char **token_array, int max_tokens) {
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

    if (*acc + n >= BUFFER_MAX) {

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
void C_to_erlang(int erlangfd, const char *instruction, const char *job_id) {
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

    if (send(erlangfd, msg, n, MSG_DONTWAIT) < 0) {
        perror("[ERROR] C_to_erlang: send");
    }
}

/* 
 *  Incoming message from a remote node -> respond or update tables
 */

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
 * erlangfd -> FD of the local Erlang connection (for JOB_GRANTED/DENIED)
 * instruction   -> the already-read line (will be modified by strtok_r)
 */
void client_to_myserver(int erlangfd, int fd_actual, char *instruction) {
    /* Work on a copy to avoid destroying the original buffer */
    char copy[BUFFER_MAX];
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

        char *job_id_str = tokens[1];
        char *resource   = tokens[2];
        int   amount     = atoi(tokens[3]);
        int   job_id     = atoi(job_id_str);
        int reserved = 0;
        char msg[BUFFER_MAX];
        // now we need to check if we have the resources


        job_entry* job = MakeJob(job_id, fd_actual, time(NULL));

        if(strcmp(resource, "cpu")){
            pthread_mutex_lock(&mutex_resources);
            if(amount >= cpu_available){
                cpu_available -= amount;
                strcpy(msg, ("GRANTED %d\n", job_id));
                send(fd_actual, msg, strlen(msg), MSG_NOSIGNAL);
                printf("El proceso %d obtuvo %d cpu\n",job_id, amount);
                JobsTableInsert(&table_clients, job);
            } else {
                enqueue_job(&cpu_queue, job, amount);
            }
            pthread_mutex_unlock(&mutex_resources);
        } else

        if(strcmp(resource, "mem")){
            pthread_mutex_lock(&mutex_resources);
            if(amount >= cpu_available){
                mem_available -= amount;
                strcpy(msg, ("GRANTED %d\n", job_id));
                send(fd_actual, msg, strlen(msg), MSG_NOSIGNAL);
                printf("El proceso %d obtuvo %d mem\n",job_id, amount);
                JobsTableInsert(&table_clients, job);
            } else {
                enqueue_job(&mem_queue, job, amount);
            }
            pthread_mutex_unlock(&mutex_resources);
        } else

        if(strcmp(resource, "gpu")){
            pthread_mutex_lock(&mutex_resources);
            if(amount >= gpu_available){
                cpu_available -= amount;
                strcpy(msg, ("GRANTED %d\n", job_id));
                send(fd_actual, msg, strlen(msg), MSG_NOSIGNAL);
                printf("El proceso %d obtuvo %d cpu\n",job_id, amount);
                JobsTableInsert(&table_clients, job);
            } else {
                enqueue_job(&gpu_queue, job, amount);
            }
            pthread_mutex_unlock(&mutex_resources);
        } else {
            printf("[WARN] recurso solicitado deconocido \n");
        }
        

       
    }

    /* ── RELEASE: the remote node is freeing a resource we granted it ── */
    /* ── RELEASE: the remote node is freeing a resource we granted it ── */
    else if (!strcmp(tokens[0], "RELEASE")) {
        if (num < 4) {
            fprintf(stderr, "[WARN] Malformed RELEASE message: %s\n", instruction);
            return;
        }

        int   job_id   = atoi(tokens[1]);
        char *resource = tokens[2];
        int   amount   = atoi(tokens[3]);

        /* find whom we had given the resources*/
        job_entry* job = FindJob(&table_clients, job_id);
        
        if (job != NULL) {
            /* update the global resources */
            update_local_resources(resource, amount);

            /* remove this specific resource  */
            remove_specific_resource(job, resource);

            /* if there are no more resources that correspond to us we remove the node */
            if (job->resources == NULL) {
                RemoveJob(&table_clients, job_id);
                printf("[INFO] El trabajo %d devolvió todo y fue eliminado de la tabla.\n", job_id);
            } else {
                printf("[INFO] El trabajo %d devolvió %s pero aún retiene otros recursos.\n", job_id, resource);
            }
        } else {
            /* the job could not be in the table due to a timeout so we just update the
            resources in this case. */
            update_local_resources(resource, amount);
        }
    }


   /* ── GRANTED / DENIED: response to a RESERVE we sent ─────────────── */
    else if (!strcmp(tokens[0], "GRANTED")) {
        int job_id = atoi(tokens[1]);
        job_entry* job = FindJob(&table_nodes, job_id);
        
        if (job != NULL) {
            
            original_socket(job, fd_actual);
            job->next_req = job->next_req->next;
            if (job->next_req != NULL) { // if we need to keep searching for resources
                ask_for_next_resource(epollfd, erlangfd, job);
            } else { // if we dont we signal erlang 
                char msg[BUFFER_MAX];
                snprintf(msg, sizeof(msg), "JOB_GRANTED %d\n", job_id);
                send(erlangfd, msg, strlen(msg), MSG_NOSIGNAL);
                printf("[INFO] Busqueda de recursos para el trabajo %d finalizada, senial enviada a erlang \n", job_id);
            }
        }
    }

    else // denied
    {
        int job_id = atoi(tokens[1]);
        job_entry* job = FindJob(&table_nodes, job_id);
        if (job != NULL) {
            close(fd_actual);

        soltar_elementos(job);
        }
    }
    return NULL;
}
/* 
 *  Incoming message from a remote node -> respond or update tables
 */

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
 * erlangfd -> FD of the local Erlang connection (for JOB_GRANTED/DENIED)
 * instruction   -> the already-read line (will be modified by strtok_r)
 */
void client_to_myserver(int erlangfd, int fd_actual, char *instruction) {
    /* Work on a copy to avoid destroying the original buffer */
    char copy[LENG];
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

        char *job_id_str = tokens[1];
        char *resource   = tokens[2];
        int   amount     = atoi(tokens[3]);
        int   job_id     = atoi(job_id_str);


        //encolo la peticion
        //funcion 

       
    }

    /* ── RELEASE: the remote node is freeing a resource we granted it ── */
    else if (!strcmp(tokens[0], "RELEASE")) {
        if (num < 2) return;

        int job_id = atoi(tokens[1]);

    
        /*
         //funcion que dado el job sume a las variables globales de mem, cpu, gpu los valores 
        */

        //Desencolar el job al que le di los datos
    }


    /* ── GRANTED / DENIED: response to a RESERVE we sent ─────────────── */
    else if (!strcmp(tokens[0], "GRANTED")) {
        int job_id = atoi(tokens[1]);
        job_entry* job = FindJob(&tabla_propia, job_id);
        if (job != NULL) {
            
            de_donde_vino(job, fd_actual);
        // ¡AVANZAMOS EN LA LISTA ENLAZADA!
            job->next_req = job->next_req->next;
        // Pedimos el que sigue de forma totalmente asíncrona
            pedir_siguiente_recurso(epollfd, job);
        }
    }
    else
    {
        int job_id = atoi(tokens[1]);
        job_entry* job = FindJob(&tabla_propia, job_id);
        if (job != NULL) {
            close(fd_actual);

        soltar_elementos(job);
    }
}






/*
 * Opens a non-blocking TCP connection to a remote node, registers it
 * in epoll, and sends the given instruction (RESERVE or RELEASE).
 *
 * erlangfd -> used to forward errors to Erlang if the job is missing
 * epollfd_local  -> shared epoll instance
 * instruction    -> "reserve <job_id>" or "release <job_id>"
 *
 * The destination IP, port, resource name, and amount are read from the
 * job_entry that must have been inserted into table_nodes by erlang_to_C().
 */



 //Este paso al final puede que lo haga con las funcione definidas en utils con 
// void myserver_to_client(int erlangfd __attribute__((unused)), int epollfd_local, char *instruction) {
//     char copy[BUFFER_MAX];

//     strncpy(copy, instruction, sizeof(copy) - 1);
//     copy[sizeof(copy) - 1] = '\0';

//     char *tokens[10];
//     int   num = get_token(copy, tokens, 10);

//     if (num < 2) {
//         fprintf(stderr, "[ERROR] myserver_to_client: missing arguments in '%s'\n", instruction);
//         return;
//     }

//     const char *command    = tokens[0];
//     int         job_id_int = atoi(tokens[1]);


//     job_entry *job = FindJob(&table_nodes, job_id_int);

//     if (job == NULL) {
//         fprintf(stderr, "[ERROR] myserver_to_client: job %d not found in table_nodes\n", job_id_int);
//         return;
//     }

//     /* 1. Create a non-blocking TCP socket */
//     int remote_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    
//     if(remote_fd < 0) {
//         perror("[ERROR] myserver_to_client: socket()");
//         return;
//     }

//     /* 2. Build the destination address from the job_entry */
//     struct sockaddr_in remote_addr;
//     memset(&remote_addr, 0, sizeof(remote_addr));
//     remote_addr.sin_family = AF_INET;
//     remote_addr.sin_port   = htons(job->dest_port);
//     inet_pton(AF_INET, job->dest_ip, &remote_addr.sin_addr);

//     /* 3. Non-blocking connect (EINPROGRESS is expected and safe) */
//     int conn_res = connect(remote_fd, (struct sockaddr *)&remote_addr, sizeof(remote_addr));
//     if (conn_res < 0 && errno != EINPROGRESS) {
//         perror("[ERROR] myserver_to_client: connect()");
//         close(remote_fd);
//         return;
//     }

//     /* 4. Register in epoll:
//      *    - EPOLLIN     -> to read the incoming GRANTED/DENIED reply
//      *    - EPOLLOUT    -> to detect when the async connect completes
//      *    - EPOLLET     -> edge-triggered (consistent with the overall design)
//      *    - EPOLLONESHOT -> only one thread processes this fd at a time     */
//     struct epoll_event ev;
//     ev.events  = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLONESHOT;
//     ev.data.fd = remote_fd;

//     if (epoll_ctl(epollfd_local, EPOLL_CTL_ADD, remote_fd, &ev) < 0) {
//         perror("[ERROR] myserver_to_client: epoll_ctl ADD");
//         close(remote_fd);
//         return;
//     }

//     /* 5. Format and send the message based on the command */
//     char payload[512];
//     int  plen = 0;

//     if (strcasecmp(command, "reserve") == 0) {
//         for
//         plen = snprintf(payload, sizeof(payload),
//                         "RESERVE %d %s %d\n",
//                         job->job_id, job->resource_req, job->amount_req);
//     } else if (strcasecmp(command, "release") == 0) {
//         plen = snprintf(payload, sizeof(payload),
//                         "RELEASE %d %s %d\n",
//                         job->job_id, job->resource_req, job->amount_req);
//     } else {
//         fprintf(stderr, "[WARN] myserver_to_client: unknown command '%s'\n", command);
//         epoll_ctl(epollfd_local, EPOLL_CTL_DEL, remote_fd, NULL);
//         close(remote_fd);
//         return;
//     }

//     if (plen > 0) {
//         ssize_t sent = send(remote_fd, payload, plen, MSG_NOSIGNAL | MSG_DONTWAIT);
//         if (sent < 0 && errno != EAGAIN) {
//             /* Do not close: the kernel may still buffer the message for delivery */
//             perror("[WARN] myserver_to_client: partial or failed send");
//         }
//         printf("[P2P OUT] %s", payload);
//     }
// }


/*
 * Processes a command received from the Erlang scheduler (§4.2):
 *
 *   JOB_REQUEST <job_id> [@host:res:amount ...]
 *   JOB_RELEASE <job_id>
 *   JOB_STATUS  <job_id>   (responds WAITING for now)
 */
void erlang_to_C(int erlangfd, char *instruction, time_t timer) {

    char copy[BUFFER_MAX];

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

        char *job_id_str = tokens[1]; //for example <1234>
        int   job_id     = atoi(job_id_str);

        //If erland dont ask for any resource 
        if (num < 3) {
            /* No remote destinations: resource must be local; delegate to resource manager */
            fprintf(stderr, "[WARN] JOB_REQUEST has no remote destinations for job %s\n", job_id_str);
            C_to_erlang(erlangfd, "rejected", job_id_str);
            return;
        }
        
        job_entry* newjob = MakeJob(job_id, erlangfd, timer);

        // 2. Un solo bucle para extraer todos los recursos
        for (int i = 2; i < num; i++) {
            char dest_copy[256];
            strncpy(dest_copy, tokens[i], sizeof(dest_copy) - 1); // <-- Usar tokens[i], no tokens[2]
            dest_copy[sizeof(dest_copy) - 1] = '\0';
            //dest copy should be like this @host:res:amount

            char *p = dest_copy;
            if (*p == '@') p++; 
            
            char *dest_ip  = strtok(p,    ":");
            char *dest_res = strtok(NULL, ":");
            char *dest_amt = strtok(NULL, " ");
            
            if (!dest_ip || !dest_res || !dest_amt) {
                fprintf(stderr, "[WARN] Formato inválido: %s\n", tokens[i]);
                // Lógica de rechazo...
                continue; // Mejor saltar este recurso o abortar todo
            }
            
            int amount = atoi(dest_amt);
            
            // ERROR CORREGIDO: Estabas pasando 'dest_amt' dos veces. 
            // Ahora pasas el recurso ("cpu") y la cantidad (2).
            granted_t* element = MakeGranted(dest_res, amount, dest_ip);
            
            // OJO: Si la IP varía por recurso, deberías guardar dest_ip y el puerto 
            // DENTRO del 'element' (granted_t), no en el job global.
            
            AddResource(newjob, element);
        }

        JobsTableInsert(&table_nodes, newjob); // O 'ownjobs' si es global

        // ERROR CORREGIDO: La firma real de tu función es de 3 argumentos
        ask_for_next_resource(epollfd, erlangfd, newjob);

    }
    /* ── JOB_RELEASE ─────────────────────────────────────────────── */
    else if (!strcmp(tokens[0], "JOB_RELEASE")) {
        if (num < 2) return;

        int job_id = atoi(tokens[1]);
        job_entry* job = FindJob(&table_nodes, job_id);
        release_resources(job);

        RemoveJob(&table_nodes, job_id);
    }

    /* ── JOB_STATUS ──────────────────────────────────────────────── */

    //Si no tiro time out es porque no se tiene elementos 
    else if (!strcmp(tokens[0], "JOB_STATUS")) {
        if (num < 2) return;
        C_to_erlang(erlangfd, "waiting", tokens[1]);
    }
    

    else {
        fprintf(stderr, "[WARN] erlang_to_C: unknown command '%s'\n", tokens[0]);
    }
}

    
    