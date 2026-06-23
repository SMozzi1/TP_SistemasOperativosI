//#define _GNU_SOURCE //actualmente compilamos con -D_GNU_SOURC, si no estuviera, se pondria esta linea
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

 //Comentadas pero estan el globals.h
// extern active_jobs table_nodes;
//A esta 
// extern active_jobs table_clients;

// extern int cpu_available;
// extern int mem_available;
// extern int gpu_available;

// fifo_queue_t cpu_queue = {NULL, NULL};
// fifo_queue_t mem_queue = {NULL, NULL};
// fifo_queue_t gpu_queue = {NULL, NULL};

//extern pthread_mutex_t mutex_resources;




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





void ask_for_next_resource(job_entry* job)
{
    // CASO BASE: Si ya no hay más recursos en la lista, terminamos con éxito
    if (job->next_req == NULL)
    {
        job->next_req = NULL;
        char id_str[16];
        snprintf(id_str, sizeof(id_str), "%d", job->job_id);
        C_to_erlang( "granted", id_str);
        return;

    }
    else
    {
        //creamos un socket para mandar mensajes 
        int remote_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);

        if (remote_fd < 0) {
            perror("[ERROR] pedir_elementos: socket()");
            return;
        }
        job->origin_socket = remote_fd;
        //Cada recurso tendra un id de donde viene, el puerto se tiene que buscar de la tabla,
        //Si no esta entonces tengo que hacer job_rejected
        struct sockaddr_in remote_addr;
        memset(&remote_addr, 0, sizeof(remote_addr));
        remote_addr.sin_family = AF_INET;
        remote_addr.sin_port   = htons(job->next_req->dest_port);
        inet_pton(AF_INET, job->next_req->dest_ip, &remote_addr.sin_addr);
        
        int conn_res = connect(remote_fd, (struct sockaddr *)&remote_addr, sizeof(remote_addr));
        if (conn_res < 0 && errno != EINPROGRESS) {
            perror("[ERROR] pedir_elementos: connect()");
            close(remote_fd);
            return;
        }
        
        // Registramos en epoll. 
        // EPOLLOUT se disparará en cuanto la conexión TCP se complete con éxito.
        struct epoll_event ev;
        ev.events  = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLONESHOT;
        ev.data.fd = remote_fd;
        if (epoll_ctl(epollfd, EPOLL_CTL_ADD, remote_fd, &ev) < 0) {
            perror("[ERROR] epoll_ctl ADD");
            close(remote_fd);
            return;
        }
        
        //Se crea un socket con EPOLLOUT que reaccionara cuando la conexion este lista para mandar el mensaje de reserve
        //el send esta en la funcion event_loop, cuando se pueda mandar el mensaje de reserve, se manda y se cambia el epoll a EPOLLIN para esperar la respuesta del nodo remoto
    }
}

/*
 * Sends a response to the Erlang scheduler process.
 * instruction: "granted" | "rejected" | "waiting" | "timeout"
 */
void C_to_erlang(const char *instruction, const char *job_id) {
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


void client_to_myserver(int fd_actual, char *instruction) {    
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
        //Enqueue ya ejecuta reserve_elements
        enqueue_jobs(resource, job_id, amount, fd_actual);
        //reserve_elements();
    }
    /* ── RELEASE: the remote node is freeing a resource we granted it ── */
    else if (!strcmp(tokens[0], "RELEASE")) {
        if (num >= 2) printf("[SERVER] RELEASE job %s en fd=%d\n", tokens[1], fd_actual);
            release_client_by_fd(fd_actual);   // la clave es el socket, no el job_id
    }
    /* ── GRANTED / DENIED: response to a RESERVE we sent ─────────────── */
    else if (!strcmp(tokens[0], "GRANTED")) {
        if (num < 2) return;
        int job_id = atoi(tokens[1]);
        job_entry* job = FindJob(&table_ourjobs, job_id);
        if (job != NULL && job->next_req != NULL) {
            original_socket(job, fd_actual);
            job->next_req = job->next_req->next;
            ask_for_next_resource(job);
        }
    }
    else
{
    if (num < 2) {
        fprintf(stderr, "[WARN] Mensaje desconocido o malformado: %s\n", instruction);
        return;
    }
    int job_id = atoi(tokens[1]);
    job_entry* job = FindJob(&table_ourjobs, job_id);
    if (job != NULL) {
        close(fd_actual);
        release_resources(job);

        char id_str[16];
        snprintf(id_str, sizeof(id_str), "%d", job_id);
        C_to_erlang("rejected", id_str);
        RemoveJob(&table_ourjobs, job_id);
    }
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


/*
 * Processes a command received from the Erlang scheduler (§4.2):
 *
 *   JOB_REQUEST <job_id> [@host:res:amount ...]
 *   JOB_RELEASE <job_id>
 *   JOB_STATUS  <job_id>   (responds WAITING for now)
 */
void erlang_to_C(char *instruction, time_t timer) {

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
            C_to_erlang("rejected", job_id_str);
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
            
            // Creamos el recurso
            granted_t* element = MakeGranted(dest_res, amount, dest_ip);
            
            // BÚSQUEDA O(1): Convertimos la IP de Erlang y buscamos en la tabla
            int target_ip_int = abs((int)inet_addr(dest_ip));
            job_entry* remote_node = FindJob(&table_nodes, target_ip_int);
            
            if (remote_node != NULL) {
                // Si encontramos al vecino, sacamos el puerto real (que guardaste en origin_socket)
                element->dest_port = remote_node->origin_socket; 
            } else {
                element->dest_port = 4200; // Fallback por si el nodo recién arranca
            }
            
            AddResource(newjob, element);
        }

        JobsTableInsert(&table_ourjobs, newjob); // O 'ownjobs' si es global

        // ERROR CORREGIDO: La firma real de tu función es de 1 argumento
        ask_for_next_resource(newjob);

    }
    /* ── JOB_RELEASE ─────────────────────────────────────────────── */
    else if (!strcmp(tokens[0], "JOB_RELEASE")) {
    if (num < 2) return;
    int job_id = atoi(tokens[1]);

    job_entry* job = FindJob(&table_ourjobs, job_id);   // antes: &table_nodes
    if (job == NULL) return;                            // ya no está, nada que hacer

    release_resources(job);                             // manda RELEASE a cada provider_fd
    RemoveJob(&table_ourjobs, job_id);                  // antes: &table_nodes
}

    /* ── JOB_STATUS ──────────────────────────────────────────────── */

    //Si no tiro time out es porque no se tiene elementos 
    else if (!strcmp(tokens[0], "JOB_STATUS")) {
    if (num < 2) return;

    int job_id = atoi(tokens[1]);
    job_entry* job = FindJob(&table_ourjobs, job_id);

    if (job == NULL) {
        // Ya no existe: se completó, se rechazó, o expiró.
        // No tenemos un estado "unknown" en el protocolo, así que avisamos timeout
        // (peor caso: que Erlang reintente el job, no que crea que sigue vivo).
        C_to_erlang("timeout", tokens[1]);
    } else if (job->next_req == NULL) {
        // Ya tiene TODOS los recursos otorgados, solo falta que Erlang lo sepa
        C_to_erlang("granted", tokens[1]);
    } else {
        C_to_erlang("waiting", tokens[1]);
    }
}


    /* ── GET_NODES ────────────────────────────────────────────────── */
    else if(!strcmp(tokens[0], "GET_NODES")){

        pthread_mutex_lock(&table_nodes.mutexTable);
        char* nodedata = obtener_string_nodos(table_nodes.job_table);
        pthread_mutex_unlock(&table_nodes.mutexTable);
        if (send(erlangfd, nodedata, strlen(nodedata), MSG_DONTWAIT) < 0) {
            perror("[ERROR] erlang_to_C: send GET_NODES response");
        }
    }
    
    else {
        fprintf(stderr, "[WARN] erlang_to_C: unknown command '%s'\n", tokens[0]);
    }
}

    
    


