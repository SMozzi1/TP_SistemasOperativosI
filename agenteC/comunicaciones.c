#include "comunicaciones.h"
#include "utils.h"


// asks for the next resource in the list of resources to be requested by the local job.
// it either sends a request to the next node or, if there are no more resources, it notifies the Erlang scheduler that the job has been granted.

void ask_for_next_resource(local_job_t* job)
{
    // If there are no more resources on the list, we have successfully completed our task.
    if (job->next_req == NULL)
    {
        job->next_req = NULL;
        char id_str[16];
        snprintf(id_str, sizeof(id_str), "%d", job->job_id);
        C_to_erlang( "granted", id_str);
        return;

    }

    //We move to the other resource we want to ask.
    else
    {
       
        //We create a socket to send messages, this message is gonna be processed by the loop in agente.c
        int remote_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);

        if (remote_fd < 0) {
            perror("[ERROR] pedir_elementos: socket()");
            return;
        }
        
        
        
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
        // we insert once we know the connection is establisshed
        job->origin_socket = remote_fd;
        // insert local job in the table
        fd_job_entry* job_entry = malloc(sizeof(fd_job_entry));
        job_entry->fd = remote_fd;
        job_entry->job = job;
        tablahash_insert(&table_nodejobs, job_entry);
        
        // Resister the socket in epoll;
        struct epoll_event ev;
        ev.events  = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLONESHOT;
        ev.data.fd = remote_fd;
        if (epoll_ctl(epollfd, EPOLL_CTL_ADD, remote_fd, &ev) < 0) {
            perror("[ERROR] epoll_ctl ADD");
            close(remote_fd);
            return;
        }
        
        //Creates an epoll event with EPOLLOUT, which will be triggered when the socket 
        //is ready to transmit data. This is used to initiate the resource reservation 
        //flow by sending the 'RESERVE' message.
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



//Cambiar estructuras
void client_to_myserver(int actual_fd, char *instruction) {    
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
        char *job_id_str = tokens[1];
        char *resource   = tokens[2];
        int   amount     = atoi(tokens[3]);
        int   job_id     = atoi(job_id_str);
        
        //every time we add a job to the queue it tryes to reserve_elements
        enqueue_jobs(resource, job_id, amount, actual_fd);
        
    }
    /* ── RELEASE: the remote node is freeing a resource we granted it ── */
    else if (!strcmp(tokens[0], "RELEASE")) {
        if (num >= 2) printf("[SERVER] RELEASE job %s en fd=%d\n", tokens[1], actual_fd);
            release_client_by_fd(actual_fd);   
    }
    /* ── GRANTED / DENIED: response to a RESERVE we sent ─────────────── */
    else if (!strcmp(tokens[0], "GRANTED")) {
        if (num < 2) return;
        int received_job_id = atoi(tokens[1]);
        fd_job_entry* dummy;
        dummy->fd = actual_fd;
        fd_job_entry* found = (fd_job_entry*) tablahash_buscar(&table_fdjobs, dummy);

        if (found != NULL && found->job != NULL) {
            local_job_t* job = found->job;
            if(job->job_id == received_job_id){
                pending_resource_t* req = job -> next_req;
                if(req != NULL){
                    job->next_req = req->next; 
                    req->next = job->granted_reqs;
                    job->granted_reqs = req;
                }
                printf("[SERVER] Job %d: recurso otorgado por fd=%d\n", job->job_id, actual_fd);
                tabla_hash_eliminar(&table_fdjobs, dummy); // we do this since ask_for_next_resource will insert another fd if we ask for another resource to another node
                ask_for_next_resource(job);
            }
        }

    }
    else
    {
        /*This section acts as a recovery routine that, in the event of an unknown message or a rejection, 
        closes the connection, releases the resources locked by the job, notifies the Erlang scheduler of the failure, 
        and removes the job from the local table.*/
        
        if (num < 2) {
            fprintf(stderr, "[WARN] Mensaje desconocido o malformado: %s\n", instruction);
            return;
        }

        int job_id = atoi(tokens[1]);
        job_entry* job = FindJob(&table_ourjobs, job_id);
        if (job != NULL) {
            close(actual_fd);

            pthread_mutex_lock(&table_ourjobs.mutexTable);
            release_resources(job);
            pthread_mutex_unlock(&table_ourjobs.mutexTable);

            char id_str[16];
            snprintf(id_str, sizeof(id_str), "%d", job_id);
            C_to_erlang("rejected", id_str);
            RemoveJob(&table_ourjobs, job_id);
        }
        ReleaseJob(job);
    }
}


//Cambiar estructuras
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

        char *job_id_str = tokens[1]; //for example <1234>
        int   job_id     = atoi(job_id_str);

        //If erland dont ask for any resource 
        if (num < 3) {
            /* No remote destinations: resource must be local; delegate to resource manager */
            fprintf(stderr, "[WARN] JOB_REQUEST has no remote destinations for job %s\n", job_id_str);
            C_to_erlang("rejected", job_id_str);
            return;
        }
        
        local_job_t* newjob = malloc(sizeof(local_job_t));
        newjob->job_id = job_id;
        newjob->erlang_socket = erlangfd;
        newjob->next_req = NULL;
        newjob->granted_reqs = NULL;
        newjob->timer = timer;
        newjob->origin_socket = -1; // Initialize to an invalid value

        pending_resource_t* tail = NULL; // auxiliar pointer to build the linked list of resources

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
            
            // we create the resource entry and add it to the linked list of resources for this job
            pending_resource_t* req = malloc(sizeof(pending_resource_t));
            strncpy(req->dest_ip, dest_ip, sizeof(req->dest_ip) - 1);
            req->dest_ip[sizeof(req->dest_ip) - 1] = '\0';
            strncpy(req->type, dest_res, sizeof(req->type) - 1);
            req->type[sizeof(req->type) - 1] = '\0';
            req->amount = atoi(dest_amt);
            req->next = NULL;

            // BÚSQUEDA O(1): Convertimos la IP de Erlang y buscamos en la tabla
            int target_ip_int = abs((int)inet_addr(dest_ip));
            received_node* dummy_node = malloc(sizeof(received_node));
            dummy_node->ip = target_ip_int;
            received_node* remote_node = (received_node*) tablahash_buscar(&table_nodes, dummy_node);
            free(dummy_node);

            if (remote_node != NULL) {
                // Si encontramos al vecino, sacamos el puerto real (que guardaste en origin_socket)
                req->dest_port = remote_node->port; 
            } else {
                req->dest_port = 4200; // Fallback por si el nodo recién arranca
            }
            
            // we add it to the end of the linked list of resources for this job
            if (newjob->next_req == NULL) {
                newjob->next_req = req;
            } else {
                tail->next = req;
            }
            tail = req;
            
            
        }

        tablahash_insertar(&table_ourjobs, newjob); // O 'ownjobs' si es global

        // ERROR CORREGIDO: La firma real de tu función es de 1 argumento
        ask_for_next_resource(newjob); // we ask for the first resource, the rest will be asked in a chain

        free(newjob); //since the implementation of the insert int the table already makes a copy of the job, we can free the original one.
    }
    /* ── JOB_RELEASE ─────────────────────────────────────────────── */
    else if (!strcmp(tokens[0], "JOB_RELEASE")) {
    if (num < 2) return;
    int job_id = atoi(tokens[1]);

    job_entry* job = FindJob(&table_ourjobs, job_id);   // antes: &table_nodes
    if (job == NULL) return;                            // ya no está, nada que hacer

    pthread_mutex_lock(&table_ourjobs.mutexTable);
    release_resources(job);                             // manda RELEASE a cada provider_fd
    pthread_mutex_unlock(&table_ourjobs.mutexTable);
    RemoveJob(&table_ourjobs, job_id);                  // antes: &table_nodes
    ReleaseJob(job);
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
    ReleaseJob(job);
}


    /* ── GET_NODES ────────────────────────────────────────────────── */
    else if(!strcmp(tokens[0], "GET_NODES")){

        pthread_mutex_lock(&table_nodes.mutexTable);
        char* nodedata = obtener_string_nodos(table_nodes.job_table);
        pthread_mutex_unlock(&table_nodes.mutexTable);
        if (send(erlangfd, nodedata, strlen(nodedata), MSG_DONTWAIT) < 0) {
            perror("[ERROR] erlang_to_C: send GET_NODES response");
        }
        free(nodedata);
    }
    
    else {
        fprintf(stderr, "[WARN] erlang_to_C: unknown command '%s'\n", tokens[0]);
    }
}

    
    


