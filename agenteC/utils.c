// #define _GNU_SOURCE

#include "utils.h"



#define JOB_TIMEOUT_SEC 30

#define NODE_TIMEOUT_SEC 15






//Timers
/*
 * Creates a periodic timerfd and registers it in epoll.
 * initial_sec:  seconds until the first expiration.
 * interval_sec: repeat period in seconds.
 */
int make_timer(int initial_sec, int interval_sec) {
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


void check_job_timeouts(active_jobs* tabla, int timeout_sec) {
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
                DestroyJob(j);

            } else {
                pp = &(*pp)->next_job;
            }
        }
    }

    pthread_mutex_unlock(&tabla->mutexTable);
}



//Used for the freedom of the resources
//and reused on the granted_t struct.
void update_local_resources(job_entry* job) {

    pthread_mutex_lock(&mutex_resources);
    if (strcmp(job->resources->type, "cpu") == 0) {
        cpu_available += job->resources->amount; 
        //process_queue(&cpu_queue, cpu_available, "cpu");
    } 
    else if (strcmp(job->resources->type, "mem") == 0) {
        mem_available += job->resources->amount;
        //process_queue(&mem_queue, mem_available, "mem");
    } 
    else if (strcmp(job->resources->type, "gpu") == 0) {
        gpu_available += job->resources->amount;
        //process_queue(&gpu_queue, gpu_available, "gpu");
    } 
    
    pthread_mutex_unlock(&mutex_resources);
}


//
void release_resources(job_entry* job)
{
    // flag to see if there is a job or any resources at all
    if (job == NULL || job->resources == NULL) {
        return;
    }

    granted_t* actual = job->resources;

    // we see all the resource until there are no resources left
    while (actual != NULL && actual != job->next_req) {
        
       
        if (actual->provider_fd >= 0) {
            char msg[256];
            snprintf(msg, sizeof(msg), "RELEASE %d %s %d\n", job->job_id, actual->type, actual->amount);
            
            // we send the message to the node that provided this resource
            send(actual->provider_fd, msg, strlen(msg), MSG_NOSIGNAL);
            
            printf("[INFO] Enviado RELEASE del recurso %s al FD %d para el Job %d\n", 
                    actual->type, actual->provider_fd, job->job_id);
            
            // we close the connection with the resource
            close(actual->provider_fd);
        
            // we set the fd as invalid.
            actual->provider_fd = -1;
        }
        
        // check the next resources
        actual = actual->next;
    }
}


//Asignation of where it came the memory, from which fd came the resource.

void original_socket(job_entry* job, int fd)
{
    if (job == NULL || job->next_req == NULL) {
        return;
    }

    // we asign the provider id to the resource
    job->next_req->provider_fd = fd;

    printf("[DEBUG] Asignado provider_fd = %d al recurso '%s' del Job %d\n", 
            fd, job->next_req->type, job->job_id);
}




// If a resource order arrived, it queues it into the corresponding queue, 
// later it calls reserve_elements to try to assign resourecs to the works on wait.
void enqueue_jobs(const char* resource, int job_id, int amount, int fd_actual) {
    p_request_t* rq = MakeRequest(job_id, fd_actual, amount);

    if(!strcmp(resource, "cpu")) {
        EnqueueRequest(&cpu_queue, rq);
    } else if (!strcmp(resource, "mem")) {
        EnqueueRequest(&mem_queue, rq);
    } else {
        EnqueueRequest(&gpu_queue, rq);
    }

    reserve_elements();
}

/*   Drains a queue: gives as much resources from the head as *avail lets.

 * Concurrency:
* - The check (*avail >= head->amount) and the two mutations (dequeue and
* avail -=) occur with BOTH locks held simultaneously ⇒ the decision is 
* atomic. This is what the previous version did wrong.
* - Lock order: mutex_resources -> mutexQueue. Nothing in the code takes them
* in reverse, so there's no deadlock.
* - The slow work (insert into table + send) happens AFTER both
* locks are released: we never hold mutex_resources while holding
* table_clients.mutexTable (see the note below, this matters).
 */
void drain_queue(p_queue_t* q, int* avail, const char* type) {
    p_request_t* ready = NULL;   /* stash, encadenado por next_req */

    pthread_mutex_lock(&mutex_resources);
    pthread_mutex_lock(&q->mutexQueue);
    while (q->first != NULL && *avail >= q->first->amount_requested) {
        p_request_t* req = DequeueRequest_locked(q);
        *avail -= req->amount_requested;
        req->next_req = ready;
        ready = req;
    }
    pthread_mutex_unlock(&q->mutexQueue);
    pthread_mutex_unlock(&mutex_resources);

    /* ---- outside the locks  ---- */
    while (ready != NULL) {
        p_request_t* req = ready;
        ready = ready->next_req;

        granted_t* g = MakeGranted((char*)type, req->amount_requested, "local");

        job_entry* je = FindJobBySocket(&table_clients, req->job_id, req->origin_socket);
        if (je == NULL) {
            je = MakeJob(req->job_id, req->origin_socket, time(NULL));
            AddResource(je, g);
            JobsTableInsert(&table_clients, je);
        } else {
            AddResource(je, g);
        }

        char msg[64];
        int n = snprintf(msg, sizeof(msg), "GRANTED %d\n", req->job_id);
        send(req->origin_socket, msg, n, MSG_NOSIGNAL);

        printf("[SERVER] Job %d: %s otorgada. Registrado en table_clients.\n",
               req->job_id, type);
        DestroyRequest(req);
    }
}

//Gives as many resources as available and it sends the granted message to the clients
void reserve_elements(void) {
    drain_queue(&cpu_queue, &cpu_available, "cpu");
    drain_queue(&mem_queue, &mem_available, "mem");
    drain_queue(&gpu_queue, &gpu_available, "gpu");
}


char* obtener_string_nodos(job_entry* table[]) {
    // Auxiliar temporal struct to group resources for a single Node.
    typedef struct {
        char ip[16];
        int port;
        int cpu;
        int mem;
        int gpu;
    } NodeSummary;

    // Temporal array to storage up to 128 unique remote nodes.
    NodeSummary unique_nodes[128];
    int node_count = 0;
    memset(unique_nodes, 0, sizeof(unique_nodes));

    // ─── 1. VISITS THE WHOLE HASH TABLE ───────────────────────────
    for (int i = 0; i < TABLE_SIZE; i++) {
        job_entry* current_job = table[i];

        // Visits the collisions list for chaining (next_job)
        while (current_job != NULL) {
            granted_t* res = current_job->resources;

            // Visits the linked lists of resources granted to that Job
            while (res != NULL) {
                int found_idx = -1;

                // Searchs to know if the IP:PORT combination is already registered
                for (int j = 0; j < node_count; j++) {
                    if (unique_nodes[j].port == res->dest_port && 
                        strcmp(unique_nodes[j].ip, res->dest_ip) == 0) {
                        found_idx = j;
                        break;
                    }
                }

               // If it doesnt exist, we create a new entry for this remote node.
                if (found_idx == -1) {
                    if (node_count < 128) {
                        strncpy(unique_nodes[node_count].ip, res->dest_ip, sizeof(unique_nodes[node_count].ip) - 1);
                        unique_nodes[node_count].port = res->dest_port;
                        found_idx = node_count;
                        node_count++;
                    } else {
                        fprintf(stderr, "[WARN] Se superó el límite de nodos únicos en el buffer temporal.\n");
                        break;
                    }
                }

                // Add the quantity to the corresponding resource
                if (strcmp(res->type, "cpu") == 0) {
                    unique_nodes[found_idx].cpu += res->amount;
                } else if (strcmp(res->type, "mem") == 0) {
                    unique_nodes[found_idx].mem += res->amount;
                } else if (strcmp(res->type, "gpu") == 0) {
                    unique_nodes[found_idx].gpu += res->amount;
                }

                res = res->next; // Next resource of the job
            }
            current_job = current_job->next_job; // Next job on the bucket
        }
    }

    // ─── 2. BUILD THE DYNAMIC STRING ──────────────────────────────
    // We reserve a wide buffer on dynamic memory (8 KB) to make the answer.
    size_t buffer_size = 8192;
    char* result = malloc(buffer_size);
    if (result == NULL) {
        return NULL;
    }

    // Initialize the string with the required head
    int offset = snprintf(result, buffer_size, "NODES ");

    // Iterate the consolited nodes to concatened them on a safe way
    for (int i = 0; i < node_count; i++) {
        // Agrega el punto y coma separador a partir del segundo nodo impreso
        if (i > 0 && (size_t)offset < buffer_size) {
            offset += snprintf(result + offset, buffer_size - offset, ";");
        }

        // Concatenate the "IP:PORT:cpu:X:mem:Y:gpu:Z" format.
        if ((size_t)offset < buffer_size) {
            offset += snprintf(result + offset, buffer_size - offset, 
                               "%s:%d:cpu:%d:mem:%d:gpu:%d",
                               unique_nodes[i].ip, 
                               unique_nodes[i].port, 
                               unique_nodes[i].cpu, 
                               unique_nodes[i].mem, 
                               unique_nodes[i].gpu);
        }
    }

    if ((size_t)offset < buffer_size - 1) { // to let the erlang process know when to stop listening
        snprintf(result + offset, buffer_size - offset, "\n");
    }

    return result; 
}

void log_error(const char *msg)   { perror(msg); }
void fatal_error(const char *msg) { perror(msg); exit(EXIT_FAILURE); }

// Returns to the local pool the resource(s) reservated on the 'fd' conecction.
// Removes the entry of table_clients and retry to serve the queues.
// Idempotent: if in 'fd' theres nothing else reserved, it does nothing.

void release_client_by_fd(int fd) {
    pthread_mutex_lock(&table_clients.mutexTable);

    int found_any = 0;

    for (int i = 0; i < TABLE_SIZE; i++) {
        job_entry** pp = &table_clients.job_table[i];
        while (*pp != NULL) {
            job_entry* job = *pp;
            if (job->origin_socket == fd) {
                found_any = 1;

                pthread_mutex_lock(&mutex_resources);
                for (granted_t* r = job->resources; r != NULL; r = r->next) {
                    if      (!strcmp(r->type, "cpu")) cpu_available += r->amount;
                    else if (!strcmp(r->type, "mem")) mem_available += r->amount;
                    else if (!strcmp(r->type, "gpu")) gpu_available += r->amount;
                }
                pthread_mutex_unlock(&mutex_resources);

                *pp = job->next_job;
                table_clients.active_count--;
                DestroyJob(job);
                // Dond advance on pp: next is already on *pp
            } else {
                pp = &(*pp)->next_job;
            }
        }
    }

    pthread_mutex_unlock(&table_clients.mutexTable);

    if (found_any) reserve_elements();
}



// If the connection that falied was ours (a own next_req waiting for
// GRANTED/DENIED of a supplier that disconnected without an answer),
// we deny the job immediately instead of waiting the 30 sec of the timeout.
void handle_outbound_disconnect(int fd) {
    pthread_mutex_lock(&table_ourjobs.mutexTable);

    job_entry* job = NULL;
    job_entry* prev = NULL;
    int idx = -1;
    for (int i = 0; i < TABLE_SIZE && job == NULL; i++) {
        job_entry* cur = table_ourjobs.job_table[i];
        job_entry* p = NULL;
        while (cur != NULL) {
            if (cur->origin_socket == fd) { job = cur; prev = p; idx = i; break; }
            p = cur; cur = cur->next_job;
        }
    }

    if (job == NULL) {
        pthread_mutex_unlock(&table_ourjobs.mutexTable);
        return;
    }

    fprintf(stderr, "[WARN] Job %d: proveedor remoto se desconectó sin responder. Rechazado.\n",
            job->job_id);

    if (prev == NULL) table_ourjobs.job_table[idx] = job->next_job;
    else               prev->next_job = job->next_job;
    table_ourjobs.active_count--;

    pthread_mutex_unlock(&table_ourjobs.mutexTable);

    char id_str[16];
    snprintf(id_str, sizeof(id_str), "%d", job->job_id);

    release_resources(job);   // frees what would be given of OTHER suppliers
    C_to_erlang("rejected", id_str);
    DestroyJob(job);
}