

#include "utils.h"
#include "job_table.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/epoll.h>

extern int cpu_available;
extern int mem_available;
extern int gpu_available;
extern pthread_mutex_t mutex_resources;

extern active_jobs table_nodes;
extern active_jobs table_clients;

extern fifo_queue_t cpu_queue;
extern fifo_queue_t mem_queue;
extern fifo_queue_t gpu_queue;



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


void update_local_resources(const char *resource_name, int amount) {
    pthread_mutex_lock(&mutex_resources);

    if (strcmp(resource_name, "cpu") == 0) {
        cpu_available += amount;
        printf("[RESOURCE] %d CPUs devueltas. Disponibles ahora: %d\n", amount, cpu_available);
        process_queue(&cpu_queue, cpu_available, "cpu");
    } 
    else if (strcmp(resource_name, "mem") == 0) {
        mem_available += amount;
        printf("[RESOURCE] %d MB de memoria devueltos. Disponibles ahora: %d MB\n", amount, mem_available);
        process_queue(&mem_queue, mem_available, "mem");
    } 
    else if (strcmp(resource_name, "gpu") == 0) {
        gpu_available += amount;
        printf("[RESOURCE] %d GPUs devueltas. Disponibles ahora: %d\n", amount, gpu_available);
        process_queue(&gpu_queue, gpu_available, "gpu");
    } 
    else {
        printf("[WARN] Intento de devolver un tipo de recurso desconocido: %s\n", resource_name);
    }

    pthread_mutex_unlock(&mutex_resources);
}


void release_resources(job_entry* job)
{
    // flag to see if there is a job or any resources at all
    if (job == NULL || job->resources == NULL) {
        return;
    }

    granted_t* actual = job->resources;

    // we see all the resource until there are no resources left
    while (actual != NULL && actual != job->next_req) {
        
       
        if (actual->providerfd >= 0) {
            char msg[256];
            snprintf(msg, sizeof(msg), "RELEASE %d %s %d\n", job->job_id, actual->type, actual->amount);
            
            // we send the message to the node that provided this resource
            send(actual->providerfd, msg, strlen(msg), MSG_NOSIGNAL);
            
            printf("[INFO] Enviado RELEASE del recurso %s al FD %d para el Job %d\n", 
                    actual->type, actual->providerfd, job->job_id);
            
            // we close the connection with the resource
            close(actual->providerfd);
        
            // we set the fd as invalid.
            actual->providerfd = -1;
        }
        
        // check the next resources
        actual = actual->next;
    }
}


void original_socket(job_entry* job, int fd)
{
    if (job == NULL || job->next_req == NULL) {
        return;
    }

    // we asign the provider id to the resource
    job->next_req->providerfd = fd;

    printf("[DEBUG] Asignado provider_fd = %d al recurso '%s' del Job %d\n", 
            fd, job->next_req->type, job->job_id);
}


void ask_for_next_resource(int epollfd, int erlangfd, job_entry* job)
{
    // CASO BASE: Si ya no hay más recursos en la lista, terminamos con éxito
    if (job->next_req == NULL)
    {
        job->next_req = NULL;
        char id_str[16];
        snprintf(id_str, sizeof(id_str), "%d", job->job_id);
        C_to_erlang(erlangfd, "granted", id_str);
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
        
        struct sockaddr_in remote_addr;
        memset(&remote_addr, 0, sizeof(remote_addr));
        remote_addr.sin_family = AF_INET;
        remote_addr.sin_port   = htons(job->dest_port);
        inet_pton(AF_INET, job->dest_ip, &remote_addr.sin_addr);
        
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
        
        // NOTA: El 'send' NO va aquí. Debe ir en tu bucle epoll cuando detectes EPOLLOUT.
        // Para lograrlo, debes asociar este 'remote_fd' con el 'job' y el 'recurso_actual'
        // en una estructura de contexto o tabla intermedia de conexiones activas.
    }
}

