#define _GNU_SOURCE

#include "utils.h"
#include "../ResourceManager/job_table.h"
#include "globals.h"

#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>


#define JOB_TIMEOUT_SEC 30

#define NODE_TIMEOUT_SEC 15



/*
chat me tiro esta idea de como manejar los recursos disponibles y las colas de espera. En vez de tener una estructura global con un mutex para cada recurso, podríamos tener una estructura que contenga la cantidad disponible de cada recurso y un mutex que proteja el acceso a esa estructura. Además, podríamos tener una función que se encargue de procesar las colas de espera cada vez que se libere un recurso, para otorgar los recursos a los trabajos en espera si es posible.
typedef struct {
    int cpu;
    int mem;
    int gpu;
    pthread_mutex_t mutex; // Este mutex protege a las 3 variables de arriba
} resource_pool_t;
*/
extern int cpu_available;
extern int mem_available;
extern int gpu_available;
extern pthread_mutex_t mutex_resources;

extern active_jobs table_nodes;
extern active_jobs table_clients;

extern fifo_queue_t cpu_queue;
extern fifo_queue_t mem_queue;
extern fifo_queue_t gpu_queue;


//Agrego epollfd asi no 
static int make_timer(int epollfd, int initial_sec, int interval_sec) {
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

//Se llamara a esta fucnion 2 veces
//Se le pasa la tabla de nodos
//Se le pasa la tabla de clientes
static void check_job_timeouts(int erlangfd, active_jobs tabla_propia) {

    time_t now = time(NULL);

    /* ── tabla_propia: our jobs waiting for a reply from remote nodes ── */
    pthread_mutex_lock(&tabla_propia.mutexTable);
    for (int i = 0; i < TABLE_SIZE; i++) {
        job_entry **pp = &tabla_propia.job_table[i];
        while (*pp) {
            job_entry *j = *pp;
            if (difftime(now, j->timestamp) >= JOB_TIMEOUT_SEC){
                fprintf(stderr, "[TIMEOUT] job %d in tabla_propia expired\n", j->job_id);

                char job_id_str[32];
                snprintf(job_id_str, sizeof(job_id_str), "%d", j->job_id);

                /* Notify Erlang only if the connection is still alive */
                if (erlangfd >= 0) {
                    C_to_erlang(erlangfd, "timeout", job_id_str);
                }

                /* Also clean up the conn_ctx for this job */
                remove_conn_ctx(j->job_id);

                *pp = j->next_job;
                DestroyJob(j);
                /* Do not advance pp: next entry is already at *pp */
            } else {
                pp = &(*pp)->next_job;
            }
        }
    }
    pthread_mutex_unlock(&tabla_propia.mutexTable);

    /* ── table_clients: reservations from remote nodes pending locally ── */
    pthread_mutex_lock(&table_clients.mutexTable);
    for (int i = 0; i < TABLE_SIZE; i++) {
        job_entry **pp = &table_clients.job_table[i];
        while (*pp) {
            job_entry *j = *pp;
            if (difftime(now, j->timestamp) >= JOB_TIMEOUT_SEC) {
                fprintf(stderr, "[TIMEOUT] job %d in table_clients expired\n", j->job_id);

                /*
                 * TODO (resource management teammate):
                 *   release_resources_for_job(j);
                 */

                *pp = j->next_job;
                DestroyJob(j);
            } else {
                pp = &(*pp)->next_job;
            }
        }
    }
    pthread_mutex_unlock(&table_clients.mutexTable);
}



//Para la liberacion de los recursos
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


//Deber
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


//Asignacion de donde vino la memoria
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



//Bucle principal para pedir elementos.
//Va pidiendo en orden y solo da granted si lo tiene a todos
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
        
        // NOTA: El 'send' NO va aquí. Debe ir en tu bucle epoll cuando detectes EPOLLOUT.
        // Para lograrlo, debes asociar este 'remote_fd' con el 'job' y el 'recurso_actual'
        // en una estructura de contexto o tabla intermedia de conexiones activas.
    }
}



//Si le llega un pedido de recurso, lo encola en la cola correspondiente y luego llama a reserve_elements para intentar asignar recursos a los trabajos en espera
void enqueue_jobs(const char* resource, int job_id, int amount, int fd_actual) {
    MakeRequest(job_id, fd_actual, amount);

    if(!strcmp(resource, "cpu")) {
        EnqueueRequest(&cpu_queue, job_id);
    } else if (!strcmp(resource, "mem")) {
        EnqueueRequest(&mem_queue, job_id);
    } else {
        EnqueueRequest(&gpu_queue, job_id);
    }

    reserve_elements();
}



//Otorgara tantos recursos tenga disponibles y se encargara de enviar el mensaje de granted a los clientes
void reserve_elements() {
    
    // ─── 1. PROCESAR COLA DE CPU ─────────────────────────────────────
    while (cpu_queue.first != NULL && cpu_available >= cpu_queue.first->amount_requested) {
        p_request_t* req = DequeueRequest(&cpu_queue);
        if (req != NULL) {
            pthread_mutex_lock(&mutex_resources);
            cpu_available -= req->amount_requested;
            pthread_mutex_unlock(&mutex_resources);

            // Al ser ID único, creamos el Job directamente de cero
            job_entry* nuevo_activo = MakeJob(req->job_id, req->origin_socket, 0); 
            granted_t* recurso = MakeGranted("cpu", req->amount_requested, "local");
            
            AddResource(nuevo_activo, recurso);
            JobsTableInsert(&table_clients, nuevo_activo);
            //agregar id
            send(req->origin_socket, "GRANTED\n", 8, MSG_NOSIGNAL);
            printf("[SERVER] Job %d: CPU otorgada. Registrado en table_clients.\n", req->job_id);

            DiscardRequest(req); 
        }
    }

    // ─── 2. PROCESAR COLA DE MEMORIA ─────────────────────────────────
    while (mem_queue.first != NULL && mem_available >= mem_queue.first->amount_requested) {
        p_request_t* req = DequeueRequest(&mem_queue);
        if (req != NULL) {
            pthread_mutex_lock(&mutex_resources);
            mem_available -= req->amount_requested;
            pthread_mutex_unlock(&mutex_resources);

            job_entry* nuevo_activo = MakeJob(req->job_id, req->origin_socket, 0);
            granted_t* recurso = MakeGranted("mem", req->amount_requested, "local");
            
            AddResource(nuevo_activo, recurso);
            JobsTableInsert(&table_clients, nuevo_activo);

            send(req->origin_socket, "GRANTED\n", 8, MSG_NOSIGNAL);
            printf("[SERVER] Job %d: Memoria otorgada. Registrado en table_clients.\n", req->job_id);

            DiscardRequest(req);
        }
    }

    // ─── 3. PROCESAR COLA DE GPU ─────────────────────────────────────
    while (gpu_queue.first != NULL && gpu_available >= gpu_queue.first->amount_requested) {
        p_request_t* req = DequeueRequest(&gpu_queue);
        if (req != NULL) {

            pthread_mutex_lock(&mutex_resources);
            gpu_available -= req->amount_requested;
            pthread_mutex_unlock(&mutex_resources);

            job_entry* nuevo_activo = MakeJob(req->job_id, req->origin_socket, 0);
            granted_t* recurso = MakeGranted("gpu", req->amount_requested, "local");
            
            AddResource(nuevo_activo, recurso);
            JobsTableInsert(&table_clients, nuevo_activo);

            send(req->origin_socket, "GRANTED\n", 8, MSG_NOSIGNAL);
            printf("[SERVER] Job %d: GPU otorgada. Registrado en table_clients.\n", req->job_id);

            DiscardRequest(req);
        }
    }
}


char* obtener_string_nodos(job_entry* table[]) {
    // Estructura auxiliar temporal para agrupar recursos por Nodo único
    typedef struct {
        char ip[16];
        int port;
        int cpu;
        int mem;
        int gpu;
    } NodeSummary;

    // Array temporal para almacenar hasta 128 nodos remotos únicos distintos
    NodeSummary unique_nodes[128];
    int node_count = 0;
    memset(unique_nodes, 0, sizeof(unique_nodes));

    // ─── 1. RECORRER LA TABLA HASH COMPLETA ───────────────────────────
    for (int i = 0; i < TABLE_SIZE; i++) {
        job_entry* current_job = table[i];

        // Recorrer la lista de colisiones por encadenamiento (next_job)
        while (current_job != NULL) {
            granted_t* res = current_job->resources;

            // Recorrer la lista enlazada de recursos otorgados a este Job
            while (res != NULL) {
                int found_idx = -1;

                // Buscar si ya registramos esta combinación de IP:Puerto
                for (int j = 0; j < node_count; j++) {
                    if (unique_nodes[j].port == res->dest_port && 
                        strcmp(unique_nodes[j].ip, res->dest_ip) == 0) {
                        found_idx = j;
                        break;
                    }
                }

                // Si no existe, creamos una nueva entrada para este nodo remoto
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

                // Sumar la cantidad al recurso correspondiente
                if (strcmp(res->type, "cpu") == 0) {
                    unique_nodes[found_idx].cpu += res->amount;
                } else if (strcmp(res->type, "mem") == 0) {
                    unique_nodes[found_idx].mem += res->amount;
                } else if (strcmp(res->type, "gpu") == 0) {
                    unique_nodes[found_idx].gpu += res->amount;
                }

                res = res->next; // Siguiente recurso del Job
            }
            current_job = current_job->next_job; // Siguiente Job en el bucket
        }
    }

    // ─── 2. CONSTRUIR EL STRING DINÁMICO ──────────────────────────────
    // Reservamos un buffer amplio en memoria dinámica (8 KB) para armar la respuesta
    size_t buffer_size = 8192;
    char* result = malloc(buffer_size);
    if (result == NULL) {
        return NULL;
    }

    // Inicializamos el string con la cabecera requerida
    int offset = snprintf(result, buffer_size, "NODES ");

    // Iteramos los nodos consolidados para concatenarlos de forma segura
    for (int i = 0; i < node_count; i++) {
        // Agrega el punto y coma separador a partir del segundo nodo impreso
        if (i > 0 && (size_t)offset < buffer_size) {
            offset += snprintf(result + offset, buffer_size - offset, ";");
        }

        // Concatena el formato "IP:PORT:cpu:X:mem:Y:gpu:Z"
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

    return result; 
}
