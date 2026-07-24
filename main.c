#include "../agenteC/globals.h"
#include "../agenteC/agente.h"

/* Local capacity of this node. The enunciado's example inventory is
 * cpu:4, mem:8192 MB, gpu:1. Change these to model a different machine. */
#define LOCAL_CPU  4
#define LOCAL_MEM  8192
#define LOCAL_GPU  1

/* Definitions of the globals declared as extern in globals.h.
 * The queues only hold waiting peer requests; availability lives in the
 * cpu/mem/gpu_available counters (guarded by mutex_resources). */
request_queue* cpu_queue;
request_queue* mem_queue;
request_queue* gpu_queue;

TablaHash table_localjobs;
TablaHash table_ourjobs;
TablaHash table_nodejobs;
TablaHash table_nodes;

int cpu_available;
int mem_available;
int gpu_available;



int main(int argc, char** argv) {
    int port ;
    if (argc < 2) {
        printf("[INFO] No se pasó puerto por argumento. Usando el default: 4200\n");
        port = 4200;
    } else {
    port = atoi(argv[1]);
    }

    initialize_connection_buffers(); // Initialize connection buffers and mutexes for all file descriptors

    /* 1. Initiate the tables */
    table_localjobs = create_table_local_jobs(); // owner of local jobs, keyed by job_id
    table_ourjobs   = create_table_fd_jobs();     // fd -> local_job_t* index
    table_nodejobs  = create_table_jobs();        // remote jobs we serve
    table_nodes     = create_table_nodes();       // discovered peers

    /* 2. Initiate the queues (they only hold waiting peer requests) */
    cpu_queue = make_queue();
    mem_queue = make_queue();
    gpu_queue = make_queue();

    /* 3. Initiate this node's free-resource counters. */
    pthread_mutex_lock(&mutex_resources);
    cpu_available = LOCAL_CPU;
    mem_available = LOCAL_MEM;
    gpu_available = LOCAL_GPU;
    pthread_mutex_unlock(&mutex_resources);

     /* 4. Start with the main trail.
      * No need for declaring epollfd nor erlangd here since
      * they live globally in agent.c */
    setup_epoll(port);

    return 0;
}
