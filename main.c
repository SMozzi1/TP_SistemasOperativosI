#include "../agenteC/globals.h"
#include "../agenteC/agente.h"

 /* Create the global queues made from the implementatiom,
 * since globals.h ask it as extern */

request_queue* cpu_queue;
request_queue* mem_queue;
request_queue* gpu_queue;



int main(int argc, char** argv) {
    int port ;
    if (argc < 2) {
        printf("[INFO] No se pasó puerto por argumento. Usando el default: 4200\n");
        port = 4200;
    } else {
    port = atoi(argv[1]);
    }

    initialize_connection_buffers(); // Initialize connection buffers and mutexes for all file descriptors

    /* 1. Iniciate the tables with the EXACT names of globals.h */
    table_ourjobs = create_table_fd_jobs(); // local jobs table
    table_nodejobs = create_table_jobs(); // remote jobs table
    table_nodes = create_table_nodes(); // nodes that we are connected to table

    /* 2. Iniciate the queues */
    cpu_queue = make_queue(4);  
    mem_queue = make_queue(8192);
    gpu_queue = make_queue(1)

     /* 3. Start with the main trail.
      * No need for declaring epollfd nor erlangd here since
      * they live globally in agent.c */
    setup_epoll(port);

    return 0;
}