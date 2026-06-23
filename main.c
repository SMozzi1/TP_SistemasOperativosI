#include "../agenteC/globals.h"
#include "../agenteC/agente.h"

 /* Create the global queues made from the implementatiom,
 * since globals.h ask it as extern */
p_queue_t cpu_queue;
p_queue_t mem_queue;
p_queue_t gpu_queue;

int main() {
    /* 1. Iniciate the tables with the EXACT names of globals.h */
    JobsTableInit(&table_ourjobs);
    JobsTableInit(&table_clients);
    JobsTableInit(&table_nodes);

    /* 2. Iniciate the queues */
    MakeQueue(&cpu_queue);
    MakeQueue(&mem_queue);
    MakeQueue(&gpu_queue);

     /* 3. Start with the main trail.
      * No need for declaring epollfd nor erlangd here since
      * they live globally in agent.c */
    setup_epoll();

    return 0;
}