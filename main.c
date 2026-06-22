#include "../agenteC/globals.h"
#include "../agenteC/agente.h"

/* Creamos las colas globales de la implementación que armaron,
 * ya que globals.h las pide como extern */
p_queue_t cpu_queue;
p_queue_t mem_queue;
p_queue_t gpu_queue;

int main() {
    /* 1. Inicializamos las tablas con los nombres EXACTOS de globals.h */
    JobsTableInit(&table_ourjobs);
    JobsTableInit(&table_clients);
    JobsTableInit(&table_nodes);

    /* 2. Inicializamos las colas */
    MakeQueue(&cpu_queue);
    MakeQueue(&mem_queue);
    MakeQueue(&gpu_queue);

    /* 3. Arrancamos el lazo principal.
     * Ya no hace falta declarar epollfd ni erlangfd acá porque 
     * viven globalmente en agente.c */
    setup_epoll();

    return 0;
}