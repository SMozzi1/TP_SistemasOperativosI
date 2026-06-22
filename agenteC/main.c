#include "globals.h"


    active_jobs table_ourJobs;
    active_jobs table_clientJobs;
    active_jobs table_nodes;

    p_queue_t cpu_queue;
     p_queue_t mem_queue;
     p_queue_t gpu_queue;

int main(){

    JobsTableInit(&table_ourJobs);
    JobsTableInit(&table_clientJobs);
    JobsTableInit(&table_nodes);

    MakeQueue(&cpu_queue);
    MakeQueue(&mem_queue);
    MakeQueue(&gpu_queue);

}