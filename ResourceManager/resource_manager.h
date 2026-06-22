#ifndef _RESOURCE_MANAGER_H_
#define _RESOURCE_MANAGER_H_

#include "rm_Queue.h"
#define POOL_SIZE 3
    
typedef struct resource_t{
    char type[8];
    int total;
    int available;
    p_queue_t* pending;

} resource_t; 

/*this refers to the general pool of resources this node has.
agent might want to initialize POOL[i]->total with something like CPU_MAX const
TODO: ask teamate if the pool should be initialized by agent and not here*/

extern resource_t POOL[POOL_SIZE];
POOL[0]->type = "cpu";
POOL[1]->type = "mem";
POOL[2]->type = "gpu"; 

resource_t* MakeResource(char* type, int amount);

void DestroyResource(resource_t* res);

resource_t* WhichResource(char* type);

int Reserve(active_jobs* table, int job_id, int socketfd , char* type, int amount);

void RetryPending(active_jobs* table, resource_t* res);

void Release(active_jobs* table ,int job_id);
#endif/*_RESOURCE_MANAGER_H_*/
