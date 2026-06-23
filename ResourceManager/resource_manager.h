#ifndef _RESOURCE_MANAGER_H_
#define _RESOURCE_MANAGER_H_

#include "rm_Queue.h"
#define POOL_SIZE 3

//resource pool this node has 
extern resource_t* POOL[POOL_SIZE];

typedef struct resource_t{
    char type[8];
    int total;
    int available;
    p_queue_t* pending;

} resource_t; 



resource_t* MakeResource(char* type, int amount);

void DestroyResource(resource_t* res);

void InitPool(void);

void DesotroyPool(void);

resource_t* WhichResource(char* type);

int Reserve(active_jobs* table, int job_id, int socketfd , char* type, int amount);

void RetryPending(active_jobs* table, resource_t* res);

void Release(active_jobs* table ,int job_id);

void ReleaseAllsocketfd(active_jobs*table, int fd);
#endif/*_RESOURCE_MANAGER_H_*/
