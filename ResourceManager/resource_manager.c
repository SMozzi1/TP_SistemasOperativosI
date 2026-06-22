#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include <time.h>
#include <string.h>
#include "job_table.h"
#include "rm_queue.h"
#include "resource_manager.h"
#include "job_table.h"
#include <time.h>

//TODO: ReleaseAllsocketfd()

resource_t* MakeResource(char* type, int amount){
    resource_t* pool = malloc(sizeof(struct resource_t));
    assert(pool);
    strncpy(pool->type, type, sizeof(pool->type) - 1);
    pool->total = amount;
    pool->available = amount;
    pool->pending = MakeQueue();

    return pool;

}

void DestroyResource(resource_t* res){
    DestroyQueue(res->pending);
    free(res);
}

resource_t* WhichResource(char* type){
    for(int i = 0; i < POOL_SIZE; i++){
        if(strcmp(POOL[i]->type, type) == 0){
            return POOL[i];
        }
    }
    printf("Resource specified doesn't exist\n");
    return NULL;
}

int Reserve(active_jobs* table, int job_id, int socketfd , char* type, int amount){
    resource_t* pool = WhichResource(type);
    if(!pool){
        printf("resource <%s> doesn't exist\n", type);
        return 0;
    }
    if(amount > pool->available){// asking more resources than available
        p_request_t* req = MakeRequest(job_id, socketfd, amount);
        EnqueueRequest(pool->pending, req);
        return 0;
    }
    else{
        granted_t* g = MakeGranted(type, amount);
        job_entry * job = MakeJob(job_id, socketfd, time(NULL));
        AddResource(job,g);
        pool->available = pool->available - amount;
        JobsTableInsert(table,job);
        
        return 1;

    }
}

void RetryPending(active_jobs* table, resource_t* res){
    while(res->pending->first != NULL && res->available >= res->pending->first->amount_requested){
        p_request_t* req = res->pending->first;
        res->pending->first = req->next_req;

        if (res->pending->first == NULL){ //empty queue
            res->pending->last = NULL;
        }
        res->available -= req->amount_requested;
        granted_t* g = MakeGranted(res->type, req->amount_requested);
        job_entry * job = MakeJob(req->job_id, req->origin_socket, time(NULL));
        AddResource(job,g);
        JobsTableInsert(table,job);

        char msg[64];
        snprintf(msg, sizeof(msg), "GRANTED %d\n", req->job_id);
        send(req->origin_socket, msg, strlen(msg), 0);


        DestroyRequest(req);
    }

}




void Release(active_jobs* table ,int job_id){
    job_entry* rel = FindJob(table,job_id);
    if(!rel){return;}

    for( granted_t* t = rel->resources; t != NULL; t = t->next){
    resource_t* res = WhichResource(t->type);
    res->available += t->amount;
    RetryPending(table,res);
    }
    RemoveJob(table, job_id);
}

