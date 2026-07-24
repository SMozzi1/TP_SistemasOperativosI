#ifndef _RESOURCE_QUEUE_H_
#define _RESOURCE_QUEUE_H_


#include <pthread.h>
#include <stdio.h>
#include "hash.h"




/*
 * A pending reservation coming from a remote agent, kept in the FIFO of the
 * resource it is waiting for. As the enunciado (5.1) requires, every request
 * is identified by its job id AND the socket it arrived on (origin_socket),
 * so we can answer GRANTED and reclaim on disconnect.
 * There is one queue per resource type, so the resource name is implicit and
 * asked_resource is only kept for logging/debugging.
 */
typedef struct  p_request_t{
    int job_id;
    int origin_socket;             // to send GRANTED and to reclaim on disconnect
    int amount_requested;
    char asked_resource[16];       // informational (the queue already implies the type)
    struct p_request_t* next_req;  // intrusive FIFO link
} request;



/*
 * A single resource type = this agent's free amount of it (resources_left) plus
 * the FIFO of peers waiting for it. Both are guarded by the SAME mutexQueue, so
 * "is there enough? -> discount -> dequeue" is one atomic decision under one lock.
 */
typedef struct p_queue_t {
    int resources_left;            // this node's free amount of this resource
    request* first;
    request* last;
    pthread_mutex_t mutexQueue;
} request_queue;



request *make_request(int job_id, int socket, int amount);

void destr_request(request* req);

request_queue *make_queue(int resources);

void enqueue_request(request_queue* queue, request* request);

/* Pops the head. Caller MUST already hold queue->mutexQueue. */
request* dequeue_request_locked(request_queue* queue);


#endif /* RESOURCE_QUEUE_H */
