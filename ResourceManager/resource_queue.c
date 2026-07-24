#include "resource_queue.h"
#include <assert.h>
#include <string.h>



request* make_request(int jid, int socketfd, int amount){

    request *newRequest = malloc(sizeof(struct p_request_t));
    assert(newRequest != NULL);
    newRequest->job_id = jid;
    newRequest->origin_socket = socketfd;
    newRequest->amount_requested = amount;
    newRequest->asked_resource[0] = '\0';
    newRequest->next_req = NULL;

    return newRequest;
}


void destr_request(request* req){
    free(req);
}


request_queue *make_queue(void){

    request_queue* newQueue = malloc(sizeof(struct p_queue_t));
    assert(newQueue != NULL);
    newQueue->first = NULL;
    newQueue->last = NULL;
    pthread_mutex_init(&newQueue->mutexQueue, NULL);

    return newQueue;
}


void enqueue_request(request_queue* queue, request* request){

    pthread_mutex_lock(&queue->mutexQueue);
    if(queue->first == NULL){
        queue->first = request;
        queue->last = request;
    } else{
        queue->last->next_req = request;
        queue->last = request;
    }
    request->next_req = NULL;

    pthread_mutex_unlock(&queue->mutexQueue);
}


/*
 * Pops the head of the queue. The caller MUST already hold queue->mutexQueue
 * (drain_queue holds both mutex_resources and mutexQueue while deciding, so it
 * cannot use a self-locking dequeue).
 */
request* dequeue_request_locked(request_queue* queue) {
    if (queue->first == NULL)
        return NULL;

    request* req = queue->first;
    queue->first = req->next_req;
    if (queue->first == NULL)
        queue->last = NULL;

    req->next_req = NULL;
    return req;
}
