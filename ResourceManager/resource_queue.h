#ifndef _RESOURCE_QUEUE_H_
#define _RESOURCE_QUEUE_H_


#include <pthread.h>
#include <stdio.h>
#include "hash.h"




//We dont use names because we will have 3 separated queue for each element
typedef struct  p_request_t{
    received_job* job; //To find the job in hash_job
    int origin_socket; //To send granted
    int amount_requested;
    char asked_resource[16]; //To know what resource we are asking for
} request;



typedef struct p_queue_t {
    int resources_left; //
    request* first;
    request* last;
    pthread_mutex_t mutexQueue;
} request_queue;



request *make_request(int job_id, int socket, int amount);

void destr_request(request* req);

request_queue *make_queue (int resources);

void enqueue_request(request_queue* queue, request* request);

request *dequeue_request(request_queue* queue);

request* dequeue_request(request_queue* queue);

request* try_dequeue(request_queue* queue);


#endif /* RESOURCE_QUEUE_H */