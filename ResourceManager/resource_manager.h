#ifndef _RESOURCE_MANAGER_H_
#define _RESOURCE_MANAGER_H_

#include <pthread.h>


//resource request element structure
typedef struct  p_request_t{
    int job_id;
    int origin_socket; 
    int amount_requested;
    struct p_request_t* next_req;
} p_request_t;

//pending requests Queue.
typedef struct p_queue_t {
    int stop;
    pthread_cond_t not_empty;
    pthread_mutex_t mutexQueue;
    p_request_t* first;
    p_request_t* last;

} p_queue_t;

typedef struct resource_t{
    char type[8],
    int total,
    int available,
    p_queue_t* pending;

} resource_t; 



p_request_t* MakeRequest(int job_id, int origin_socket, int amount);

void DestroyRequest(p_request_t* request); 

p_queue_t* MakeQueue(); 

int IsEmpty(p_queue_t* resource_queue); 

void EnqueueRequest(p_queue_t* resource_queue, p_request_t* request);

p_request_t* DequeueRequest(p_queue_t* resource_queue); 

void DestroyQueue(p_queue_t* resource_queue); 

void StopQueue(p_queue_t* resource_queue);

void PrintQueue(p_queue_t* resource_queue); 

void DiscardRequest(p_queue_t* resource_queue);

#endif /*_RESOURCE_MANAGER_H_*/

