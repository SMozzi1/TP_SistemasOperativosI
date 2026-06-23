
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include "resource_manager.h"



// request constructor
p_request_t* MakeRequest(int job_id, int origin_socket, int amount){

    p_request_t* newRequest = malloc(sizeof(p_request_t));
    assert(newRequest);

    newRequest->job_id = job_id;
    newRequest->origin_socket = origin_socket;
    newRequest->amount_requested = amount;
    newRequest->next_req = NULL; 

    return newRequest;
    }

//request destructor
void DestroyRequest(p_request_t* request){
    free(request);
}


//queue constructor
p_queue_t* MakeQueue(){
    p_queue_t* NewQueue = malloc(sizeof(p_queue_t));
    assert(NewQueue);
    NewQueue->stop = 0;
    pthread_mutex_init(&NewQueue->mutexQueue, NULL);
    pthread_cond_init(&NewQueue->not_empty, NULL);
    NewQueue->first = NULL;
    NewQueue->last = NULL;
   
    return  NewQueue;
}

/*checks if the queue is empty
- IsEmpty IS NOT thread safe by itself. It has to 
be called between mutexes.*/

int IsEmpty(p_queue_t* resource_queue){
    int result = (resource_queue->first == NULL && resource_queue->last == NULL);
    return result;
}

/*puhses a pending request into the queue*/
void EnqueueRequest( p_queue_t* resource_queue , p_request_t* request){

    pthread_mutex_lock(&resource_queue->mutexQueue);

        if(IsEmpty(resource_queue)){ /*first element in the queue case*/
                resource_queue->first = request;
                resource_queue->last = request;
            }
        else{ 
              resource_queue->last->next_req = request; 
              resource_queue->last = request;

             }

     pthread_cond_broadcast(&resource_queue->not_empty); /*signal new elements in the queue*/
     pthread_mutex_unlock(&resource_queue->mutexQueue);
    }


//returns first element of the queue without freeing the element.

p_request_t* DequeueRequest(p_queue_t* resource_queue){
    pthread_mutex_lock(&resource_queue->mutexQueue);

               if(IsEmpty(resource_queue)){/*wait for new elements*/
                  while(resource_queue->first == NULL && !resource_queue->stop){

                       pthread_cond_wait(&resource_queue->not_empty,
                                         &resource_queue->mutexQueue); 
                    }  

                    /*condition to check if we actually stopped 
                    the queue and we're not just waiting for new elements*/
                    if(resource_queue->first == NULL && resource_queue->stop){ 
                           pthread_mutex_unlock(&resource_queue->mutexQueue);
                           return NULL;}
                    }

               p_request_t* result = resource_queue->first;
               resource_queue->first = result->next_req;

               if(resource_queue->first == NULL){ /*we dequeued and now the queue is empty.*/
                   resource_queue->last = NULL; }

               result->next_req = NULL;
               pthread_mutex_unlock(&resource_queue->mutexQueue);
               return result;
               }

/*Frees the first element of the queue. DequeueRequest already handles thread safety.*/
void DiscardRequest(p_queue_t* resource_queue){
       DestroyRequest(DequeueRequest(resource_queue));
       
}

/*queue can't tell the difference between an empty queue waiting for new elements,and one that
 has stopped and ceased operations by itself. So we make a function that switches the stop val in the queue struct.*/

void StopQueue(p_queue_t* resource_queue){
    pthread_mutex_lock(&resource_queue->mutexQueue);
    resource_queue->stop = 1;
    pthread_cond_broadcast(&resource_queue->not_empty);
    pthread_mutex_unlock(&resource_queue->mutexQueue);
}

/*Frees the queue*/
void DestroyQueue(p_queue_t* resource_queue){
     p_request_t* delete;

    while(resource_queue->first != NULL){
         delete = resource_queue->first;
         resource_queue->first = resource_queue->first->next_req;
         DestroyRequest(delete); 
    }
    resource_queue->last = NULL;
    pthread_mutex_destroy(&resource_queue->mutexQueue);
    free(resource_queue);
}

/*prints Queue*/
void PrintQueue(p_queue_t* resource_queue){

  pthread_mutex_lock(&resource_queue->mutexQueue);

  p_request_t* trace = resource_queue->first;
    while(trace!= NULL){
           printf("job_id is <%d> , origin_socket is <%d>, amount requested is <%d>\n", 
                   trace->job_id, trace->origin_socket, trace->amount_requested); 
    trace = trace->next_req;
           
 }
  pthread_mutex_unlock(&resource_queue->mutexQueue);
}



/*unsafe version of DequeueRequest. Assumes calling process already has a lock*/
p_request_t* DequeueRequest_locked(p_queue_t* q) {
    p_request_t* r = q->first;
    if (r == NULL) return NULL;
    q->first = r->next_req;
    if (q->first == NULL) q->last = NULL;
    r->next_req = NULL;
    return r;
}
