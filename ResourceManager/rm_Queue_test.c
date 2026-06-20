#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include "resource_manager.h"

void* test_producer(void * arg){
 p_queue_t* queue = (p_queue_t*) arg;
 for (int i = 0; i < REQ_AMOUNT; i++){
      int soc = rand()% RANDO_MAX;
      int id = rand() % RANDO_MAX;
      p_request_t* req = MakeRequest(id, soc, i * 2);
      EnqueueRequest(queue, req);
      printf("[QUEUED] element: job_id = <%d>\t origin_socket = <%d>\t amount = <%d>\t\n",
             req->job_id, req->origin_socket, req->amount_requested);
     }
    return NULL;
 }

void* test_consumer(void* arg){
    p_queue_t* queue = (p_queue_t*) arg;
    while(1){
            p_request_t* req = DequeueRequest(queue);
            if(req == NULL){break;}
            printf("[DEQUEUED] element: job_id = <%d>\t origin_socket = <%d>\t amount= <%d>\t\n"
            ,req->job_id, req->origin_socket, req->amount_requested);
            DestroyRequest(req);
     }
    return NULL;
}

int main(){

pthread_t t1, t2;
p_queue_t* PendingRequests = MakeQueue(); 

printf("Queue is empty?: %d\n", IsEmpty(PendingRequests)); 
pthread_create(&t1, NULL, test_producer, PendingRequests );
pthread_create(&t2, NULL, test_consumer, PendingRequests );

pthread_join(t1, NULL); 
StopQueue(PendingRequests);
pthread_join(t2, NULL);

DestroyQueue(PendingRequests);


return 0;}

