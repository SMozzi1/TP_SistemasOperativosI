#include "resource_queue.h"



request* make_request(int jid, int socketfd, int amount){

    request *newRequest = malloc(sizeof(struct p_request_t));
    assert(newRequest != NULL);
    newRequest -> job_id = jid;
    newRequest -> origin_socket = socketfd;
    newRequest -> amount_requested = amount;
    newRequest -> next_req = NULL;

    return newRequest;
}


void destr_request(request* req){
    free(req);
}


request_queue *make_queue(int resources){

    request_queue* newQueue = malloc(sizeof(struct p_queue_t));
    newQueue -> resources_left = resources;
    newQueue -> first = NULL;
    newQueue -> last = NULL;
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

    pthread_mutex_unlock(&queue->mutexQueue);
}


//para comparar se haria 
//queue -> first -> cantidad que pide <= cant total que se tien
//hacer dequeue_request
//(basicamente si tengo los elementos para darle al request, entonces lo desencolo)
request* dequeue_request(request_queue* queue) {

    pthread_mutex_lock(&queue->mutexQueue);

    request* newRequest = queue->first;
    queue->first = queue->first->next_req;


    pthread_mutex_unlock(&queue->mutexQueue);
    

    return newRequest;
}
