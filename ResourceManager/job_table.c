#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include <time.h>
#include <string.h>
#include "job_table.h"
#include <sys/socket.h>

#define TABLE_SIZE 256

//granted constructor
granted_t* MakeGranted(char* type, int amount, int dest_ip){
    granted_t* new = malloc(sizeof(struct granted_t));
    assert(new);
    strncpy(new->type,type, sizeof(new->type) - 1);
    new->type[sizeof(new->type) - 1] = '\0'; 
    new->amount = amount;

    int providerfd; //fd que dio el recurso, (para hacer realese)
    char dest_ip[16]; 
    int dest_port;

    new->next = NULL;
    
    return new;
}

//granted node destructor
void DestroyGranted(granted_t* granted_res){
    free(granted_res);}

//granted list destructor
void DestroyGrantedList(granted_t* granted_list){
    while(granted_list!= NULL){
           granted_t* destroy = granted_list;
           granted_list = granted_list->next;
           DestroyGranted(destroy);
           }

}

//job entry constructor
job_entry* MakeJob(int job_id, int origin_socket, time_t time){
    job_entry* new = malloc(sizeof(struct job__entry));
    assert(new);
    new->job_id = job_id;
    new->origin_socket = origin_socket;
    new->timestamp = time;
    new->resources = NULL; 
    new->next_job = NULL;
    
    return new;
    } 

//job entry destructor 
void DestroyJob(job_entry* job){
    DestroyGrantedList(job->resources);
    free(job);
}

// //adds element to the list of granted resources
// void AddResource(job_entry* job, granted_t* res){
//    if(job->resources == NULL){ // empty list
//        job->resources = res;
//     }
//    else{
//         res->next = job->resources;
//         job->resources = res;
//         }
// }

//Para agregar al final
void AddResource(job_entry* job, granted_t* res){

    res->next = NULL;

    if (job->resources == NULL) { 
        job->resources = res;
    }
    else {
        granted_t* actual = job->resources;
        while (actual->next != NULL) {
            actual = actual->next; // Avanzamos hasta el final
        }
        
        actual->next = res;
    }

    job->next_req = job->resources; 
}



//active job table constructor 
void JobsTableInit(active_jobs* table){
    for(int i = 0; i < TABLE_SIZE; i++){
        table->job_table[i] = NULL;
    }
       table->active_count = 0;
       pthread_mutex_init(&table->mutexTable, NULL);
}

//active job table destructor
void DestroyJobsTable(active_jobs* table){
   for(int i = 0; i < TABLE_SIZE; i++){
        job_entry* entry = table->job_table[i];
        while(entry != NULL){
            job_entry* destroy = entry;
            entry = entry->next_job; 
            DestroyJob(destroy);
        }
        table->job_table[i] = NULL;
      }
        pthread_mutex_destroy(&table->mutexTable);
        table->active_count = 0; 
  }

/*Se supone que como las id generadas por erlang son unicas, no deberia
  haber una mala distribucion. A CHECKEAR!!! (soy mozzi: esta bien son unicas
  y la hash funciona eficiente por eso)
*/

int HashF(int job_id){
     return job_id % TABLE_SIZE;
}

void JobsTableInsert(active_jobs* table, job_entry* job){
    pthread_mutex_lock(&table->mutexTable);
   int idx = HashF(job->job_id); 
    job->next_job = table->job_table[idx];
    table->job_table[idx] = job;
    table->active_count++; 
    pthread_mutex_unlock(&table->mutexTable);
}


job_entry* FindJob(active_jobs* table, int job_id){
    pthread_mutex_lock(&table->mutexTable);
    int search = HashF(job_id);
    job_entry* look = table->job_table[search];
    while( look != NULL && look->job_id != job_id ){
        look = look->next_job;
       }
    if (look == NULL){
    printf("job <%d>\t is not in the table\n", job_id);
    }
    pthread_mutex_unlock(&table->mutexTable);
    return look;
   }
    
void RemoveJob(active_jobs* table, int job_id){
    pthread_mutex_lock(&table->mutexTable);
     int idx = HashF(job_id);
     job_entry* prev = NULL;
     job_entry* current = table->job_table[idx];
     while (current != NULL && current->job_id != job_id){
        prev = current; 
        current = current->next_job;
     }
    assert(current != NULL); //no existe el job
    if(prev == NULL)
      table->job_table[idx] = current->next_job; // el job que queriamos borrar era el primero
    else
        prev->next_job = current->next_job;
    
    table->active_count--;
    DestroyJob(current);
    pthread_mutex_unlock(&table->mutexTable);
    
}

void PrintResources(granted_t* resources){
    granted_t* look = resources;
    while(look!=NULL){
    printf("<%s: %d> ", look->type, look->amount);
    look = look->next;}
printf("\n");
}

void PrintJob(job_entry* job){
    printf("job_id: <%d> socket: <%d> time: <%s>", job->job_id, job->origin_socket, 
                                                      ctime(&job->timestamp));
    printf("active resources: ");
    PrintResources(job->resources);
}
void PrintTable(active_jobs* table){
    printf("START\n");

    for(int i = 0; i < TABLE_SIZE; i++){
        job_entry* print = table->job_table[i];
        if(print == NULL){
            printf("----\n");
        }
        while(print != NULL){
            PrintJob(print);
            print = print->next_job;
        }
       
    }
    printf("END\n");
}

/*-------- queue functions ---------*/

void init_queue(fifo_queue_t* queue) {
    queue->head = NULL;
    queue->tail = NULL;
    pthread_mutex_init(&queue->queue_mutex, NULL);
}

void enqueue_job(fifo_queue_t* queue, job_entry* job, int amount) {
    pending_node_t* new_node = malloc(sizeof(pending_node_t));
    if (!new_node) return;
    
    new_node->job = job;
    new_node->amount_req = amount;
    new_node->next = NULL;

    /* we protect the incertion to the queue */
    pthread_mutex_lock(&queue->queue_mutex);

    if (queue->tail == NULL) {
        queue->head = new_node;
        queue->tail = new_node;
    } else {
        queue->tail->next = new_node;
        queue->tail = new_node;
    }

    pthread_mutex_unlock(&queue->queue_mutex);
}


void process_queue(fifo_queue_t* queue, int* available_resource, const char* resource_name) {
    /* take the lock */
    pthread_mutex_lock(&queue->queue_mutex);
    int resources = 1; // to stop if we cant give the resources
    while (queue->head != NULL && resources) {
        pending_node_t* first = queue->head;

        /* here we assume that we had the lock to the global resources previous to the call
        of this function  */
        if (*available_resource >= first->amount_req) {
            *available_resource -= first->amount_req;

            char msg[128];
            strcpy(msg, ("GRANTED %d\n", first->job->job_id));
            send(first->job->origin_socket, msg, strlen(msg), MSG_NOSIGNAL);

            printf("[INFO] Desencolando: Trabajo %d obtuvo %d de %s.\n",
                   first->job->job_id, first->amount_req, resource_name);

            queue->head = first->next;
            if (queue->head == NULL) {
                queue->tail = NULL;
            }
            free(first);
        } else {
            resources = 0; 
        }
    }

    pthread_mutex_unlock(&queue->queue_mutex);
}


/* ------- aux functions -------*/

void remove_specific_resource(job_entry* job, const char* resource_name) {
    if (job == NULL || job->resources == NULL) return;

    granted_t* actual = job->resources;
    granted_t* anterior = NULL;

    while (actual != NULL) {
        if (strcmp(actual->type, resource_name) == 0) {
            /* we find it so we take it from the queue */
            if (anterior == NULL) {
                job->resources = actual->next; // it was the first-one
            } else {
                anterior->next = actual->next; // it was in the middle or at the end
            }
            
            /* we free the memory */
            DestroyGranted(actual);
            return;
        }
        anterior = actual;
        actual = actual->next;
    }
}

