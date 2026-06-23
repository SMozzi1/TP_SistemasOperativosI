#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include <time.h>
#include <string.h>
#include <sys/socket.h>

#include "../agenteC/globals.h"
#include "job_table.h"

#define TABLE_SIZE 256

//granted constructor
granted_t* MakeGranted(char* type, int amount, char* dest_ip){
    granted_t* new = malloc(sizeof(struct granted_t));
    assert(new);
    strncpy(new->type,type, sizeof(new->type) - 1);
    new->type[sizeof(new->type) - 1] = '\0'; 
    new->amount = amount;

    strncpy(new->dest_ip, dest_ip, IP_LEN - 1);
    new->dest_ip[IP_LEN - 1] = '\0';

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
    job_entry* new = malloc(sizeof(struct job_entry));
    assert(new);
    new->job_id = job_id;
    new->origin_socket = origin_socket;
    new->timestamp = time;
    new->resources = NULL; 
    new->next_job = NULL;
    new->next_req = NULL;
    
    return new;
    } 

//job entry destructor 
void DestroyJob(job_entry* job){
    DestroyGrantedList(job->resources);
    free(job);
}


//Adds resource at the end of the list of granted resources
void AddResource(job_entry* job, granted_t* res){

    res->next = NULL;

    if (job->resources == NULL) { 
        job->resources = res;
    }
    else {
        granted_t* actual = job->resources;
        while (actual->next != NULL) {
            actual = actual->next; // Iterate until it reaches end of the list
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

/*hashing function*/
int HashF(int job_id){
     return job_id % TABLE_SIZE;
}

//Inserts a job in the table
void JobsTableInsert(active_jobs* table, job_entry* job){
    pthread_mutex_lock(&table->mutexTable);
   int idx = HashF(job->job_id); 
    job->next_job = table->job_table[idx];
    table->job_table[idx] = job;
    table->active_count++; 
    pthread_mutex_unlock(&table->mutexTable);
}

//returns job_entry corresponding to specified by job_id 
job_entry* FindJob(active_jobs* table, int job_id){
    pthread_mutex_lock(&table->mutexTable);
    int search = HashF(job_id);
    job_entry* look = table->job_table[search];
    while( look != NULL && look->job_id != job_id ){
        look = look->next_job;
       }
    pthread_mutex_unlock(&table->mutexTable);
    return look;
}

//Removes job from the table.   
void RemoveJob(active_jobs* table, int job_id){
    pthread_mutex_lock(&table->mutexTable);
     int idx = HashF(job_id);
     job_entry* prev = NULL; /*tracks the node behind current for relinking*/

     job_entry* current = table->job_table[idx];

     /* traverse the bucket list until we find the job or exhaust the list */
     while (current != NULL && current->job_id != job_id){
        prev = current; 
        current = current->next_job;
     }

    /* job not found — nothing to remove */
    if (current == NULL) {                
        pthread_mutex_unlock(&table->mutexTable);
        return;
    }
    /*we relink the nodes bypassing the node we are removing*/
    if(prev == NULL)
      table->job_table[idx] = current->next_job; /* removed node was the head of the bucket */
    else
     /* The target node is mid-list*/
        prev->next_job = current->next_job;
    
    table->active_count--;
    DestroyJob(current);
    pthread_mutex_unlock(&table->mutexTable);
    
}
/*prints resources*/
void PrintResources(granted_t* resources){
    granted_t* look = resources;
    while(look!=NULL){
    printf("<%s: %d> ", look->type, look->amount);
    look = look->next;}
printf("\n");
}
/*prints job*/
void PrintJob(job_entry* job){
    printf("job_id: <%d> socket: <%d> time: <%s>", job->job_id, job->origin_socket, 
                                                      ctime(&job->timestamp));
    printf("active resources: ");
    PrintResources(job->resources);
}
/*prints entire table*/
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

/*Search jobs by fd inside of table_ourjobs*/
job_entry* BuscarJobPorFD(int fd) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        job_entry* current = table_ourjobs.job_table[i];
        while (current != NULL) {
            if (current->origin_socket == fd) {
                return current;
            }
            current = current->next_job;
        }
    }
    return NULL; /*no job with given fd found*/
}

/*searches job by fd inside a given table*/
job_entry* FindJobBySocket(active_jobs* table, int job_id, int origin_socket){
    pthread_mutex_lock(&table->mutexTable);
    int idx = HashF(job_id);
    job_entry* look = table->job_table[idx];
    while (look != NULL && !(look->job_id == job_id && look->origin_socket == origin_socket)) {
        look = look->next_job;
    }
    pthread_mutex_unlock(&table->mutexTable);
    return look;
}