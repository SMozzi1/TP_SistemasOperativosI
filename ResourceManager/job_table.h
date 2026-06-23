#ifndef _JOB_TABLE_H_
#define _JOB_TABLE_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

#define TABLE_SIZE 256
#define IP_LEN 16

//Granted resource structure
typedef struct granted_t {
    char type[8];           /* resource type: gpu, mem, cpu */
    int amount;             /*reserved amount*/
    int provider_fd;        /* fd provided by resource (used for release) */
    char dest_ip[IP_LEN];   /* ip of the node to request the resource from */
    int dest_port;          /* port associated to that ip */
    struct granted_t* next; /* linked list of granted resources */
} granted_t;


//Job entry structure
typedef struct job_entry { 
    int job_id;
    int origin_socket;
    time_t timestamp; 
    granted_t* resources;
    granted_t* next_req; /*aux pointer*/
    struct job_entry* next_job; 


} job_entry; 

//job table
typedef struct active_jobs {
   job_entry* job_table[TABLE_SIZE];
   int active_count; //amount of total jobs in the table
   pthread_mutex_t mutexTable; 
} active_jobs;

typedef struct active_nodes {
    int* node_fd[TABLE_SIZE];
}active_nodes;

/*-----Granted Resource interface-----------*/
granted_t* MakeGranted(char* type, int amount, char* dest_ip);
void DestroyGranted(granted_t* granted_res);
void DestroyGrantedList(granted_t* granted_list);
void PrintResources(granted_t* resources);

/*------Job_entry interface----------------*/
job_entry* MakeJob(int job_id, int origin_socket, time_t time);
void DestroyJob(job_entry* job);
void AddResource(job_entry* job, granted_t* res);
void PrintJob(job_entry* job); 

/*------jobs table interfae--------------*/
void JobsTableInit(active_jobs* table);
void DestroyJobsTable(active_jobs* table);
int HashF(int job_id);
void JobsTableInsert(active_jobs* table, job_entry* job);
job_entry* FindJob(active_jobs* table, int job_id);
void RemoveJob(active_jobs* table, int job_id);
void PrintTable(active_jobs* table);
job_entry* FindJobBySocket(active_jobs* table, int job_id, int origin_socket);
job_entry* BuscarJobPorFD(int fd_listo);




#endif /*_JOB_TABLE_H_*/