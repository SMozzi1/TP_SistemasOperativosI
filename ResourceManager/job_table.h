#ifndef _JOB_TABLE_H_
#define _JOB_TABLE_H_


#define TABLE_SIZE 256

typedef struct granted_t {
    int 
    char type[8];//gpu, mem, cpu
    int amount;   //reserved amount
    int providerfd; //fd que dio el recurso, (para hacer realese)
    struct granted_t* next; //linked list of resources granted
} granted_t;

typedef struct job_entry { 
    int job_id;
    int origin_socket;
    time_t timestamp; //checkear bien de que tipo son.
    granted_t* resources;
    granted_t* next_req; //Apunta al proximo recurso a verificar
    struct job_entry* next_job; //colisiones por encadenamiento.

} job_entry; 

typedef struct active_jobs {
   job_entry* job_table[TABLE_SIZE];
   int active_count; //amount of total jobs in the table
   pthread_mutex_t mutexTable; 
} active_jobs;

/*-----Granted Resource interface-----------*/
granted_t* MakeGranted(char* type, int amount);
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

#endif /*_JOB_TABLE_H_*/