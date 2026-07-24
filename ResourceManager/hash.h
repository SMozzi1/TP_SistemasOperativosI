#ifndef _HASH_
#define _HASH_

#include <stdio.h>
#include <time.h>
#include "tablahash.h"
#define HASH_SIZE 256


// resources structure
//Inside local_job struct
typedef struct pending_resource {
    char type[8];
    int amount;
    char dest_ip[16];
    int dest_port;
    int provider_fd;              // socket of the node that granted this resource (-1 until granted)
    struct pending_resource *next; //a list that have Node1 cpu-> Node2 mem -> Node3 gpu
} pending_resource_t;


/* --- Server ---*/


/**
 * in this section we have all the logistics of the jobs and nodes that we receive from other nodes
 */

typedef struct _received_node
{
    int ip;
    int port;
    int gpu;
    int mem;
    int cpu;
    time_t time;

}received_node;

//Jobs we get after a reserve
typedef struct _received_job
{
    int id;
    int ip;
    int port;
    int cpu_granted; 
    int mem_granted;
    int gpu_granted;
    int original_socket; // the socket of the node that is asking for resources

}received_job;


// specific funcionts for nodes
unsigned func_hash_node(void* data);
int comp_node(void* data1, void* data2);
void* copy_node(void* data);
void destr_node(void* data);

// specific functions for jobs
unsigned func_hash_job(void* data);
int comp_job(void* data1, void* data2);
void* copy_job(void* data);
void dest_job(void* data);

HashTable create_table_nodes();
HashTable create_table_jobs();

/* --- Client --- */ 

/*
* in this section we will have all the local jobs logistics that we receive from erlang
Job_request logic
*/

typedef struct {
    int job_id;
    int erlang_socket;
    int origin_socket;        // fd of the node that we are asking for resources
    pending_resource_t *next_req;
    pending_resource_t *granted_reqs; // list of granted resources
    time_t timer; // to know if it has a timeout
} local_job_t;

typedef struct {
    int fd;  
    local_job_t *job;
} fd_job_entry;

unsigned func_hash_fd(void *data);
int comp_fd(void *data1, void *data2);
void *copy_fd(void *data);
void destr_fd(void *data);
HashTable create_table_fd_jobs();

/*
 * Owner table for the local jobs we run on Erlang's behalf. Keyed by job_id.
 * IMPORTANT: this table takes OWNERSHIP of the local_job_t pointer (the copy
 * function returns the pointer as-is instead of cloning), so the single heap
 * allocation is shared with the fd index (fd_job_entry.job). The destructor
 * frees the struct AND both pending_resource_t lists exactly once, on removal.
 */
unsigned func_hash_localjob(void *data);
int comp_localjob(void *data1, void *data2);
void *copy_localjob(void *data);
void destr_localjob(void *data);
HashTable create_table_local_jobs();

#endif // _HASH_