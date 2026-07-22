#ifndef _HASH_
#define _HASH_

#include <stdio.h>
#include <time.h>
#include "tablahash.h"
#define HASH_SIZE 256



typedef struct _received_node
{
    int ip;
    int port;
    int gpu;
    int mem;
    int cpu;
    time_t time;

}received_node;


typedef struct _received_job
{
    int id;
    int ip;
    int port;

}received_job;


// specific funcionts for nodes
unsigned hash_node(void* data);
int comp_node(void* data1, void* data2);
void* copy_node(void* data);
void dest_node(void* data);

// specific functions for jobs
unsigned hash_job(void* data);
int comp_job(void* data1, void* data2);
void* copy_job(void* data);
void dest_job(void* data);

TablaHash create_table_nodes();
TablaHash create_table_jobs();


#endif // _HASH_