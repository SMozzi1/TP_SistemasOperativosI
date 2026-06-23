// globales.h
#ifndef GLOBALES_H
#define GLOBALES_H


#include "../ResourceManager/job_table.h"
#include "../ResourceManager/resource_manager.h"


extern active_jobs table_ourjobs;
extern active_jobs table_nodes;
extern active_jobs table_clients;



extern p_queue_t cpu_queue;
extern p_queue_t mem_queue;
extern p_queue_t gpu_queue;


extern int cpu_available;
extern int mem_available;
extern int gpu_available;



extern pthread_mutex_t mutex_resources;


extern int epollfd;
extern int erlangfd;

#endif