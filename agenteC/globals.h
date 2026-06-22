// globales.h
#ifndef GLOBALES_H
#define GLOBALES_H
#include "../ResourceManager/job_table.h"
#include "../ResourceManager/resource_manager.h"


extern active_jobs table_ourjobs;
extern active_jobs table_nodos;
extern active_jobs table_recursos;


extern p_queue_t cpu_queue;
extern p_queue_t mem_queue;
extern p_queue_t gpu_queue;

extern int epollfd;
extern int erlangfd;

#endif