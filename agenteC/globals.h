#ifndef _GLOBALS_H_
#define _GLOBALS_H_


#include "../ResourceManager/hash.h"
#include "../ResourceManager/resource_queue.h"


#define BUFFER_LEN 1024      // Standard buffer size for reading network data

//extern TablaHash hash_nodo;
//extern TablaHash hash_job;

extern TablaHash table_ourjobs;
extern TablaHash table_nodejobs;
extern TablaHash table_nodes;

extern request_queue* cpu_queue;
extern request_queue* mem_queue;
extern request_queue* gpu_queue;

extern int epollfd;
extern int erlangfd;

#endif