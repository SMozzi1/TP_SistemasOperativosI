#ifndef _GLOBALS_H_
#define _GLOBALS_H_


#include "../ResourceManager/hash.h"
#include "../ResourceManager/resource_queue.h"


#include <pthread.h>

#define BUFFER_LEN 1024      // Standard buffer size for reading network data

/* Local jobs (from Erlang):
 *  - table_localjobs : OWNER, keyed by job_id (holds the local_job_t).
 *  - table_ourjobs   : fd -> local_job_t* index for the reservation chain.
 * Remote side:
 *  - table_nodejobs  : jobs from other agents we granted resources to.
 *  - table_nodes     : discovered peer directory (from UDP ANNOUNCE).
 */
extern TablaHash table_localjobs;
extern TablaHash table_ourjobs;
extern TablaHash table_nodejobs;
extern TablaHash table_nodes;

extern request_queue* cpu_queue;
extern request_queue* mem_queue;
extern request_queue* gpu_queue;

/* This agent's own free resources. Every access MUST hold mutex_resources
 * because the NUM_WORKERS worker threads touch them concurrently. */
extern int cpu_available;
extern int mem_available;
extern int gpu_available;
extern pthread_mutex_t mutex_resources;

extern int epollfd;
extern int erlangfd;

#endif