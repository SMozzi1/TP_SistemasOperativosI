#include <pthread.h>
#include <string.h>
#include "../ResourceManager/job_table.h"


typedef struct {
    int broadcast_timer_fd;   /* UDP broadcast timer  (fires every 5 s) */
    int timeout_timer_fd;     /* Job timeout checker  (fires every 5 s) */
} worker_args_t;


static int make_timer(int epollfd, int initial_sec, int interval_sec);
void check_job_timeouts(int erlangfd, active_jobs tabla_propia);


void update_local_resources(const char *resource_name, int amount);
void release_resources(job_entry* job);
void original_socket(job_entry* job, int fd);
void ask_for_next_resource(int epollfd, int erlangfd, job_entry* job);

void enqueue_jobs(const char* resource, int job_id, int amount, int fd_actual);
void reserve_elements();

char* obtener_string_nodos(job_entry* job);
