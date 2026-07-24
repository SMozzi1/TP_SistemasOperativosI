#include "hash.h"
#include <assert.h>


// specific funcionts for Server nodes
unsigned func_hash_node(void* data){
    received_node* node = (received_node*) data;
    return (unsigned)(node->ip); // we hash using the ip of the node, since it is unique for each node
}

int comp_node(void* data1, void* data2){
    received_node* node1 = (received_node*)data1;
    received_node* node2 = (received_node*)data2;

    return !((node1->ip == node2->ip)); // 0 if true other if false
}

void* copy_node(void* data){
    received_node* node = (received_node*)data;
    received_node* copy = malloc(sizeof(struct _received_node));
    assert(copy != NULL);
    *copy = *node;
    return copy;
}

void destr_node(void* data){
    free(data);
}

// specific functions for Server jobs
unsigned func_hash_job(void* data){
    received_job* job = (received_job*) data;
    return (job->id + job->ip + job->port);
}
int comp_job(void* data1, void* data2){
    received_job* job1 = (received_job*) data1;
    received_job* job2 = (received_job*) data2;

    return !(job1->id == job2->id && job1->ip == job2->ip && job1->port == job2->port);
}
void* copy_job(void* data){
    received_job* job = (received_job*) data;
    received_job* copy = malloc(sizeof(struct _received_job));
    *copy = *job;
    return copy;
}
void dest_job(void* data){
    free (data);
}

TablaHash create_table_nodes(){
    return tablahash_create(copy_node, comp_node, destr_node, func_hash_node);
}


//
TablaHash create_table_jobs(){
    return tablahash_create(copy_job, comp_job, dest_job, func_hash_job);
}


// functions for local jobs

unsigned func_hash_fd(void *data) {
    fd_job_entry *e = (fd_job_entry *)data;
    return (unsigned)e->fd;
}

int comp_fd(void *data1, void *data2) {
    fd_job_entry *a = (fd_job_entry *)data1;
    fd_job_entry *b = (fd_job_entry *)data2;
    return !(a->fd == b->fd);
}

void *copy_fd(void *data) {
    fd_job_entry *orig = (fd_job_entry *)data;
    fd_job_entry *copia = malloc(sizeof(fd_job_entry));
    *copia = *orig;
    return copia;
}

void destr_fd(void *data) { free(data); }

TablaHash create_table_fd_jobs() {
    return tablahash_create(copy_fd, comp_fd, destr_fd, func_hash_fd);
}


// functions for the owner table of local jobs (keyed by job_id)

unsigned func_hash_localjob(void *data) {
    local_job_t *job = (local_job_t *)data;
    return (unsigned)job->job_id;
}

int comp_localjob(void *data1, void *data2) {
    local_job_t *a = (local_job_t *)data1;
    local_job_t *b = (local_job_t *)data2;
    return !(a->job_id == b->job_id);
}

// Pointer-owning "copy": the table stores the same heap pointer instead of
// cloning, so the fd index and the owner share one allocation. Never free a
// local_job_t elsewhere; free happens once here, via destr_localjob.
void *copy_localjob(void *data) {
    return data;
}

void destr_localjob(void *data) {
    local_job_t *job = (local_job_t *)data;
    pending_resource_t *r = job->next_req;
    while (r != NULL) {
        pending_resource_t *next = r->next;
        free(r);
        r = next;
    }
    r = job->granted_reqs;
    while (r != NULL) {
        pending_resource_t *next = r->next;
        free(r);
        r = next;
    }
    free(job);
}

TablaHash create_table_local_jobs() {
    return tablahash_create(copy_localjob, comp_localjob, destr_localjob, func_hash_localjob);
}