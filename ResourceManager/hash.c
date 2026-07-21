#include "hash.h"
#include <assert.h>


// specific funcionts for nodes
unsigned hash_node(void* data){
    received_node* node = (received_node*) data;
    return (unsigned)(node->ip + node->port);
}

int comp_node(void* data1, void* data2){
    received_node* node1 = (received_node*)data1;
    received_node* node2 = (received_node*)data2;

    return !((node1->ip == node2->ip) && (node1->port == node2->port)); // 0 if true other if false
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

// specific functions for jobs
unsigned hash_job(void* data){
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
    return tablahash_crear(HASH_SIZE, copy_node, comp_node, destr_node, hash_node);
}

TablaHash create_table_jobs(){
    return tablahash_crear(HASH_SIZE, copy_node, comp_node, destr_node, hash_node);
}