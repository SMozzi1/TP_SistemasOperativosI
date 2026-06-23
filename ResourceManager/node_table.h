#ifndef _NODE_TABLE_H_
#define _NODE_TABLE_H_

#include <time.h>
#include <pthread.h>

#define TABLE_SIZE 256
#define NODE_TIME_OUT 15


typedef struct node_res{
    int cpu;
    int mem;
    int gpu;
} node_res;


typedef struct node_entry{
    char ip[50];
    int fd;
    int port;
    node_res res;
    time_t last_seen;
    struct node_entry* next;

}node_entry;

typedef struct node_table{
    node_entry* buckets[TABLE_SIZE];
    int count;
    pthread_mutex_t lock;

}node_table;
//initialize resource
node_res MakeNodeRes(int cpu, int mem, int gpu);

//node entry constructor
node_entry* MakeNodeEntry(char* ip, int fd, int port, node_res res, time_t last_seen);
void DestroyNode(node_entry* node);
void DestroyNodeList(node_entry * node);
void NodeTableInit(node_table* table);
void DestroyNodeTable(node_table* table);
void NodeTableInsert(node_table* table, node_entry* node);
node_entry* FindNode(node_table* table, char* ip, int port);
void NodeTableUpsert(node_table* table, char* ip, int port, node_res res, time_t timestamp);
void RemoveStaleNodes(node_table* table);
void NodeTableToStr(node_table* table, char* out, int max_len);







#endif /*_NODE_TABLE_H_*/