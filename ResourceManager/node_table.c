#include "node_table.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <assert.h>

//TODO: explain whatever is going on here

node_res MakeNodeRes(int cpu, int mem, int gpu){
    node_res new;
    new.cpu = cpu; new.mem = mem; new.gpu = gpu;
    return new;
}

node_entry* MakeNodeEntry(char* ip, int port, node_res res, time_t last_seen){
    node_entry* node = malloc(sizeof(struct node_entry));
    assert(node);
    strncpy(node->ip, ip, sizeof(node->ip)- 1);
    node->ip[sizeof(node->ip)-1] = '\0';

    node->port = port;
    node->res = res;
    node->last_seen = last_seen;
    node->next = NULL;

    return node;
}

void DestroyNode(node_entry* node){
    free(node);
}

void DestroyNodeList(node_entry * node){
    while(node != NULL){
        node_entry* destroy = node;
        node = node->next;
        DestroyNode(destroy);
        
    }
}
void NodeTableInit(node_table* table){
    for(int i = 0; i < TABLE_SIZE; i++){
        table->buckets[i] = NULL;
    }
       table->count = 0;
       pthread_mutex_init(&table->lock, NULL);
}

void DestroyNodeTable(node_table* table){
   for(int i = 0; i < TABLE_SIZE; i++){
        node_entry* node = table->buckets[i];
        while(node != NULL){
            node_entry* destroy = node;
            node = node->next; 
            DestroyNode(destroy);
        }
        table->buckets[i] = NULL;
      }
        pthread_mutex_destroy(&table->lock);
        table->count = 0; 
}

static int HashIP(const char *ip, int port){
    char key[64];
    snprintf(key, sizeof(key), "%s:%d", ip, port);

    unsigned long h = 5381;
    for (int i = 0; key[i] != '\0'; i++)
        h = h * 33 + (unsigned char)key[i];

    return (int)(h % TABLE_SIZE);
}

void NodeTableInsert(node_table* table, node_entry* node){
    pthread_mutex_lock(&table->lock);
   int idx = HashIP(node->ip, node->port); 
    node->next= table->buckets[idx];
    table->buckets[idx] = node;
    table->count++; 
    pthread_mutex_unlock(&table->lock);
}

node_entry* FindNode(node_table* table, char* ip, int port){
    pthread_mutex_lock(&table->lock);
    int search = HashIP(ip,port);
    node_entry* look = table->buckets[search];
    while( look != NULL && 
        !(strcmp(look->ip,ip) == 0 && look->port == port)){
        look = look->next;
       }
    if (look == NULL){
    printf("node <%s:%d>\t is not in the table\n", ip, port);
    }
    pthread_mutex_unlock(&table->lock);
    return look;
}


//we need this to avoid a Double lock in NodeTableUpsert
static node_entry* FindNode_nolock(node_table* table, char* ip, int port){
    int search = HashIP(ip,port);
    node_entry* look = table->buckets[search];
    while( look != NULL && 
        !(strcmp(look->ip,ip) == 0 && look->port == port)){
        look = look->next;
       }
    if (look == NULL){
    printf("node <%s:%d>\t is not in the table\n", ip, port);
    }
    return look;
}

static void NodeTableInsert_nolock(node_table* table, node_entry* node){
   int idx = HashIP(node->ip, node->port); 
    node->next= table->buckets[idx];
    table->buckets[idx] = node;
    table->count++; 

}



void NodeTableUpsert(node_table* table, char* ip, int port, node_res res, time_t timestamp){
   pthread_mutex_lock(&table->lock);
   node_entry* node = FindNode_nolock(table, ip, port);
   if(node == NULL){ //node is not on the table so we add it.
    node = MakeNodeEntry(ip, port, res, timestamp);
    NodeTableInsert_nolock(table, node);
   }
   else{ //update time
        node->res = res;
        node->last_seen = timestamp;
   }
    pthread_mutex_unlock(&table->lock);
}
//remove nodes that hadn't been updated for more than 15 seconds
void RemoveStaleNodes(node_table* table){
    pthread_mutex_lock(&table->lock);
    for(int i = 0; i < TABLE_SIZE; i++){
        node_entry* prev = NULL;
        node_entry* current = table->buckets[i];

        while (current != NULL){
            time_t now = time(NULL);
            if (difftime(now, current->last_seen) > NODE_TIME_OUT){
                node_entry* next = current->next;
                if(prev == NULL){ //first on the list
                    table->buckets[i] = next;
                }
                else{
                        prev->next = next;
                }

                DestroyNode(current);
                table->count--;
                current = next;
            } else{
                 prev = current;
                 current = current->next;   
            }
        }

    }
        pthread_mutex_unlock(&table->lock);
}


void NodeTableToStr(node_table* table, char* out, int max_len){
    snprintf(out,max_len, "NODES ");
    int current_len = strlen(out);

    pthread_mutex_lock(&table->lock);

    for(int i = 0; i < TABLE_SIZE; i++ ){
    node_entry* current = table->buckets[i];
        while(current!=NULL){
            char node_str[250];
            snprintf(node_str, sizeof(node_str), "%s:%d:cpu:%d:mem:%d:gpu:%d;",
                current->ip, current->port,
                current->res.cpu, current->res.mem,
                current->res.gpu );
            int node_len = strlen(node_str);
            if (current_len + node_len < max_len){
                strncat(out,node_str,max_len-current_len -1);
                current_len += node_len;
            }else{
                break; //buffer is full
            }
            current = current->next;
        }
    }
    pthread_mutex_unlock(&table->lock);
 
    //cleaning buffer operations:
    //we check that after "NODES " we send an actual list of nodes
    //and is terminated properly. Else we send NODES \n to tell erlang that
    //it's an empty list
    if (current_len > 6 && out[current_len - 1 ] == ';'){
        out[current_len - 1] = '\n';
    } else{
        strncat(out, "\n", max_len - current_len -1);
    }
}


