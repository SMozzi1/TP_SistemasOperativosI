#include "read_instructions.h"


//Array of connection states, indexed by file descriptor. 
//Each entry holds the buffer and accumulated byte count for that specific connection.
Connection connections[MAX_FDS];




void initialize_connection_buffers() {
    for (int i = 0; i < MAX_FDS; i++) {
        connections[i].accumulated_bytes = 0;
        pthread_mutex_init(&connections[i].connlock, NULL);  // Initialize the mutex for each connection
    }
}




int get_token(char *instruction, char **token_array, int max_tokens) {

    int i = 0;
    char *saveptr;
    //strtok_r is thread safe 
    char *t = strtok_r(instruction, " \n\r", &saveptr);
    while (t != NULL && i < max_tokens) {
        token_array[i++] = t;
        t = strtok_r(NULL, " \n\r", &saveptr);
    }
    return i;

}






void clear_connection_buffer(int fd) {
    if (fd >= 0 && fd < MAX_FDS) {
        pthread_mutex_lock(&connections[fd].connlock);  
        connections[fd].accumulated_bytes = 0;
        pthread_mutex_unlock(&connections[fd].connlock); 
    }
}






int read_until_newline(int fd, char *output_line) {
    if (fd < 0 || fd >= MAX_FDS) return -1; // Invalid file descriptor

    pthread_mutex_lock(&connections[fd].connlock);  // Lock the mutex for this connection

    /*  storage: Pointer to the persistent character buffer assigned to this specific file descriptor.
        It holds the raw data stream until a full, newline-terminated message is detected.
        acc: Pointer to the counter tracking how many bytes are currently held in the 'storage' buffer.
        It allows the function to know where to append new data and how much remains after a line is extracted.
    */
    char  *storage = connections[fd].buffer;
    int   *acc     = &connections[fd].accumulated_bytes;

    if (*acc > 0) {
        fprintf(stderr, "[DEBUGGEANDO] fd=%d ya tenía %d bytes acumulados al reusarse: %.80s\n", fd, *acc, storage);
    }

    int space = BUFFER_LEN - *acc - 1;
    
    if (space <= 0) {
        fprintf(stderr, "[WARN] read_until_newline: buffer full on fd=%d, resetting\n", fd);
        *acc = 0;
        space = BUFFER_LEN - 1;
    }

    char temp[1024];
    int  n = recv(fd, temp, space, 0);

    if (n == 0){
        pthread_mutex_unlock(&connections[fd].connlock);  // Unlock before returning
        return -1;  /* Peer closed the connection */
    }

    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            pthread_mutex_unlock(&connections[fd].connlock);  // Unlock before returning
            return 0;
        }
        pthread_mutex_unlock(&connections[fd].connlock);  // Unlock before returning
        return -1;
    }

    memcpy(&storage[*acc], temp, n);
    *acc += n;
    storage[*acc] = '\0';

    /* Searches for the first newline character in the accumulated buffer. */
    char *nl = strchr(storage, '\n');

    /* If a complete line is detected (nl != NULL). */
    if (nl != NULL) {

        int line_len = (nl - storage) + 1;
        memcpy(output_line, storage, line_len);
        output_line[line_len] = '\0';

        int remaining = *acc - line_len;
        if (remaining > 0) {
            memmove(storage, &storage[line_len], remaining);
        }
        
        /* Updates the accumulator to reflect the remaining bytes and signifies success. */
        memset(&storage[remaining], 0, BUFFER_LEN - remaining);
        *acc = remaining;
        pthread_mutex_unlock(&connections[fd].connlock);  // Unlock before returning
        return 1;
    }
    
    pthread_mutex_unlock(&connections[fd].connlock);  // Unlock before returning
    return 0;
}




//we get a line element:quantity
int get_quantity(char* line){
    
    int i = 0;
    char *saveptr;
    //strtok_r is thread safe 
    char *t = strtok_r(line, ":", &saveptr);
    char *q = strtok_r(NULL, " ", &saveptr);
    
    i = atoi(q);
    return i;
}




int ip_str_to_int(char *ip_str) {
    struct in_addr addr;
    if (inet_pton(AF_INET, ip_str, &addr) != 1) {
        // string inválido — decidí qué hacer acá:
        // devolver 0, -1, o propagar el error de otra forma
        return -1;
    }
    return (int)addr.s_addr;
}



char* get_udp_message(int fd){
    
    char buf[BUFFER_LEN];
    struct sockaddr_in sender;
    socklen_t slen = sizeof(sender);
    
    int bytes = recvfrom(fd, buf, sizeof(buf) - 1, 0, (struct sockaddr *)&sender, &slen);
    if (bytes <= 0)  return;

    buf[bytes] = '\0';
    char copy[BUFFER_LEN];
    strncpy(copy, buf, sizeof(copy) - 1);
    copy[sizeof(copy) - 1] = '\0';

    char *tokens[10];
    int num = get_token(copy, tokens, 10);

    if (num < 2) return; // At least we need ANNOUNCE and the port

    char sender_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(sender.sin_addr), sender_ip, sizeof(sender_ip));

    //receive_node nodo == FindJob(ip, port);

    //If the node is new, we add 
    if (nodo == NULL){
        int cant;

        //pthread_mutex_lock(&mutex_table_nodes);

        //recieve_node nodo;
        //nodo.ip = sender_ip;
        //nodo.port = atoi(tokens[1]);

        
        cant = get_quantity(tokens[2]);
        //nodo->cpu = cant;
        

        cant = get_quantity(tokens[3]); //We get the amount for mem
        //nodo.mem = cant;
        

        cant = get_quantity(tokens[4]);
        //nodo->gpu = cant;
        
        
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        //nodo->time = ts.tv_sec;
        //pthread_mutex_unlock(&mutex_table_nodes);


    } else {
        //We update the Node whith the new values
            
        cant = get_quantity(tokens[2]);
        //nodo->cpu = cant;
        

        cant = get_quantity(tokens[3]); //We get the amount for mem
        //nodo.mem = cant;
        

        cant = get_quantity(tokens[4]);
        //nodo->gpu = cant;


        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        //nodo->time = ts.tv_sec;
        //pthread_mutex_unlock(&mutex_table_nodes);
    }




}


char buf[BUFFER_LEN];
    struct sockaddr_in sender;
    socklen_t slen = sizeof(sender);

    int bytes = recvfrom(fd, buf, sizeof(buf) - 1, 0, (struct sockaddr *)&sender, &slen);
    if (bytes <= 0)  return;

    buf[bytes] = '\0'; // to end when copying
    char copy[BUFFER_LEN];
    strncpy(copy, buf, sizeof(copy) - 1);
    copy[sizeof(copy) - 1] = '\0';

    char *tokens[10];
    int num = get_token(copy, tokens, 10);
    if (num < 2) return; // At least we need ANNOUNCE and the port

                
    //We get the ip 
    char sender_ip[IP_SIZE];
    inet_ntop(AF_INET, &(sender.sin_addr), sender_ip, sizeof(sender_ip));
                

    int nodo_puerto = atoi(tokens[1]);
                
    /* 
        Converts the sender's IP address (string) into a 32-bit integer identifier.
        inet_addr: Translates the IPv4 string (e.g., "172.21.155.36") into a 32-bit 
        binary representation in network byte order.This serves as a unique numeric key for the node in the job table.
    */
    int nodo_id = abs((int)inet_addr(sender_ip)); 

    // We check if the node is already in the table
    job_entry* nodo = FindJob(&table_nodes, nodo_id);
    int is_new = (nodo == NULL);

    if (!is_new) {
        nodo->timestamp = time(NULL);
        granted_t* current_res = nodo->resources;
        while (current_res != NULL) {
            granted_t* next_res = current_res->next;
            free(current_res);
            current_res = next_res;
        }
    nodo->resources = NULL;
    } else {
        nodo = MakeJob(nodo_id, nodo_puerto, time(NULL));
    }

    // Pointer for use with strtok_r
    char *saveptr1; 

    // process resources
    for(int i = 2; i < num; i++) {
        char *res_token = tokens[i]; 
                        
        // We use 'strtol_r' (reentrant) which is thread-safe.
        char *res_type  = strtok_r(res_token, ":", &saveptr1);
        char *res_amt   = strtok_r(NULL, ":", &saveptr1);

        if (res_type && res_amt) {
            granted_t* res = malloc(sizeof(granted_t));
            if (res == NULL) continue;

            strncpy(res->type, res_type, sizeof(res->type) - 1);
            res->type[sizeof(res->type) - 1] = '\0';
                            
            res->amount = atoi(res_amt);
            res->provider_fd = -1; 
                            
            strncpy(res->dest_ip, sender_ip, sizeof(res->dest_ip) - 1);
            res->dest_ip[sizeof(res->dest_ip) - 1] = '\0';
                            
            res->dest_port = nodo_puerto;
                            
            //Insert at the beginning of the node's linked list
            res->next = nodo->resources;
            nodo->resources = res;
        }
    }

    /*
        We only insert it into the table if it's a node that didn't previously exist.
        (If it already existed, we directly modify its resources using the pointer provided by FindJob)
    */
    if (is_new) {
        JobsTableInsert(&table_nodes, nodo);
        printf("[UDP RECEIVE] ¡Nuevo nodo descubierto en la red!, puerto %d \n", nodo_puerto);
    } else {
        printf("[UDP RECEIVE] Latido recibido. Nodo ya estaba en la tabla. %d\n", nodo_puerto);
    }
    ReleaseJob(nodo);

