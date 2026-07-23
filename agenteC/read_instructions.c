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




char* get_udp_message(int fd){

    char buf[BUFFER_LEN];
    struct sockaddr_in sender;
    socklen_t slen = sizeof(sender);

    int bytes = recvfrom(fd, buf, sizeof(buf) - 1, 0, (struct sockaddr *)&sender, &slen);
    if (bytes <= 0) return NULL;

    buf[bytes] = '\0';
    char copy[BUFFER_LEN];
    strncpy(copy, buf, sizeof(copy) - 1);
    copy[sizeof(copy) - 1] = '\0';

    char *tokens[10];
    int num = get_token(copy, tokens, 10);

    if (num < 2) return NULL; // At least we need ANNOUNCE and the port

    int ip_int = (int)sender.sin_addr.s_addr;
    int port = atoi(tokens[1]);

    received_node nodo;
    nodo.ip = ip_int;
    nodo.port = port;
    nodo.cpu = get_quantity(tokens[2]);
    nodo.mem = get_quantity(tokens[3]);
    nodo.gpu = get_quantity(tokens[4]);

    
    nodo.time = get_monotonic_time();

    //if it is new then it insert in hash_nodes, otherwise replace it 
    tablahash_insertar(table_nodes, (void*)&nodo);

    return NULL;
}

char* get_nodes_instance() {

    pthread_mutex_lock(&table_nodes->table_mutex);

    size_t buffer_size = 8192;
    char* result = malloc(buffer_size);

    if (result == NULL) {
        pthread_mutex_unlock(&table_nodes->table_mutex);
        return NULL;
    }

    int offset = snprintf(result, buffer_size, "NODES ");
    int first = 1;  // para saber cuándo NO anteponer ';'

    for (unsigned i = 0; i < table_nodes->capacidad; i++) {
        NodoLista* actual = table_nodes->elems[i].lista;

        while (actual != NULL) {
            received_node* nodo = (received_node*)actual->dato;

            // Separador ';' antes de cada nodo salvo el primero
            if (!primero && (size_t)offset < buffer_size) {
                offset += snprintf(result + offset, buffer_size - offset, ";");
            }
            primero = 0;

            // Convertimos la IP de int a string legible para el protocolo
            struct in_addr addr;
            addr.s_addr = (uint32_t)nodo->ip;
            char ip_str[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &addr, ip_str, sizeof(ip_str));

            if ((size_t)offset < buffer_size) {
                offset += snprintf(result + offset, buffer_size - offset,
                                    "%s:%d:cpu:%d:mem:%d:gpu:%d",
                                    ip_str,
                                    nodo->port,
                                    nodo->cpu,
                                    nodo->mem,
                                    nodo->gpu);
            }

            actual = actual->next;
        }
    }

    pthread_mutex_unlock(&table_nodes->table_mutex);

    // Para que Erlang sepa dónde termina de escuchar
    if ((size_t)offset < buffer_size - 1) {
        snprintf(result + offset, buffer_size - offset, "\n");
    }

    return result;
}
