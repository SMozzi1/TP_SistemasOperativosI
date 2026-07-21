#include "loopfunc.h"

//A
int connect_erlang(int fd, int epollfd ) {

    int new_fd = accept4(fd, NULL, NULL, SOCK_NONBLOCK);
    if (new_fd < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK)
        log_error("Erlang accept4");
        //A fd never will be negative, so we can return -1 to indicate an error and let the caller handle it.
        return -1;
    }
    struct epoll_event ev;
    ev.events  = EPOLLIN | EPOLLONESHOT;
    ev.data.fd = new_fd;

    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, new_fd, &ev) < 0) {
        log_error("epoll_ctl ADD erlang conn");
        close(new_fd);
        return -1;
    }


    printf("[EVENT A] Erlang connected on fd=%d\n", new_fd); 
    return new_fd;

}


//B
void connect_client(int fd, int epollfd){
    struct sockaddr_in client_addr;
    socklen_t len = sizeof(client_addr);
    int client_fd = accept4(fd, (struct sockaddr *)&client_addr, &len, SOCK_NONBLOCK);
                
    if (client_fd < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK)
        log_error("Remote accept4");
        return;
    }

    /*
        EPOLLONESHOT: ensures only ONE thread processes this fd
        per event round, preventing race conditions without an
        additional per-fd mutex.
    */
    struct epoll_event ev;
    ev.events  = EPOLLIN | EPOLLONESHOT;
    ev.data.fd = client_fd;

    //Add the client_fd into epoll
    //every time this fd send us a message, erlang_wait wakes up
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
        log_error("epoll_ctl ADD remote client");
        close(client_fd);
    }

    printf("[EVENT B] Remote node connected: %s fd=%d\n", inet_ntoa(client_addr.sin_addr), client_fd);

}

//C
void udp_broadcast(int socket_UDP, int port, int cpu, int mem, int gpu) {
    //We send a message whith our port and elements
    char msg[126];
    pthread_mutex_lock(&mutex_resources);

    /*
        Format: ANNOUNCE <port> <resources>
        The sender IP is NOT in the payload; it is extracted
        from recvfrom() by the receiving node.
    */
    snprintf(msg, sizeof(msg), "ANNOUNCE %d cpu:%d mem:%d gpu:%d\n", port, cpu, mem, gpu);
    pthread_mutex_unlock(&mutex_resources);

    struct sockaddr_in bcast_addr;
    memset(&bcast_addr, 0, sizeof(bcast_addr));
    bcast_addr.sin_family      = AF_INET;
    bcast_addr.sin_port        = htons(12529); //harcoded but the port will be always the same for the broadcast
    // comento esta linea ya que para testear en docker fijo una ip de la red virtual pero es la linea correspondiente
    bcast_addr.sin_addr.s_addr = htonl(INADDR_BROADCAST);
    //bcast_addr.sin_addr.s_addr = inet_addr("10.5.255.255");
    ssize_t sent = sendto(socket_UDP, msg, strlen(msg), 0,(struct sockaddr *)&bcast_addr, sizeof(bcast_addr));

    if (sent < 0) 
        log_error("[UDP BCAST] sendto failed");
    else 
        printf("[UDP BCAST] Announcement sent: %s\n", msg);

}

//E
void udp_datagram_from_remote(int fd){
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
    char sender_ip[INET_ADDRSTRLEN];
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
}

//F
void message_from_erlang(int fd, int epollfd){
    
    char line[BUFFER_LEN];
    int result;

    while ((result = read_until_newline(fd, line)) == 1) {
        printf("[ERLANG ->] %s", line);
        erlang_to_C(line, time(NULL));
    }   
    
    if (result == -1) {
        log_error("[EVENT F] Erlang disconnected");
        epoll_ctl(epollfd, EPOLL_CTL_DEL, erlangfd, NULL);
        close(erlangfd);
        clear_connection_buffer(erlangfd);
        erlangfd = -1;
        return;
    }

    // we need to recreate the event to handle the epolloneshot.
    struct epoll_event ev;
    ev.events  = EPOLLIN | EPOLLONESHOT;
    ev.data.fd = fd;
    epoll_ctl(epollfd, EPOLL_CTL_MOD, fd, &ev);

}

//G
void send_request(int fd, int epollfd){
    // We find out which Job and which Resource own this FD
    job_entry* job = BuscarJobPorFD(fd); 
            
    if (job != NULL && job->next_req != NULL) {
        char msg[256];
        snprintf(msg, sizeof(msg), "RESERVE %d %s %d\n", job->job_id, job->next_req->type, job->next_req->amount);
                                        
        send(fd, msg, strlen(msg), MSG_NOSIGNAL);
                        
        /* 
            Rearm the epoll registration to switch from EPOLLOUT (writing) to EPOLLIN (reading).
            This ensures we stop monitoring for write-ready states and begin waiting for 
            the peer's response to our request. We use EPOLLET (Edge-Triggered) and 
            EPOLLONESHOT for high-performance, single-thread-per-event delivery.
        */
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
        ev.data.fd = fd;
        epoll_ctl(epollfd, EPOLL_CTL_MOD, fd, &ev);
    }
    ReleaseJob(job);
}

//H
void recive_message_from_remote(int fd, int epollfd){
    char line[BUFFER_LEN];
    int result = read_until_newline(fd, line);

    if (result == 1) {
        printf("[REMOTE ->] fd=%d: %s", fd, line);
        /*
            Dispatch based on content:
            - RESERVE/RELEASE -> remote node requests or frees a resource
            - GRANTED/DENIED  -> reply to a RESERVE we sent
        */
        client_to_myserver(fd, line);

    } else if (result == -1) {
        fprintf(stderr, "[EVENT G] Remote node fd=%d disconnected\n", fd);
        release_client_by_fd(fd);          // release what you had reserved for this fd
        handle_outbound_disconnect(fd);    // in case we call ourselves
        epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, NULL); //then deleate from epoll
        close(fd);
        clear_connection_buffer(fd);
        return;
    }

    /*
        After EPOLLONESHOT fires, the kernel disables monitoring for
        this fd. We must re-enable it explicitly with EPOLL_CTL_MOD.
        Rearm for result == 0 (waiting for more data) and result == 1
        (ready for the next_job message). Skip only for result == -1 (dead).
    */
    if (result >= 0) {
        struct epoll_event ev_rearm;
        ev_rearm.events  = EPOLLIN | EPOLLONESHOT;
        ev_rearm.data.fd = fd;
        if (epoll_ctl(epollfd, EPOLL_CTL_MOD, fd, &ev_rearm) < 0) {
            log_error("epoll_ctl MOD rearm");
        }
    }

}