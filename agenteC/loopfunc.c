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
// Announces this node's OWN free resources, read from each queue's
// resources_left under its own mutexQueue (best-effort snapshot).
void udp_broadcast(int socket_UDP, int port) {
    char msg[126];

    pthread_mutex_lock(&cpu_queue->mutexQueue); int cpu = cpu_queue->resources_left; pthread_mutex_unlock(&cpu_queue->mutexQueue);
    pthread_mutex_lock(&mem_queue->mutexQueue); int mem = mem_queue->resources_left; pthread_mutex_unlock(&mem_queue->mutexQueue);
    pthread_mutex_lock(&gpu_queue->mutexQueue); int gpu = gpu_queue->resources_left; pthread_mutex_unlock(&gpu_queue->mutexQueue);

    /*
    Format: ANNOUNCE <port> <resources>
    The sender IP is NOT in the payload; it is extracted
    from recvfrom() by the receiving node.
    */
    snprintf(msg, sizeof(msg), "ANNOUNCE %d cpu:%d mem:%d gpu:%d\n", port, cpu, mem, gpu);

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
        printf("[UDP BCAST] Announcement sent: %s", msg);
}

//E
void udp_datagram_from_remote(int fd){
      char buf[BUFFER_LEN];
    struct sockaddr_in sender;
    socklen_t slen = sizeof(sender);

    int bytes = recvfrom(fd, buf, sizeof(buf) - 1, 0, (struct sockaddr *)&sender, &slen);
    if (bytes <= 0) return;

    buf[bytes] = '\0';
    char copy[BUFFER_LEN];
    strncpy(copy, buf, sizeof(copy) - 1);
    copy[sizeof(copy) - 1] = '\0';

    char *tokens[10];
    int num = get_token(copy, tokens, 10);

    if (num < 5) return; // ANNOUNCE <port> cpu:.. mem:.. gpu:..

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
    printf("[UDP E] Received ANNOUNCE from %s:%d - cpu:%d mem:%d gpu:%d\n",
           inet_ntoa(sender.sin_addr), port, nodo.cpu, nodo.mem, nodo.gpu);
}

//F
void message_from_erlang(int fd, int epollfd){
    
    char line[BUFFER_LEN];
    int result;

    while ((result = read_until_newline(fd, line)) == 1) {
        printf("[ERLANG ->] %s", line);
        // Monotonic timestamp so the job timeout sweep compares like-for-like.
        erlang_to_C(line, get_monotonic_time());
    }
    
    if (result == -1) {
        fprintf(stderr, "[EVENT F] Erlang disconnected\n");
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
    // Find the local job that owns this outbound FD via the fd index.
    fd_job_entry key;
    key.fd = fd;
    fd_job_entry* found = (fd_job_entry*) tablahash_buscar(table_ourjobs, &key);

    if (found != NULL && found->job != NULL && found->job->next_req != NULL) {
        local_job_t* job = found->job;
        char msg[256];
        snprintf(msg, sizeof(msg), "RESERVE %d %s %d\n",
                 job->job_id, job->next_req->type, job->next_req->amount);

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
}

//H
void recive_message_from_remote(int fd, int epollfd){
    char line[BUFFER_LEN];
    int result;

    /* Drain every complete line already available (messages can arrive
     * coalesced, and edge-triggered outbound sockets require full draining). */
    while ((result = read_until_newline(fd, line)) == 1) {
        printf("[REMOTE ->] fd=%d: %s", fd, line);
        /*
            Dispatch based on content:
            - RESERVE/RELEASE -> remote node requests or frees a resource
            - GRANTED/DENIED  -> reply to a RESERVE we sent
        */
        client_to_myserver(fd, line);
    }

    if (result == -1) {
        fprintf(stderr, "[EVENT H] Remote node fd=%d disconnected\n", fd);
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
        /* ENOENT is expected when a GRANTED just turned this fd into a held
         * provider connection and removed it from epoll (see B1); ignore it. */
        if (epoll_ctl(epollfd, EPOLL_CTL_MOD, fd, &ev_rearm) < 0 && errno != ENOENT) {
            log_error("epoll_ctl MOD rearm");
        }
    }

}