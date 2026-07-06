#include "loopfunc.h"


int connect_erlang(int fd, int epollfd ) {

    int new_fd = accept4(fd, NULL, NULL, SOCK_NONBLOCK);
    if (new_fd < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK)
        log_error("Erlang accept4");
        //A fd never will be negative, so we can return -1 to indicate an error and let the caller handle it.
        return -1;
    }
    struct epoll_event ev;
    ev.events  = EPOLLIN;
    ev.data.fd = new_fd;

    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, new_fd, &ev) < 0) {
        log_error("epoll_ctl ADD erlang conn");
        close(new_fd);
        return -1;
    }


    printf("[EVENT A] Erlang connected on fd=%d\n", new_fd); 
    return new_fd;

}



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


//Inentar bajar las variables, podria usar la ayuda del globals.h para las variables 
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



