
/*
 * agente.c
 *
 * Entry point for the Resource Manager Agent.
 * Initializes sockets, epoll, the threads, and the event loop.
 *
 * Fixes applied over the original draft:
 *  - Consistent naming: socket_UDP (not udp_socket).
 *  - Two TCP sockets cannot bind to the same port on the same address;
 *    the Erlang socket listens on 127.0.0.1:PORT and the server socket
 *    on 0.0.0.0:PORT. Both binds are valid on Linux.
 *  - main() calls setup_epoll() (not the non-existent crear_epoll()).
 *  - ANNOUNCE format corrected: "ANNOUNCE <port> <resources>" (image 2).
 *    The sender IP is extracted from recvfrom() by the receiver.
 *  - Secondary timerfd for pending job timeouts (JOB_TIMEOUT_SEC).
 *  - UDP event: sender IP is extracted from recvfrom() struct.
 *  - memset(connections) moved into setup_epoll() to avoid ordering issues in main().
 */

#include <signal.h>
#include "globals.h"
#include "timer.h"
#include "comunicaciones.h"
#include "agente.h"

#include <arpa/inet.h>


// Inventory of available local resources.

int epollfd = -1;
int erlangfd = -1;

/* Mutex we use to prevent race condition */
pthread_mutex_t mutex_resources = PTHREAD_MUTEX_INITIALIZER;

// active_jobs table_ourjobs;
// received_job table_nodes;
// received_job table_clients;



#define MAX_EVENTS 64        // Maximum number of events epoll will process in a single wake-up
#define NUM_WORKERS 4        // Number of threads in our Thread Pool


#define BROADCAST_PORT 12529 
//Need manualy be changed

int socket_server;
int socket_erlang;
int socket_UDP;


/**
 * No necesita cambios ya que no usa estructuras de datos ni funciones de la tabla hash.
 */

//Socket initialization
static void initialize_listen_sockets(int port) {
    struct sockaddr_in server_addr, erlang_addr, udp_addr;
    //Works as a boolean to activate an socket option
    int opt = 1;

    /* Create all three sockets in non-blocking mode */
    socket_server = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    socket_erlang = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    socket_UDP    = socket(AF_INET, SOCK_DGRAM  | SOCK_NONBLOCK, 0);

    if (socket_server < 0 || socket_erlang < 0 || socket_UDP < 0)
        fatal_error("socket() failed");

    /* SO_REUSEADDR + SO_REUSEPORT allow fast server restarts without
     * "Address already in use" errors. Must be set individually.    */
    if (setsockopt(socket_server, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0 ||
        setsockopt(socket_server, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0 ||
        setsockopt(socket_erlang, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0 ||
        setsockopt(socket_erlang, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0 ||
        setsockopt(socket_UDP,    SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
        fatal_error("setsockopt REUSEADDR/REUSEPORT failed");

    /* Explicit permission to send broadcast packets on the UDP socket */
    if (setsockopt(socket_UDP, SOL_SOCKET, SO_BROADCAST, &opt, sizeof(opt)) < 0)
        fatal_error("setsockopt SO_BROADCAST failed");

    /* socket_server: listens on ALL interfaces, port PORT */
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family      = AF_INET;
    server_addr.sin_port        = htons(port);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    /*
     * socket_erlang: accepts connections from localhost ONLY (127.0.0.1).
     * Both TCP sockets can share PORT because their bind addresses differ
     * (0.0.0.0 vs 127.0.0.1).
     */
    memset(&erlang_addr, 0, sizeof(erlang_addr));
    erlang_addr.sin_family      = AF_INET;
    erlang_addr.sin_port        = htons(port);
    erlang_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    /* socket_UDP: receives broadcasts on BROADCAST_PORT */
    memset(&udp_addr, 0, sizeof(udp_addr));
    udp_addr.sin_family      = AF_INET;
    udp_addr.sin_port        = htons(BROADCAST_PORT);
    udp_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    //We bind all of them
    if (bind(socket_server, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0 ||
        bind(socket_erlang, (struct sockaddr *)&erlang_addr, sizeof(erlang_addr)) < 0 ||
        bind(socket_UDP,    (struct sockaddr *)&udp_addr,    sizeof(udp_addr))    < 0)
        fatal_error("bind() failed");

    //We only set socket_server and socket_erlang as listening sockets
    if (listen(socket_server, 10) < 0 || listen(socket_erlang, 10) < 0)
        fatal_error("listen() failed");

    // printf("[INIT] Listening on TCP 0.0.0.0:%d (nodes), 127.0.0.1:%d (Erlang), UDP %d (broadcast)\n",
    //        PORT, PORT, BROADCAST_PORT);
}


//Event loop (executed by every threads)
static void* event_loop(void *arg) {

    //We pass the same timerfd to all threads, but only one thread 
    //will be woken up by epoll at a time.
    worker_args_t     *args   = (worker_args_t *)arg;

    int port = args->port;
    struct epoll_event events[MAX_EVENTS];

    while (1) {
        int nfds = epoll_wait(epollfd, events, MAX_EVENTS, -1);

        if (nfds < 0) {
            if (errno == EINTR) continue;   /* Signal interrupted the wait; safe to retry */
                log_error("epoll_wait failed");

            continue;
        }

        for (int i = 0; i < nfds; i++) {
            
            int fd = events[i].data.fd;

            
            /* ── A: new connection from the Erlang scheduler ────────── */
            if (fd == socket_erlang) {
                
                int new_fd = connect_erlang(fd, epollfd);
                // If socket_erlang 
                // if (new_fd < 0) {
                //     // Error already logged in connect_erlang()
                //     continue;
                // }
                erlangfd = new_fd;
                  
            }

            /* ── B: new connection from a remote node ───────────────── */
            else if (fd == socket_server) {
                
                connect_client(fd, epollfd);
    
            }

            /* ── C: UDP broadcast timer fired. send a message ───────────────────────── */
            else if (fd == args->broadcast_timer_fd) {
                
                /*  Must read the timer fd to clear its readable state;
                otherwise epoll keeps waking us up in a busy loop.  
                */
               uint64_t exp;
               if (read(fd, &exp, sizeof(exp)) < 0 && errno != EAGAIN) {
                   log_error("read broadcast timer");
                   continue;
                }

                udp_broadcast(socket_UDP ,port, cpu_available, mem_available, gpu_available);

            }

            /* ── D: job timeout timer fired ─────────────────────────── */
            /*  
                Reads the timer file descriptor to clear its state and invokes timeout 
                checks for both remote node jobs and local jobs.
            */
            else if (fd == args->timeout_timer_fd) { 
                uint64_t exp;
                if (read(fd, &exp, sizeof(exp)) < 0 && errno != EAGAIN) {
                    log_error("read timeout timer");
                    continue;
                }
                check_nodes_timeouts(&table_nodes);
                check_ourjob_timeouts(&table_ourjobs);
            }

            /* ── E: incoming UDP datagram from another node ─────────── */
            //We get the message ANNOUNCE and keep the Node in table_nodes
            else if (fd == socket_UDP) {
              
                udp_datagram_from_remote(fd);
                
            }

            /* ── F: incoming TCP data from Erlang ───────────────────── */
            /// Read messages arriving from Erlang
            else if (fd == erlangfd) {

                message_from_erlang(fd, epollfd);

            }

            /* 
                ── G: Send message from a job requesting - Request job
                
                If a socket is flagged with EPOLLOUT, it indicates that the underlying 
                network buffer is ready to transmit data; we use this event to trigger 
                the dispatch of a pending resource request.
                */
            else if (events[i].events & EPOLLOUT) {

                send_request(fd, epollfd);

            }

            /* ── H: incoming TCP data from a remote node (or outgoing socket) */
            else {
                
                recive_message_from_remote(fd, epollfd);

            }
        }
    }
    return NULL;
}



void setup_epoll(int port) {
    /* Ignore SIGPIPE: if a peer closes while we are writing, send()
     * returns -1 with errno=EPIPE instead of killing the process.  */
    signal(SIGPIPE, SIG_IGN);


    /* Create the shared epoll instance */
    epollfd = epoll_create1(0);
    if (epollfd < 0) fatal_error("epoll_create1 failed");

    /* Open and bind the three network endpoints */
    initialize_listen_sockets(port);

    /* Register listening sockets in epoll.
     * EPOLLEXCLUSIVE: only ONE thread is woken up per incoming event.
     * This prevents the "thundering herd" problem without a global lock. */
    struct epoll_event ev_s, ev_e, ev_u;

    ev_s.events  = EPOLLIN | EPOLLEXCLUSIVE;
    ev_s.data.fd = socket_server;

    ev_e.events  = EPOLLIN | EPOLLEXCLUSIVE;
    ev_e.data.fd = socket_erlang;

    ev_u.events  = EPOLLIN | EPOLLEXCLUSIVE;   
    ev_u.data.fd = socket_UDP;

    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, socket_server, &ev_s) < 0 ||
        epoll_ctl(epollfd, EPOLL_CTL_ADD, socket_erlang, &ev_e) < 0 ||
        epoll_ctl(epollfd, EPOLL_CTL_ADD, socket_UDP,    &ev_u) < 0)
        fatal_error("epoll_ctl ADD listening sockets failed");

    /*
        Create TWO timerfd instances:
        1. broadcast_timer -> sends ANNOUNCE via UDP every 5 s (first fire: 1 s)
        2. timeout_timer   -> checks expired jobs every 5 s   (first fire: 5 s)
     
         Both timers are shared across all worker threads. Because epoll delivers
        a timer event to exactly one thread at a time, no extra locking is needed
        for the timer read itself (check_job_timeouts uses its own mutex internally).
    */
    static worker_args_t args;  /* static so it outlives this stack frame */
    args.broadcast_timer_fd = make_timer(1, 5, epollfd);
    args.timeout_timer_fd   = make_timer(5, 5, epollfd);
    args.port = port;

    /* Spawn the threads */
    pthread_t threads[NUM_WORKERS];
    for (int i = 0; i < NUM_WORKERS; i++) {
        if (pthread_create(&threads[i], NULL, event_loop, &args) != 0)
            fatal_error("pthread_create failed");
        printf("[INIT] Worker thread %d spawned\n", i);
    }

    /* Main thread blocks here until all workers finish (effectively forever) */
    for (int i = 0; i < NUM_WORKERS; i++) {
        pthread_join(threads[i], NULL);
    }

    /* Cleanup (unreachable in normal operation) */
    close(args.broadcast_timer_fd);
    close(args.timeout_timer_fd);
    close(socket_server);
    close(socket_erlang);
    close(socket_UDP);
    close(epollfd);
}
