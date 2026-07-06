
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
#include "comunicaciones.h"
#include "agente.h"

#include <arpa/inet.h>


// Inventory of available local resources.
int cpu_available = 4;
int mem_available = 8192;
int gpu_available = 1;

int epollfd = -1;
int erlangfd = -1;
active_jobs table_ourjobs;

/* Mutex we use to prevent race condition */
pthread_mutex_t mutex_resources = PTHREAD_MUTEX_INITIALIZER;

active_jobs table_nodes;
active_jobs table_clients;



#define MAX_EVENTS 64        // Maximum number of events epoll will process in a single wake-up

#define BUFFER_LEN 1024      // Standard buffer size for reading network data
#define NUM_WORKERS 4        // Number of threads in our Thread Pool
#define MAX_FDS    1024      // Maximum file descriptors supported by our read_until_newline function

#define BROADCAST_PORT 12529 
//Need manualy be changed

int JOB_TIMEOUT_SEC = 30;
int NODE_TIMEOUT_SEC = 15;

int socket_server;
int socket_erlang;
int socket_UDP;


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
                if (new_fd < 0) {
                    // Error already logged in connect_erlang()
                    continue;
                }
                erlangfd = new_fd;
                  
            }

            /* ── B: new connection from a remote node ───────────────── */
            else if (fd == socket_server) {
                
                conecct_client(fd, epollfd);
    
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
                check_job_timeouts(&table_nodes, NODE_TIMEOUT_SEC);
                check_job_timeouts(&table_ourjobs, JOB_TIMEOUT_SEC);
            }

            /* ── E: incoming UDP datagram from another node ─────────── */
            //We get the message ANNOUNCE and keep the Node in table_nodes
            else if (fd == socket_UDP) {
                
                char buf[BUFFER_LEN];
                struct sockaddr_in sender;
                socklen_t slen = sizeof(sender);

                int bytes = recvfrom(socket_UDP, buf, sizeof(buf) - 1, 0,
                                    (struct sockaddr *)&sender, &slen);
                if (bytes <= 0) continue;

                buf[bytes] = '\0'; // to end when copying
                char copy[LENG];
                strncpy(copy, buf, sizeof(copy) - 1);
                copy[sizeof(copy) - 1] = '\0';

                char *tokens[10];
                int num = get_token(copy, tokens, 10);
                if (num < 2) continue; // At least we need ANNOUNCE and the port

                
                //We get the ip 
                char sender_ip[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, &(sender.sin_addr), sender_ip, sizeof(sender_ip));
                

                int nodo_puerto = atoi(tokens[1]);
                
                /* Converts the sender's IP address (string) into a 32-bit integer identifier.
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
                    
                    // CORRECTION 2: We use 'strtol_r' (reentrant) which is thread-safe.
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


                /*We only insert it into the table if it's a node that didn't previously exist.
                (If it already existed, we directly modify its resources using the pointer provided by FindJob)*/
                if (is_new) {
                    JobsTableInsert(&table_nodes, nodo);
                    printf("[UDP RECEIVE] ¡Nuevo nodo descubierto en la red!\n");
                } else {
                    printf("[UDP RECEIVE] Latido recibido. Nodo ya estaba en la tabla.\n");
                }
                
            }

            /* ── F: incoming TCP data from Erlang ───────────────────── */
            /// Read messages arriving from Erlang
            else if (fd == erlangfd) {
                char line[BUFFER_LEN];
                int result = read_until_newline(fd, line);

                if (result == 1) {
                    printf("[ERLANG ->] %s", line);
                    erlang_to_C(line, time(NULL));
                } else if (result == -1) {
                    log_error("[EVENT F] Erlang disconnected");
                    epoll_ctl(epollfd, EPOLL_CTL_DEL, erlangfd, NULL);
                    close(erlangfd);
                    clear_connection_buffer(erlangfd);
                    erlangfd = -1;
                }
              
            }

            /* ── G: Send message from a job requesting - Request job*/
            // If a socket is flagged with EPOLLOUT, it indicates that the underlying 
            // network buffer is ready to transmit data; we use this event to trigger 
            // the dispatch of a pending resource request.
            else if (events[i].events & EPOLLOUT) {
                //
                int fd_listo = events[i].data.fd;
                
                // We find out which Job and which Resource own this FD
                job_entry* job = BuscarJobPorFD(fd_listo); 
                
                if (job != NULL && job->next_req != NULL) {
                    char msg[256];
                    snprintf(msg, sizeof(msg), "RESERVE %d %s %d\n", 
                            job->job_id, job->next_req->type, job->next_req->amount);
                            
                 
                    send(fd_listo, msg, strlen(msg), MSG_NOSIGNAL);
                    
                    /* Rearm the epoll registration to switch from EPOLLOUT (writing) to EPOLLIN (reading).
                    This ensures we stop monitoring for write-ready states and begin waiting for 
                    the peer's response to our request. We use EPOLLET (Edge-Triggered) and 
                    EPOLLONESHOT for high-performance, single-thread-per-event delivery.
                    */
                    struct epoll_event ev;
                    ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
                    ev.data.fd = fd_listo;
                    epoll_ctl(epollfd, EPOLL_CTL_MOD, fd_listo, &ev);
                }
            }

            /* ── H: incoming TCP data from a remote node (or outgoing socket) */
            else {
                char line[BUFFER_LEN];
                int result = read_until_newline(fd, line);

                if (result == 1) {
                    printf("[REMOTE ->] fd=%d: %s", fd, line);
                    /*
                     * Dispatch based on content:
                     *  - RESERVE/RELEASE -> remote node requests or frees a resource
                     *  - GRANTED/DENIED  -> reply to a RESERVE we sent
                     */
                    client_to_myserver(fd, line);

                } else if (result == -1) {
                    fprintf(stderr, "[EVENT G] Remote node fd=%d disconnected\n", fd);
                    release_client_by_fd(fd);          // release what you had reserved for this fd
                    handle_outbound_disconnect(fd);    // in case we call ourselves
                    epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, NULL); //then deleate from epoll
                    close(fd);
                    clear_connection_buffer(fd);
                    continue;
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
        }
    }
    return NULL;
}



void setup_epoll(int port) {
    /* Ignore SIGPIPE: if a peer closes while we are writing, send()
     * returns -1 with errno=EPIPE instead of killing the process.  */
    signal(SIGPIPE, SIG_IGN);

    // /* Initialize per-fd buffers and both job tables */
    // memset(connections, 0, sizeof(connections));
    // init_jobs_table(&table_nodes);
    // init_jobs_table(&table_clients);

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
    args.broadcast_timer_fd = make_timer(1, 5);
    args.timeout_timer_fd   = make_timer(5, 5);
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
