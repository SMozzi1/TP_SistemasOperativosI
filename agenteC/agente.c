#define _GNU_SOURCE

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

#include "comunicaciones.h"
#include "agente.h"
#include <signal.h>


/* Inventario de recursos locales disponibles */
int cpu_available = 4;
int mem_available = 8192;
int gpu_available = 1;

/* Mutex para proteger el inventario local de condiciones de carrera */
pthread_mutex_t mutex_resources = PTHREAD_MUTEX_INITIALIZER;

active_jobs table_nodes;
active_jobs table_clients;



#define MAX_EVENTS 64        // Maximum number of events epoll will process in a single wake-up
#define PORT       4200      // TCP Port for both Erlang (localhost) and Remote Nodes (Any IP)
#define BUFFER_LEN 1024      // Standard buffer size for reading network data
#define NUM_WORKERS 4        // Number of threads in our Thread Pool
#define MAX_FDS    1024      // Maximum file descriptors supported by our read_until_newline function
#define JOB_TIMEOUT_SEC 30

#define BROADCAST_PORT 12529 
//Need manualy be changed
#define ANNOUNCEMENT_MSG "ANNOUNCE 4200 cpu:4 mem:8192 gpu:1"


int socket_server;
int socket_erlang;
int socket_UDP;
int epollfd;
int erlangfd;


//Logging helpers 

static void log_error(const char *msg)   { perror(msg); }
static void fatal_error(const char *msg) { perror(msg); exit(EXIT_FAILURE); }


//Socket initialization
static void initialize_listen_sockets(void) {
    struct sockaddr_in server_addr, erlang_addr, udp_addr;
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
    server_addr.sin_port        = htons(PORT);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    /*
     * socket_erlang: accepts connections from localhost ONLY (127.0.0.1).
     * Both TCP sockets can share PORT because their bind addresses differ
     * (0.0.0.0 vs 127.0.0.1).
     */
    memset(&erlang_addr, 0, sizeof(erlang_addr));
    erlang_addr.sin_family      = AF_INET;
    erlang_addr.sin_port        = htons(PORT);
    erlang_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    /* socket_UDP: receives broadcasts on BROADCAST_PORT */
    memset(&udp_addr, 0, sizeof(udp_addr));
    udp_addr.sin_family      = AF_INET;
    udp_addr.sin_port        = htons(BROADCAST_PORT);
    udp_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(socket_server, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0 ||
        bind(socket_erlang, (struct sockaddr *)&erlang_addr, sizeof(erlang_addr)) < 0 ||
        bind(socket_UDP,    (struct sockaddr *)&udp_addr,    sizeof(udp_addr))    < 0)
        fatal_error("bind() failed");

    if (listen(socket_server, 10) < 0 || listen(socket_erlang, 10) < 0)
        fatal_error("listen() failed");

    printf("[INIT] Listening on TCP 0.0.0.0:%d (nodes), 127.0.0.1:%d (Erlang), UDP %d (broadcast)\n",
           PORT, PORT, BROADCAST_PORT);
}

//Timers
/*
 * Creates a periodic timerfd and registers it in epoll.
 * initial_sec:  seconds until the first expiration.
 * interval_sec: repeat period in seconds.
 */

    //Event loop (executed by every threads)



void *event_loop(void *arg) {
    worker_args_t     *args   = (worker_args_t *)arg;
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
                int new_fd = accept4(socket_erlang, NULL, NULL, SOCK_NONBLOCK);
                if (new_fd < 0) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK)
                        log_error("Erlang accept4");

                    continue;
                }

                struct epoll_event ev;
                ev.events  = EPOLLIN;
                ev.data.fd = new_fd;

                if (epoll_ctl(epollfd, EPOLL_CTL_ADD, new_fd, &ev) < 0) {
                    log_error("epoll_ctl ADD erlang conn");
                    close(new_fd);
                    continue;
                }

                /*
                 * Store the fd globally.
                 * NOTE: if multiple simultaneous Erlang connections were possible
                 * (unlikely by design), this would need a mutex-protected array.
                 * For this assignment a single erlangfd is sufficient.
                 */
                erlangfd = new_fd;
                printf("[EVENT A] Erlang connected on fd=%d\n", erlangfd);
            }

            /* ── B: new connection from a remote node ───────────────── */
            else if (fd == socket_server) {
                struct sockaddr_in client_addr;
                socklen_t len = sizeof(client_addr);
                int client_fd = accept4(socket_server,
                                        (struct sockaddr *)&client_addr,
                                        &len, SOCK_NONBLOCK);
                if (client_fd < 0) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK)
                        log_error("Remote accept4");
                    continue;
                }

                /*
                 * EPOLLONESHOT: ensures only ONE thread processes this fd
                 * per event round, preventing race conditions without an
                 * additional per-fd mutex.
                 */
                struct epoll_event ev;
                ev.events  = EPOLLIN | EPOLLONESHOT;
                ev.data.fd = client_fd;

                if (epoll_ctl(epollfd, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
                    log_error("epoll_ctl ADD remote client");
                    close(client_fd);
                }

                printf("[EVENT B] Remote node connected: %s fd=%d\n",
                       inet_ntoa(client_addr.sin_addr), client_fd);
            }

            /* ── C: UDP broadcast timer fired ───────────────────────── */
            else if (fd == args->broadcast_timer_fd) {
                uint64_t exp;
                /* Must read the timer fd to clear its readable state;
                 * otherwise epoll keeps waking us up in a busy loop.  */
                if (read(fd, &exp, sizeof(exp)) < 0) {
                    log_error("read broadcast timer");
                }

                /*
                 * Format: ANNOUNCE <port> <resources>  (image 2)
                 * The sender IP is NOT in the payload; it is extracted
                 * from recvfrom() by the receiving node.
                 */
                struct sockaddr_in bcast_addr;
                memset(&bcast_addr, 0, sizeof(bcast_addr));
                bcast_addr.sin_family      = AF_INET;
                bcast_addr.sin_port        = htons(BROADCAST_PORT);
                bcast_addr.sin_addr.s_addr = htonl(INADDR_BROADCAST);

                ssize_t sent = sendto(socket_UDP, ANNOUNCEMENT_MSG,
                                      strlen(ANNOUNCEMENT_MSG), 0,
                                      (struct sockaddr *)&bcast_addr,
                                      sizeof(bcast_addr));
                if (sent < 0) log_error("[UDP BCAST] sendto failed");
                else printf("[UDP BCAST] Announcement sent: %s\n", ANNOUNCEMENT_MSG);
            }

            /* ── D: job timeout timer fired ─────────────────────────── */
            else if (fd == args->timeout_timer_fd) {
                uint64_t exp;
                if (read(fd, &exp, sizeof(exp)) < 0) {
                    log_error("read timeout timer");
                }
                check_job_timeouts(erlangfd, table_nodes);
                check_job_timeouts(erlangfd, table_clients);
            }

            /* ── E: incoming UDP datagram from another node ─────────── */
            else if (fd == socket_UDP) {
                char buf[BUFFER_LEN];
                struct sockaddr_in sender;
                socklen_t slen = sizeof(sender);

                int bytes = recvfrom(socket_UDP, buf, sizeof(buf) - 1, 0,
                                     (struct sockaddr *)&sender, &slen);
                if (bytes <= 0) continue;

                buf[bytes] = '\0';

                /*
                 * The sender's IP comes from the sockaddr_in, NOT from the
                 * payload (spec change, image 2).
                 */
                char *sender_ip = inet_ntoa(sender.sin_addr);

                //printf("[UDP RECV] ANNOUNCE from %s: %s\n", sender_ip, buf);

                /*
                 * TODO (node management teammate):
                 *   Parse "ANNOUNCE <port> <resources>" from buf,
                 *   then update the active node table with:
                 *     - IP        = sender_ip
                 *     - port, resources = parsed from buf
                 *     - timestamp = time(NULL)
                 *   Protect with a mutex if the table is shared across threads.
                 */
            }

            /* ── F: incoming TCP data from Erlang ───────────────────── */
            else if (fd == erlangfd) {
                char line[BUFFER_LEN];
                int result = read_until_newline(fd, line);

                if (result == 1) {
                    printf("[ERLANG ->] %s", line);
                    erlang_to_C(erlangfd, line, args->timeout_timer_fd);
                } else if (result == -1) {
                    log_error("[EVENT F] Erlang disconnected");
                    epoll_ctl(epollfd, EPOLL_CTL_DEL, erlangfd, NULL);
                    close(erlangfd);
                    clear_connection_buffer(erlangfd);
                    erlangfd = -1;
                }
                /* result == 0: incomplete message, epoll will notify us again */
            }

            /* ── G: Send message from a job requesting*/
            if (events[i].events & EPOLLOUT) {
                int fd_listo = events[i].data.fd;
                
                // Aquí buscas qué Job y qué Recurso son dueños de este FD
                job_entry* job = BuscarJobPorFD(fd_listo); // Debes implementar esta lógica de búsqueda
                
                if (job != NULL && job->next_req != NULL) {
                    char msg[256];
                    snprintf(msg, sizeof(msg), "RESERVE %d %s %d\n", 
                            job->job_id, job->next_req->type, job->next_req->amount);
                            
                    // ¡Aquí enviamos el mensaje por fin!
                    send(fd_listo, msg, strlen(msg), MSG_NOSIGNAL);
                    
                    // Modificamos epoll para quitar EPOLLOUT, ahora solo queremos LEER (EPOLLIN) la respuesta
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
                    client_to_myserver(erlangfd, fd, line);

                } else if (result == -1) {
                    fprintf(stderr, "[EVENT G] Remote node fd=%d disconnected\n", fd);

                    /*
                     * Release all resources this node held (table_clients).
                     * TODO (resource management teammate): iterate jobs for this
                     * fd and call release_resource() for each one.
                     */
                    epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, NULL);
                    close(fd);
                    clear_connection_buffer(fd);
                    continue; /* Do not rearm: the socket is dead */
                }

                /*
                 * MANDATORY EPOLLONESHOT REARM.
                 * After EPOLLONESHOT fires, the kernel disables monitoring for
                 * this fd. We must re-enable it explicitly with EPOLL_CTL_MOD.
                 * Rearm for result == 0 (waiting for more data) and result == 1
                 * (ready for the next_job message). Skip only for result == -1 (dead).
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





void setup_epoll(void) {
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
    initialize_listen_sockets();

    /* Register listening sockets in epoll.
     * EPOLLEXCLUSIVE: only ONE thread is woken up per incoming event.
     * This prevents the "thundering herd" problem without a global lock. */
    struct epoll_event ev_s, ev_e, ev_u;

    ev_s.events  = EPOLLIN | EPOLLEXCLUSIVE;
    ev_s.data.fd = socket_server;

    ev_e.events  = EPOLLIN | EPOLLEXCLUSIVE;
    ev_e.data.fd = socket_erlang;

    ev_u.events  = EPOLLIN;   /* UDP does not need EPOLLEXCLUSIVE */
    ev_u.data.fd = socket_UDP;

    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, socket_server, &ev_s) < 0 ||
        epoll_ctl(epollfd, EPOLL_CTL_ADD, socket_erlang, &ev_e) < 0 ||
        epoll_ctl(epollfd, EPOLL_CTL_ADD, socket_UDP,    &ev_u) < 0)
        fatal_error("epoll_ctl ADD listening sockets failed");

    /*
     * Create TWO timerfd instances:
     *  1. broadcast_timer -> sends ANNOUNCE via UDP every 5 s (first fire: 1 s)
     *  2. timeout_timer   -> checks expired jobs every 5 s   (first fire: 5 s)
     *
     * Both timers are shared across all worker threads. Because epoll delivers
     * a timer event to exactly one thread at a time, no extra locking is needed
     * for the timer read itself (check_job_timeouts uses its own mutex internally).
     */
    static worker_args_t args;  /* static so it outlives this stack frame */
    args.broadcast_timer_fd = make_timer(1, 5);
    args.timeout_timer_fd   = make_timer(5, 5);

    // we initialize the tables
    //tabla de trabajos propios
    JobsTableInit(&table_nodes);

    //Tabla de trabajos de Nodos Remotos
    JobsTableInit(&table_clients);

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

//Place holder funcion que tiene 3 tablas hash 
int initialize_connections()
{
    

}