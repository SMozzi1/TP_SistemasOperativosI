#include <stdio.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/socket.h>

#define MAX_EVENTS 10
#define PUERTO 4200

int inicializar_sockets_escucha(int * socket_escucha, int * socket_erlang)
{
  int socketEfd, socketERLfd;
  //se = socket escucha, serl = socket erlang
  struct sockaddr_in se, serl;
  socketEfd = socket(AF_INET, SOCK_STREAM, 0);
  socketERLfd = socket(AF_INET, SOCK_STREAM, 0);

  if(socketEfd < 0 || socketERLfd < 0)
    //handle_error();
    quit("socket");

    //
  if (setsockopt(socketEfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) < 0 ||
   	  setsockopt(socketERLfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) < 0)
			quit("setsockopt");

  se.sin_family = AF_INET;
  se.sin_port = htons(PUERTO);
  se.sin_addr.s_addr = htonl(INADDR_ANY);  //INADDR_LOOPBACK equivale a la direccion 127.0.0.1

  serl.sin_family = AF_INET;
  serl.sin_port = htons(PUERTO);
  serl.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

  if(bind(socketEfd, (struct sockaddr *)&se, sizeof se) < 0 || bind(socketERLfd, (struct sockaddr *)&serl, sizeof serl) < 0)
    //handle_error(q);
    quit("bind");
	//La segunda variable del listen define la maxima cantidad de clientes pueden esperar por conectarse
  if(listen(socketEfd, 10) < 0 || listen(socketERLfd, 10) < 0)
    quit("listen");
  *(socket_escucha) = socketEfd;
  *(socket_erlang) = socketERLfd;

}


void crear_epoll()
{
	struct epoll_event ev_escucha, ev_erl, events[MAX_EVENTS];
  int epollfd;
  epollfd = epoll_create();
   
  if(epollfd < 0)
    exit(EXIT_FAILURE);

  epoll_event.events = EPOLLIN;
  epoll_event.data.fd = listen_sock;

  int socket_escucha, socket_erlang;
  inicializar_sockets_escucha(&socket_escucha, &socket_erlang);

  if(epoll_ctl(epollfd, EPOLL_CTL_ADD, socket_escucha, &ev_escucha) < 0 ||
     epoll_ctl(epollfd, EPOLL_CTL_ADD, socket_erlang, &ev_erl) < 0 )
    exit(EXIT_FAILURE);
  
	//aceptar_eventos();    
}

int main()
{

	retutn 0;
}