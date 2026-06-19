#define _GNU_SOURCE //This is needed to use accept4
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>


#define MAX_EVENTS 10
#define PUERTO 4200

//No me convence este handler, tengo que cammbiarlo
//este handler termina todo el programa si hay error lo cual no deberia ser 
void handler(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}


void inicializar_sockets_escucha(int * socket_escucha, int * socket_erlang, int *socket_UDP)
{
  int socketEfd, socketERLfd, socketUDP;
  
  struct sockaddr_in se, serl, sudp;

  //We set the nonblocking mode by adding SOCK_NONBLOCK to the second argument to socket()
  socketEfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
  socketERLfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
  socketUDP = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);

  
  if(socketEfd < 0 || socketERLfd < 0 || socketUDP < 0)
    handler("SOCKET");

//Manipulate  options for the socket referred to by the file descriptor sockfd.
//To manipulate options at the sockets API level, level is specified as SOL_SOCKET.
//SO_REUSEADDR  is a socket option that allows a network application to forcibly bind to an IP
//address and port combination that is currently stuck in the TIME_WAIT state.
//Que es yes 
  int yes = 1;
  if (setsockopt(socketEfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) < 0 ||
   	  setsockopt(socketERLfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) < 0|| 
      setsockopt(socketUDP, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) < 0)
			handler("SETSOCKOPT");

  sudp.sin_family = AF_INET;
  //function htonl converts the unsigned integer hostlong from host byte order to network byte order.
  sudp.sin_port = htonl(PUERTO);
  //INADDR_ANY listen from every ip
  sudp.sin_addr.s_addr = htonl(INADDR_ANY);

  se.sin_family = AF_INET;
  se.sin_port = htons(PUERTO);
  se.sin_addr.s_addr = htonl(INADDR_ANY);  
  
  serl.sin_family = AF_INET;
  serl.sin_port = htons(PUERTO + 1);
  //INADDR_LOOPBACK is the same as the direccion 127.0.0.1
  serl.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

  if(bind(socketEfd, (struct sockaddr *)&se, sizeof se) < 0 || 
     bind(socketERLfd, (struct sockaddr *)&serl, sizeof serl) < 0 ||
     bind(socketUDP, (struct sockaddr *)&sudp, sizeof sudp))
    handler("BIND");
    

	//The backlog argument defines the maximum length to which the queue of pending connections for sockfd may grow.
  if(listen(socketEfd, 10) < 0 || listen(socketERLfd, 10) < 0)
    handler("LISTEN");

  *(socket_escucha) = socketEfd;
  *(socket_erlang) = socketERLfd;
  *(socket_UDP) = socketUDP;

}







//agregar caso udp actualizar lista de eelementos


//capaz puedo no pasarle el socket_escucha y spcket_erlang si las seteo como globales 
void aceptar_eventos(int epollfd, struct epoll_event events[MAX_EVENTS], int socket_escucha, int socket_erlang)
{
  //We declare this variable to know when the message comes form erlang
  int erlangfd;
  //We start the loop 
  while(1)
  {
    int nfds;
    //The last argument specifies the number of milliseconds that epoll_wait()  will  block.
    nfds = epoll_wait(epollfd, events, MAX_EVENTS, -1);
    if(nfds < 0){
      handler("EPOLL_WAIT");
    }
    
    for(int i = 0; i < nfds; i++) 
    {

      //We add the c
      if(events[i].data.fd == socket_erlang)
      {
        
        erlangfd = accept(socket_erlang, NULL, NULL);
          if (erlangfd < 0)
              handler("ACCEPT4 ERLANG");
          //If it is possible to connect, we add the fd to the epoll
          struct epoll_event ev_erl;
          ev_erl.events = EPOLLIN;
          ev_erl.data.fd = erlangfd;

          if (epoll_ctl(epollfd, EPOLL_CTL_ADD, erlangfd, &ev_erl) < 0)
              handler("EPOLL_CTL ERLANG_FD");
      }


      //If there is a new client, we add it to the epoll. 
      if(events[i].data.fd == socket_escucha)
      {
        int client_sock;
        struct sockaddr_in cl;


        struct epoll_event ev_client;
        socklen_t cllen = sizeof(cl);
        //We use accept4 to set the O_NONBLOCK flag 
        client_sock = accept4(socket_escucha, (struct sockaddr *)&cl, &cllen, SOCK_NONBLOCK);
        
        if(client_sock < 0) 
          handler("ACCEPT4");
        //Epollout becase i want to send messages
        //Chat me dice de eliminar el epollout hasta que quiera mandar el mensaje(ver)
        ev_client.events = EPOLLIN;
        ev_client.data.fd = client_sock;
        if(epoll_ctl(epollfd, EPOLL_CTL_ADD, client_sock, &ev_client) < 0)
          handler("EPOLL_CTL");
      }
      //If erlang sends us a message
      if(events[i].data.fd == erlangfd)
      {
        char buff[1024];
        
        int bytes = read(erlangfd, buff, sizeof(buff) - 1);

        if(bytes > 0) {
          buff[bytes] = '\0';
          
          //Print de testeo
          printf("[ERLANG] Comandos entrantes: %s", buff);

          //Desglosar mensajes 
          //ejecutar mensajes (trabajo de estructural)

          //
        }

      }
      //We recive messages from others Nodes
      else {
        char buff[1024];
        int bytes = read(events[i].data.fd, buff, sizeof(buff) - 1);

        if(bytes > 0) {
          buff[bytes] = '\0';
          // Acá metés tu bucle while(strtok_r...) para picalear los mensajes pesados
          printf("[NODOS] Comando entrante: %s\n", buff);

          //Desglosar mensajes 
          //ejecutar mensajes (trabajo de estructural)
        
        }
      }
    }
  }
  
}





void crear_epoll()
{
	struct epoll_event ev_escucha, ev_erl, ev_udp, events[MAX_EVENTS];
  int epollfd;
  epollfd = epoll_create1(0);
   
  if(epollfd < 0)
    exit(EXIT_FAILURE);
    
  int socket_escucha, socket_erlang, socket_UDP;
  inicializar_sockets_escucha(&socket_escucha, &socket_erlang, &socket_UDP);

  ev_escucha.events = EPOLLIN;
  ev_escucha.data.fd = socket_escucha;

  ev_erl.events = EPOLLIN;
  ev_erl.data.fd = socket_erlang;

  ev_udp.events = EPOLLIN;
  ev_udp.data.fd = socket_UDP;

  if(epoll_ctl(epollfd, EPOLL_CTL_ADD, socket_escucha, &ev_escucha) < 0 ||
     epoll_ctl(epollfd, EPOLL_CTL_ADD, socket_erlang, &ev_erl) < 0 ||
     epoll_ctl(epollfd, EPOLL_CTL_ADD, socket_UDP, &ev_udp) < 0)
    handler("EPOLL_CTL");
  
	aceptar_eventos(epollfd, events , socket_escucha, socket_erlang);    
}


int main()
{
  crear_epoll();
	return 0;
}