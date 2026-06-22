#ifndef COMUNICACIONES_H
#define COMUNICACIONES_H

//Los includes estan en utils.h

#include "globals.h"
#include "utils.h"
#include "../ResourceManager/job_table.h"

//MAX_FDS debe ser igual o mayor al número máximo de file descriptors que tu proceso puede tener abiertos simultáneamente
#define MAX_FDS 1024


#define LENG 1024
// Max capacity for the stream reconstruction buffer
#define BUFFER_MAX 2048

//Es el buffer acumulador por socket. Lo necesitás porque TCP no garantiza que un 
//mensaje llegue completo en un solo recv()
typedef struct text_buffer{
    char buffer[512];
    int accumulated_bytes;
} ConnectionState;


/*Funciones para parsear y recibir mensajes de otros*/
int get_token(char *instruction, char **token_array, int max_tokens);   
void clear_connection_buffer(int fd);
/*
 *  Reads from a non-blocking socket piece by piece until a newline character is found.
 *  fd The socket file descriptor to read from (e.g., erlangfd).
 *  output_line Buffer where the complete line will be copied once fully assembled.
 *  1 if the line is complete, 0 if more data is needed, -1 on error/disconnection.
 */
int read_until_newline(int fd, char* output_line);



void ask_for_next_resource(job_entry* job);



/*Funciones para comunicarnos con los fd y con erlang*/
void client_to_myserver(int fd_actual, char *instruction);


/**
  Sends a formatted response back to Erlang safely using MSG_NOSIGNAL.
  The socket connected to the Erlang node.
  estado The status string (e.g., "granted", "rejected", "waiting").
  job_id The identifier of the job being processed.
 */
void C_to_erlang(const char *instruction, const char *job_id);



/**
 *  Parses instructions coming from Erlang and triggers the appropriate action.
 *  erlangfd The socket connected to the Erlang node.
 *  instruction The raw null-terminated string received from the socket.
 */
void erlang_to_C(char* instruction, time_t timefd);








#endif /* NETWORK_UTILS_H */