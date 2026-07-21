#ifndef __INSTRUCTIONS_H__
#define __INSTRUCTIONS_H__

#include "globals.h"
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>

#define MAX_FDS 1024 

/*
    Manages the partial data accumulation for non-blocking stream sockets.
    buffer: Local storage for incoming data chunks before a newline is detected.
    accumulated_bytes: Current number of bytes stored in the buffer.
*/
typedef struct text_buffer{
    char buffer[BUFFER_LEN];
    int accumulated_bytes;
    pthread_mutex_t connlock;  // Mutex to protect access to this buffer in multi-threaded contexts
} Connection;


/*
    Initializes the connection buffers and their associated mutexes for all file descriptors.
*/
void initialize_connection_buffers();

/*
    Splits a string into an array of tokens based on whitespace.
    Returns the number of tokens successfully parsed.
 */
int get_token(char *instruction, char **token_array, int max_tokens);   

/*
    Resets the internal state buffer associated with a specific file descriptor.
    Should be called upon client disconnection to prevent data leakage or 
    leftover artifacts in future connections using the same fd.
*/
void clear_connection_buffer(int fd);


/*
    Reads from a non-blocking socket piece by piece until a newline character is found.
    fd: The socket file descriptor to read from.
    output_line: Buffer where the complete line will be copied once fully assembled.
    Returns 1 if the line is complete, 0 if more data is needed, -1 on error/disconnection.
 */
int read_until_newline(int fd, char* output_line);







#endif /* __INSTRUCTIONS_H__ */