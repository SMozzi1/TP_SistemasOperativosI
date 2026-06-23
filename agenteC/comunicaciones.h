#ifndef COMUNICACIONES_H
#define COMUNICACIONES_H

//Los includes estan en utils.h

#include "globals.h"
#include "utils.h"
#include "../ResourceManager/job_table.h"

//MAX_FDS debe ser igual o mayor al número máximo de file descriptors que tu proceso puede tener abiertos simultáneamente
//MAX_FDS must be equal or greater to the max number of file descriptors that your process
//possibly haves open simultaneously.
#define MAX_FDS 1024


#define LENG 1024
// Max capacity for the stream reconstruction buffer
#define BUFFER_MAX 2048

/*
    Manages the partial data accumulation for non-blocking stream sockets.
    buffer: Local storage for incoming data chunks before a newline is detected.
    accumulated_bytes: Current number of bytes stored in the buffer.
*/
typedef struct text_buffer{
    char buffer[512];
    int accumulated_bytes;
} ConnectionState;


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


/*
    Initiates a resource request workflow for a given job.
    Traverses the job's resource list and prepares the necessary networking 
    requests to peer nodes.
*/
void ask_for_next_resource(job_entry* job);



/*
    Processes incoming TCP messages from remote nodes and dispatches them.
    Identifies whether the message is a request (RESERVE/RELEASE) or a response 
    (GRANTED/DENIED) and invokes the corresponding business logic.
*/
void client_to_myserver(int fd_actual, char *instruction);

/*
    Sends a formatted response back to the Erlang scheduler safely using MSG_NOSIGNAL.
    instruction: The command or status type (e.g., "GRANTED").
    job_id: The unique identifier of the job being processed.
 */
void C_to_erlang(const char *instruction, const char *job_id);



/*
    Parses instructions received from the Erlang scheduler and triggers local actions.
    instruction: The raw string command sent by the Erlang process.
    timefd: The current timestamp to be associated with the triggered action.
 */
void erlang_to_C(char* instruction, time_t timefd);









#endif /* NETWORK_UTILS_H */