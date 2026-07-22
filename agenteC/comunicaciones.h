#ifndef COMUNICACIONES_H
#define COMUNICACIONES_H

//Los includes estan en utils.h

#include "globals.h"
#include "utils.h"




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