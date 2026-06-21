#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include <time.h>
#include <string.h>
#include "job_table.h"

int main(){

active_jobs table;
    JobsTableInit(&table);

    // -------Insert -------------------------------------
    job_entry *j1 = MakeJob(1001, 5, time(NULL));
    AddResource(j1, MakeGranted("cpu", 2));
    AddResource(j1, MakeGranted("mem", 4096));
    JobsTableInsert(&table, j1);

    job_entry *j2 = MakeJob(2045, 6, time(NULL));
    AddResource(j2, MakeGranted("gpu", 1));
    JobsTableInsert(&table, j2);

    // force colission
    job_entry *j3 = MakeJob(1001 + TABLE_SIZE, 7, time(NULL));
    AddResource(j3, MakeGranted("cpu", 1));
    JobsTableInsert(&table, j3);

    printf("### after 3 inserts ###\n");
    PrintTable(&table);

    // --------Find -------------------------------------------
    printf("### find 1001 ###\n");
    job_entry *found = FindJob(&table, 1001);
    
    if (found)
        printf("found: job_id= %d socket= %d\n",
               found->job_id, found->origin_socket);

    printf("### find 9999 (not on the table) ###\n");
    FindJob(&table, 9999);

    // -------Remove----------------------------------------------
    printf("### remove 1001 ###\n");
    RemoveJob(&table, 1001);
    PrintTable(&table);

    printf("### remove 2045 ###\n");
    RemoveJob(&table, 2045);
    PrintTable(&table);
    // ------Destroy ---------------------------------------------
    printf("### destroy tabla ###\n");
    DestroyJobsTable(&table);
    printf("active_count: %d\n", table.active_count);

return 0;}
