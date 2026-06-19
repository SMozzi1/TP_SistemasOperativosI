-module(scheduler).

-export([start/1,coordinator/1]).

-export([job_generator/2, coordinator_loop/3]).

-export([msg_a_job/3]).

start(Port) ->
    spawn(?MODULE, coordinator, [Port]).

coordinator(Port) ->
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port, [
        {active, true},
        {packet, line}
    ]),

    spawn(?MODULE, job_generator, [1001, self()]),
    coordinator_loop(Socket, #{}, undefined).

coordinator_loop(Socket,Jobs_Map,Gen_pid) ->
    receive 
        {register_and_request, JobId, PidJob, Recursos} ->
            Msg = formatear_job_request(JobId, Recursos),
            gen_tcp:send(Socket, Msg),
            NewJobsMap = maps:put(JobId, PidJob, Jobs_Map),
            coordinator_loop(Socket, NewJobsMap, GenPid);

        {finished, JobId} ->
            gen_tcp:send(Socket, "JOB_RELEASE " ++ integer_to_list(JobId) ++ "\n"),
            NewJobsMap = maps:remove(JobId, Jobs_Map),
            coordinator_loop(Socket, NewJobsMap, GenPid);
        
        {tcp, Socket, "JOB_GRANTED " ++ Rest} ->
            msg_a_job(Jobs_Map, Rest, granted),
            loop(Socket, Jobs_Map, GenPid);

        {tcp, Socket, "JOB_DENIED " ++ Rest} ->
            msg_a_job(Jobs_Map, Rest, denied),
            loop(Socket, Jobs_Map, GenPid);

        {tcp, Socket, "JOB_TIMEOUT " ++ Rest} ->
            msg_a_job(Jobs_Map, Rest, timeout),
            loop(Socket, Jobs_Map, GenPid);


        {get_nodes, Pid} ->
            gen_tcp:send(Socket, "GET_NODES\n"),
            loop(Socket, Jobs_Map, Pid);

        {tcp, Socket, "NODES " ++ Rest} ->
            GenPid ! {nodes, Rest},
            loop(Socket, Jobs_Map, undefined);




msg_a_job(Jobs_Map, Rest, Msg) ->
    JobId = list_to_integer(string:trim(Rest)),
    PidJob = maps:get(JobId, Jobs_Map),
    PidJob ! Msg .