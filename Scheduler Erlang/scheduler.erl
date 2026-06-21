
-module(scheduler).
-import(scheduler_utils, [msg_to_job/3, write_on_log/2, parse_nodes/1, parse_node/1, make_resources_for_job/1, pick_resource/2, job_request_format/2]).

-export([start/0,coordinator/0]).


-export([job_generator/2, coordinator_loop/3]).


-export([job/3]).

%% TIme it takes the generator to create the next job.
-define(GENERATOR_LOOP_TIME, 5000).

%% Time at max of a job for relaunching itself
-define(JOB_TIMEOUT_MAX_RELAUNCH, 20).

%% Used port.
-define(PORT, 4200).


%% @doc Starts the scheduler agent.
%% @spec start() -> ok.
start() ->
    spawn(?MODULE, coordinator, []).


%% @doc Connects to the C local host, creates the job generator and starts coordinator_loop
%% @spec coordinator() -> no_return()
coordinator() ->
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, ?PORT, [
        %%-active -> no need to use tcp recv for the C messages. they reach as normal mgs.
        {active, true},
        %% -packet , line -> the msg ends with "\n" included
        {packet, line}
    ]),

    %% Starts the job_generator with the first JobId and the coordinator Pid
    spawn(?MODULE, job_generator, [1001, self()]),

    write_on_log(started, ?PORT),   %% Marks the start of a run.

    %% From here on, goes on a loop of receiving/sending messages from different parts
    coordinator_loop(Socket, #{}, undefined).


%% @doc Loops indefinetly managing messages between the C server and different functions from the client
%%      Keeps the Socket, as well as a Map to store the current jobs and GenPid for the "GET_NODES" section.
%% @spec coordinator_loop(integer(), map(), integer()) -> no_return()
coordinator_loop(Socket,Jobs_Map,GenPid) ->
    receive 

        %% JOB INIT/END MESSAGES

        %% a job has to be registered in the map and sent it to the C agent
        {register_and_request, JobId, PidJob, Message} ->
            gen_tcp:send(Socket, Message),
            NewJobsMap = maps:put(JobId, PidJob, Jobs_Map),
            coordinator_loop(Socket, NewJobsMap, GenPid);

        %% a job has finished its work time, thus should be deleted from the map
        {finished, JobId} ->
            gen_tcp:send(Socket, "JOB_RELEASE " ++ integer_to_list(JobId) ++ "\n"),
            NewJobsMap = maps:remove(JobId, Jobs_Map),
            coordinator_loop(Socket, NewJobsMap, GenPid);

        %% -----------------

        %% TCP MESSAGES OF JOBS REQUEST STATE

        %% Note that either GRANTED, DENINED and TIMEOUT do the same thing
        %% just changing the atom sent to msg_to_job()

        {tcp, Socket, "JOB_GRANTED " ++ Rest} ->
            write_on_log(granted,list_to_integer(string:trim(Rest))),
            msg_to_job(Jobs_Map, Rest, granted),
            coordinator_loop(Socket, Jobs_Map, GenPid);

        {tcp, Socket, "JOB_DENIED " ++ Rest} ->
            write_on_log(denied,list_to_integer(string:trim(Rest))),
            msg_to_job(Jobs_Map, Rest, denied),
            coordinator_loop(Socket, Jobs_Map, GenPid);

        {tcp, Socket, "JOB_TIMEOUT " ++ Rest} ->
            write_on_log(timeout,list_to_integer(string:trim(Rest))),
            msg_to_job(Jobs_Map, Rest, timeout),
            coordinator_loop(Socket, Jobs_Map, GenPid);

        %% -----------------
        
        %% GET_NODES MESSAGES

        %% These two messages works with the GenPid. If the messages is
        %% about asking for GET_NODES, it saves the pid where it comics.
        %% Otherwise, you use GenPid and sent the nodes to that pid. 

        {get_nodes, Pid} ->
            gen_tcp:send(Socket, "GET_NODES\n"),
            coordinator_loop(Socket, Jobs_Map, Pid);

        {tcp, Socket, "NODES " ++ Rest} ->
            GenPid ! {nodes, Rest},
            coordinator_loop(Socket, Jobs_Map, undefined);

        %% -----------------

        %% CLOSE/ERROR OF C SERVER MESSAGES

        %% In either case, we visit each Job from the Jobs_Map to sent
        %% them a message, in order to them to cancell their operations.

        %% Note that it doesnt call
        {tcp_closed, Socket} ->
            write_on_log(connection_closed, "all_jobs_cancelled"),
            lists:foreach(fun(PidJob) -> PidJob ! cancelled end, maps:values(Jobs_Map));

        {tcp_error, Socket, _Reason} ->
            write_on_log(connection_closed, "all_jobs_cancelled"),
            lists:foreach(fun(PidJob) -> PidJob ! cancelled end, maps:values(Jobs_Map))
        
    end.





%% @doc Ask for resources and, if granted, simulates work .
%% @spec job(integer(), integer(), list()) -> ok.
job(CoordPid , JobId , Resources) ->
    Message = job_request_format(JobId,Resources),
    CoordPid ! {register_and_request , JobId, self(), Message},
    receive
        granted ->
            %% Simulates work
            Time = rand:uniform(20),
            timer:sleep(Time),
            CoordPid ! {finished, JobId};
        denied ->
            %% Job unnacepted -> dies
            ok;
        timeout ->
            %% Timeout -> relaunches job after some time
            Time = rand:uniform(?JOB_TIMEOUT_MAX_RELAUNCH),
            timer:sleep(Time),
            job(CoordPid, JobId, Resources);
        cancelled ->
            %% C agent died or had an error -> dies
            ok
    end.



%% @doc Ask for nodes information to C, and creates a job with a resources solitude, after that, it loops into itself
%% @spec job_generator(integer(),integer()) -> no_return().
job_generator(JobId, CoordPid) ->
    CoordPid ! {get_nodes, self()},
    receive
        {nodes, Data} -> 
            %% First, it parsed the Data from the nodes received
            Parsed_Data = parse_nodes(Data),

            Resources = make_resources_for_job(Parsed_Data),
            spawn(?MODULE, job, [CoordPid, JobId, Resources]),

            timer:sleep(?GENERATOR_LOOP_TIME),

            %% After this, it recalls itself, with a increase in the JobId
            job_generator(JobId + 1 , CoordPid)
    end.








%%NOTA DE DEADLOCK
%% Se deberia implementar otra estrategia en caso que otro equipo
%% no respete este orden (quiza con wait-die)



%% TODO: logging
%%   - Registrar en archivo .log cada granted, denied, timeout
%%   - Formato: "[timestamp] JOB <id> GRANTED/DENIED/TIMEOUT"
%%   - Usar file:write_file o io:format

%% DISEÑO PENDIENTE:
%%   - Acordar con mozzi:
%%       * Puerto de conexion local Erlang <-> C
%%       * Formato exacto de JOB_REQUEST
%%       * Formato exacto de NODES (separadores, orden de campos)
%%   - Decidir comportamiento ante JOB_TIMEOUT:
%%       * El job reintenta o muere?
%%       * El coordinator limpia el map ante timeout?
%%   - Decidir estrategia de armar_recursos:
%%       * Pide a un solo nodo o a varios?
%%       * Cuantos recursos pide por job?
%%   - Validar ordenamiento global cpu < gpu < mem
%%     con el escenario de deadlock del TP (seccion 6)
%%   - Probar con nc -l antes de tener el agente C listo

