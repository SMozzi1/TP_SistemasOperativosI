-module(scheduler).

-export([start/1,coordinator/1]).

-export([job_generator/2, coordinator_loop/3]).

-export([msg_to_job/3]).



%% @doc Starts the scheduler agent. Receives the port as an argument
%% @spec start(integer()) -> ok.
start(Port) ->
    spawn(?MODULE, coordinator, [Port]).


%% @doc Connects to the C local host, creates the job generator and starts coordinator_loop
%% @spec coordinator(integer()) -> no_return()
coordinator(Port) ->
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port, [
        %%-active -> llegan los mensajes automaticamente al buzon
        %%           util para recibir msgs de tcp como {tcp, Socket, Data}
        {active, true},
        %% -packet , line -> el msg es hasta el \n
        {packet, line}
    ]),

    %% Starts the job_generator with the first JobId and the coordinator Pid
    spawn(?MODULE, job_generator, [1001, self()]),

    %% From here on, goes on a loop of receiving/sending messages from different parts
    coordinator_loop(Socket, #{}, undefined).


%% @doc Loops indefinetly managing messages between the C server and different functions from the client
%%      Keeps the Socket, as well as a Map to store the current jobs and GenPid for the "GET_NODES" section.
%% @spec coordinator_loop(integer(), map(), integer()) -> no_return()
coordinator_loop(Socket,Jobs_Map,GenPid) ->
    receive 

        %% JOB INIT/END MESSAGES

        %% a job has to be registered in the map and sent it to the C agent
        {register_and_request, JobId, PidJob, Resources} ->
            gen_tcp:send(Socket, Resources),
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
            msg_to_job(Jobs_Map, Rest, granted),
            coordinator_loop(Socket, Jobs_Map, GenPid);

        {tcp, Socket, "JOB_DENIED " ++ Rest} ->
            msg_to_job(Jobs_Map, Rest, denied),
            coordinator_loop(Socket, Jobs_Map, GenPid);

        {tcp, Socket, "JOB_TIMEOUT " ++ Rest} ->
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
            lists:foreach(fun(PidJob) -> PidJob ! cancelled end, maps:values(Jobs_Map));

        {tcp_error, Socket, _Reason} ->
            lists:foreach(fun(PidJob) -> PidJob ! cancelled end, maps:values(Jobs_Map))
        
    end.

%% @doc Function to modularize the sending of state messages to the jobs
%% @spec maps_to_job(map(), list(), atomic()) 
msg_to_job(Jobs_Map, Rest, Msg) ->
    JobId = list_to_integer(string:trim(Rest)),
    PidJob = maps:get(JobId, Jobs_Map),
    PidJob ! Msg .


%% @doc Builds the "@IP:type:amount" string for one resource
%% @spec resource_to_string({string(), integer(), atom(), integer()}) -> string()
resource_to_string({IP, _Port, Tipo, Cantidad}) ->
    "@" ++ IP ++ ":" ++ atom_to_list(Tipo) ++ ":" ++ integer_to_list(Cantidad).

%% @doc Builds the full JOB_REQUEST string for the C agent
%% @spec format_job_request(integer(), list()) -> string()
job_request_format(JobId, Recursos) ->
    [Cpu, Mem, Gpu] = Recursos,
    CpuString = resource_to_string(Cpu),
    MemString = resource_to_string(Mem),
    GpuString = resource_to_string(Gpu),
    "JOB_REQUEST " ++ integer_to_list(JobId) ++ " " ++ CpuString ++ " " ++ MemString ++ " " ++ GpuString ++ "\n".




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
            Time = rand:uniform(20),
            timer:sleep(Time),
            job(CoordPid, JobId, Resources)
    end.





job_generator(JobId, CoordPid) ->
    CoordPid ! {get_nodes, self()},
    receive
        {nodes, Data} -> 
            Parsed_Data = parse_nodes(Data),
            Resources = make_resources(Parsed_Data),
            spawn(?MODULE, job, [CoordPid, JobId, Resources]),

            timer:sleep(5000),
            job_generator(JobId + 1 , CoordPid)
    end.




%% @doc Parses the full nodes string from C into a list of {IP, Port, Resources}
%% @spec parse_nodes(string()) -> list()
parse_nodes(Data) ->
    NodeStrings = string:split(Data, ";", all),
    lists:map(fun parse_node/1, NodeStrings).


%% @doc Parses a single node string into {IP, Port, Resources}
%% @spec parse_node(string()) -> {string(), integer(), list()}
parse_node(NodeStr) ->
    [IP, PortStr, "cpu", CpuStr, "mem", MemStr, "gpu", GpuStr] = string:split(NodeStr, ":", all),
    Port = list_to_integer(PortStr),
    %% Note: while the nodes includes their resources in the following order:
    %%          cpu > mem > gpu
    %% We decide to make each node as cpu > gpu > mem to follow a lexigraphic order
    Resources = [{cpu, list_to_integer(CpuStr)}, 
                {gpu, list_to_integer(GpuStr)}, 
                {mem, list_to_integer(MemStr)}],
    {IP, Port, Resources}.



make_resources(Data) ->
    CpuSol = pick_resource(cpu, Data),
    GpuSol = pick_resource(gpu, Data),
    MemSol = pick_resource(mem, Data),
    [CpuSol,GpuSol,MemSol].

%% @doc Picks a random node and a random amount of a given resource type
%% @spec pick_resource(atom(), list()) -> {string(), integer(), atom(), integer()}
pick_resource(Type, Nodes) ->
    {IP, Port, Resources} = lists:nth(rand:uniform(length(Nodes)), Nodes),
    {Type, TypeStr} = lists:keyfind(Type, 1, Resources),
    {IP, Port, Type, rand:uniform(TypeStr)}.
    




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