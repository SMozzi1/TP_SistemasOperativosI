-module(scheduler).

-export([start/1,coordinator/1]).

-export([job_generator/2, coordinator_loop/3]).

-export([msg_to_job/3]).



%% @doc Starts the scheduler agent. Receives the port as an argument
%% @spec start(int()) -> ok.
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
        {register_and_request, JobId, PidJob, Recursos} ->

            Msg =format_job_request(JobId, Recursos),
            gen_tcp:send(Socket, Msg),
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


format_job_request(JobId,Recursos) -> todo.

job_generator(CurrentPid,CoordPid) -> todo.


%% TODO: job/3
%%   - Recibir {register_and_request, JobId, self(), Recursos} al coordinator
%%   - Hacer receive de: granted | denied | timeout | cancelled
%%   - Si granted: timer:sleep() simulando trabajo, luego {finished, JobId} al coordinator
%%   - Si denied: morir
%%   - Si timeout: ? (a acordar)
%%   - Si cancelled: morir (C se desconecto)

%% TODO: job_generator/2
%%   - Mandar {get_nodes, self()} al coordinator
%%   - Esperar {nodes, Data} del coordinator
%%   - Llamar parsear_nodos(Data)
%%   - Llamar armar_recursos(Nodos) para construir lista de recursos
%%   - spawn job con JobId N y recursos armados
%%   - timer:sleep(5000)
%%   - Llamar recursivamente con N+1

%% TODO: parsear_nodos/1
%%   - Splitear string por ";" para separar nodos
%%   - Para cada nodo splitear por ":" para obtener IP, Puerto, recursos
%%   - Devolver lista de {IP, Puerto, #{cpu => X, mem => Y, gpu => Z}}
%%   - Acordar formato exacto con companiero de C

%% TODO: armar_recursos/1
%%   - Recibir lista de nodos parseados
%%   - Para cada recurso elegir un nodo al azar de la lista
%%   - Pedir cantidad random entre 1 y el maximo disponible del nodo
%%   - Devolver lista ORDENADA: cpu < mem < gpu (evito deadlock)

%%NOTA DE DEADLOCK
%% Se deberia implementar otra estrategia en caso que otro equipo
%% no respete este orden (quiza con wait-die)

%% TODO:format_job_request/2
%%   - Armar string "JOB_REQUEST <job_id> @ip:port:res:amount ..."
%%   - Acordar formato exacto con companiero de C

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
%%   - Validar ordenamiento global cpu < mem < gpu
%%     con el escenario de deadlock del TP (seccion 6)
%%   - Probar con nc -l antes de tener el agente C listo