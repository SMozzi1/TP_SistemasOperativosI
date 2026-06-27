-module(scheduler_utils).

-export([msg_to_job/3, write_on_log/2]).


-export([parse_nodes/1,parse_node/1]).

-export([job_request_format/2,resource_to_string/1, append_if_not_zero/2]).

-export([pick_resource/2,make_resources_for_job/1]).

%% COORDINATOR AUX FUNCTIONS:

%% @doc Function to modularize the sending of state messages to the jobs
%% @spec maps_to_job(map(), list(), atomic()) -> ok().
msg_to_job(Jobs_Map, Rest, Msg) ->
    JobId = list_to_integer(string:trim(Rest)),
    PidJob = maps:get(JobId, Jobs_Map),
    PidJob ! Msg .


%% @doc Writes on the log that the received state was sent into the jobId
%% @spec write_on_log(atomic(),integer()) -> ok().
write_on_log(started, Port) ->
    Line = io_lib:format("[~p] --- COORDINATOR STARTED on port ~p --- ~n~n", [calendar:local_time(), Port]),
    file:write_file("scheduler.log", Line, [append]);
write_on_log(State, JobId) ->
    Line = io_lib:format("[~p] JOB ~p ~p~n~n", [calendar:local_time(), JobId, State]),
    file:write_file("scheduler.log", Line, [append]).


%% -------------------------------

%% RESOURCE MESSAGE CREATION FUNCTIONS:


%% @doc Builds the "@IP:type:amount" string for one resource
%% @spec resource_to_string({string(), integer(), atom(), integer()}) -> string()
resource_to_string({IP, _Port, Type, Quantity}) ->
    "@" ++ IP ++ ":" ++ atom_to_list(Type) ++ ":" ++ integer_to_list(Quantity).


%% @doc Builds the full JOB_REQUEST string for the C agent
%% @spec format_job_request(integer(), list()) -> string()
job_request_format(JobId, Resources) ->
    [Cpu, Mem, Gpu] = Resources,
    Base = "JOB_REQUEST " ++ integer_to_list(JobId),
    Base1 = append_if_not_zero(Base, Cpu),
    Base2 = append_if_not_zero(Base1, Mem),
    Base3 = append_if_not_zero(Base2, Gpu),
    Base3 ++ "\n".

append_if_not_zero(Base, Resource) ->
    case Resource of
        {_IP, _Port, _Type, 0} -> Base;
        _ -> Base ++ " " ++ resource_to_string(Resource)
    end.

%% -------------------------------

%% GET_NODES -> LIST OF NODE AUX FUNCTIONS:


%% @doc Parses the full nodes string from C into a list of {IP, Port, Resources}
%% @spec parse_nodes(string()) -> list()
%parse_nodes(Data) ->
%    NodeStrings = string:split(Data, ";", all),
%    lists:map(fun parse_node/1, NodeStrings).
parse_nodes(Data) ->
    case string:trim(Data) of
        ""    -> [];
        Clean ->
            NodeStrings = string:split(Clean, ";", all),
            lists:map(fun parse_node/1, NodeStrings)
    end.

%% @doc Parses a single node string into {IP, Port, Resources}
%% @spec parse_node(string()) -> {string(), integer(), list()}
parse_node(NodeStr) ->
    Clean = string:trim(NodeStr), %% cleans the "/n" at the end
    [IP, PortStr, "cpu", CpuStr, "mem", MemStr, "gpu", GpuStr] = string:split(Clean, ":", all),
    Port = list_to_integer(PortStr),
    %% Note: the nodes includes their resources in the following order:
    %%          cpu > mem > gpu
    %% And we respect it at the time to send messages to the C agent :)
    Resources = [{cpu, list_to_integer(CpuStr)}, 
                {mem, list_to_integer(MemStr)}, 
                {gpu, list_to_integer(GpuStr)}],
    {IP, Port, Resources}.

%% -------------------------------

%% PICK RESOURCES FROM NODES LISTS AUX FUNCTIONS:

%% @doc Grabs each tuple created for each resource on pick_resource() and puts it on a tuple
%% @spec make_resources_for_job(list()) -> list().
make_resources_for_job(Data) ->
    CpuSol = pick_resource(cpu, Data),
    MemSol = pick_resource(mem, Data),
    GpuSol = pick_resource(gpu, Data),
    [CpuSol,MemSol,GpuSol].


%% @doc Picks a random node and a random amount of a given resource type
%% @spec pick_resource(atom(), list()) -> {string(), integer(), atom(), integer()}
pick_resource(Type, Nodes) ->
    {IP, Port, Resources} = lists:nth(rand:uniform(length(Nodes)), Nodes),
    {Type, TypeStorage} = lists:keyfind(Type, 1, Resources),
    %% We pick from 0 to TypeStr (max capacity)
    {IP, Port, Type, rand:uniform(TypeStorage + 1) - 1}.
    
%% -------------------------------
