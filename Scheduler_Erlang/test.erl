-module(test).
-export([start/1]).

%% test - Fake C Agent for local testing
%%
%% Simulates the behavior of the C Resource Manager Agent (agente.c)
%% for testing the Erlang scheduler without needing the real C agent.
%%
%% Response probabilities per JOB_REQUEST:
%%   - 60% JOB_GRANTED
%%   - 20% JOB_DENIED
%%   - 20% JOB_TIMEOUT
%%
%% HOW TO RUN:
%%   1. Compile:
%%      erlc test.erl scheduler_utils.erl scheduler.erl
%%
%%   2. Start test agent (terminal 1):
%%      erl -noshell -eval "test:start(4200), timer:sleep(infinity)."
%%
%%   3. Start scheduler (terminal 2):
%%      erl -noshell -eval "scheduler:start(), timer:sleep(60000), halt()."
%%
%%   4. Check logs in scheduler.log

start(Port) ->
    {ok, ListenSocket} = gen_tcp:listen(Port, [
        {active, true},
        {packet, line},
        {reuseaddr, true}
    ]),
    io:format("[FAKE_C] Listening on port ~p~n", [Port]),
    accept_loop(ListenSocket).

accept_loop(ListenSocket) ->
    {ok, Socket} = gen_tcp:accept(ListenSocket),
    io:format("[FAKE_C] Erlang scheduler connected~n"),
    Handler = spawn(fun() -> handle_loop(Socket) end),
    gen_tcp:controlling_process(Socket, Handler),
    accept_loop(ListenSocket).

handle_loop(Socket) ->
    receive
        {tcp, Socket, "GET_NODES\n"} ->
            io:format("[FAKE_C] Received GET_NODES~n"),
            Nodes = "192.168.1.10:8100:cpu:4:mem:8192:gpu:1;192.168.1.11:8101:cpu:2:mem:4096:gpu:1\n",
            gen_tcp:send(Socket, "NODES " ++ Nodes),
            handle_loop(Socket);

        {tcp, Socket, "JOB_REQUEST " ++ Rest} ->
            io:format("[FAKE_C] Received JOB_REQUEST: ~p~n", [Rest]),
            [JobIdStr | _] = string:split(string:trim(Rest), " ", all),
            timer:sleep(300),
            case rand:uniform(5) of
                4 ->
                    io:format("[FAKE_C] Responding JOB_DENIED ~s~n", [JobIdStr]),
                    gen_tcp:send(Socket, "JOB_DENIED " ++ JobIdStr ++ "\n");
                5 ->
                    io:format("[FAKE_C] Responding JOB_TIMEOUT ~s~n", [JobIdStr]),
                    gen_tcp:send(Socket, "JOB_TIMEOUT " ++ JobIdStr ++ "\n");
                _ ->
                    io:format("[FAKE_C] Responding JOB_GRANTED ~s~n", [JobIdStr]),
                    gen_tcp:send(Socket, "JOB_GRANTED " ++ JobIdStr ++ "\n")
            end,
            handle_loop(Socket);

        {tcp, Socket, "JOB_RELEASE " ++ Rest} ->
            io:format("[FAKE_C] Received JOB_RELEASE: ~p~n", [Rest]),
            handle_loop(Socket);

        {tcp, Socket, "JOB_STATUS " ++ Rest} ->
            io:format("[FAKE_C] Received JOB_STATUS: ~p~n", [Rest]),
            JobIdStr = string:trim(Rest),
            gen_tcp:send(Socket, "WAITING " ++ JobIdStr ++ "\n"),
            handle_loop(Socket);

        {tcp, Socket, "GET_NODES\n"} ->
            handle_loop(Socket);

        {tcp_closed, Socket} ->
            io:format("[FAKE_C] Connection closed~n");

        {tcp_error, Socket, Reason} ->
            io:format("[FAKE_C] Error: ~p~n", [Reason]);

        Other ->
            io:format("[FAKE_C] Unknown message: ~p~n", [Other]),
            handle_loop(Socket)
    end.