-module(raft_server_rpc_erl).

%% raft_server_rpc
-behaviour(raft_server_rpc).
-export([init/1, send/3, recv/2]).

-type state() :: undefined.
-type endpoint() :: mg_utils:gen_ref().

-spec init(_Args) ->
    state().
init(_) ->
    undefined.

%% this is a copy of gen_server:cast
-spec send(endpoint(), raft_server_rpc:message(), state()) ->
    state().
send(To, Message, State) ->
    FullMessage = {'$raft_rpc', Message},
    ok = case To of
            {global, Name} ->
                catch global:send(Name, FullMessage);
            {via, Mod, Name} ->
                catch Mod:send(Name, FullMessage);
            _ ->
                case catch erlang:send(To, FullMessage, [noconnect]) of
                    noconnect -> spawn(erlang, send, [To, FullMessage]);
                    Other     -> Other
                end
        end,
    State.

-spec recv(term(), state()) ->
    {raft_server_rpc:message(), state()} | invalid_data.
recv({'$raft_rpc', Message}, State) ->
    {Message, State};
recv(_, _State) ->
    invalid_data.
