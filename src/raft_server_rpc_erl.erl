-module(raft_server_rpc_erl).

%% raft_server_rpc
-behaviour(raft_server_rpc).
-export([init/1, send/3, recv/2]).

-type state() :: _.
-type endpoint() :: mg_utils:gen_ref().

-spec init(_Args) ->
    state().
init(_) ->
    undefined.

%% this is a copy of gen_server:cast
-spec send(endpoint(), raft_server_rpc:message(), state()) ->
    state().
send(To, Message, _) ->
    FullMessage = {?MODULE, Message},
    ok = case To of
            {global, GlobalName} ->
                catch global:send(GlobalName, FullMessage);
            {via, Mod, Name} ->
                catch Mod:send(Name, FullMessage);
            LocalName ->
                % noconnect тут зачем?
                case catch erlang:send(LocalName, FullMessage, [noconnect]) of
                    noconnect -> spawn(erlang, send, [To, FullMessage]);
                    Other     -> Other
                end
        end.

-spec recv(term(), state()) ->
    {raft_server_rpc:message(), state()} | invalid_data.
recv({?MODULE, Message}, _) ->
    {Message, undefined};
recv(_, _) ->
    invalid_data.
