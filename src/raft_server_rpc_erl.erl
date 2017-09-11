-module(raft_server_rpc_erl).

%% raft_server_rpc
-behaviour(raft_server_rpc).
-export([send/3, recv/2]).

-type endpoint() :: mg_utils:gen_ref().

%% this is a copy of gen_server:cast
-spec send(_, endpoint(), raft_server_rpc:message()) ->
    ok.
send(_, To, Message) ->
    FullMessage = {raft_server_rpc, Message},
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

-spec recv(_, term()) ->
    raft_server_rpc:message().
recv(_, Message) ->
    Message.
