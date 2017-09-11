%%%
%%% Как работает фрагментация?
%%% Адоптация?
%%% SAC?
%%%
%%%
-module(raft_server_rpc_sctp).

%% API
-export_type([endpoint/0]).
-export_type([peer    /0]).
-export([start_link/2]).

%% raft_server_rpc
-behaviour(raft_server_rpc).
-export([send/3, recv/2]).

%%
%% API
%%
-spec start_link(mg_utils:gen_reg_name(), raft_server_rpc_sctp_dispatcher:options()) ->
    mg_utils:gen_start_ret().
start_link(RegName, Options) ->
    raft_server_rpc_sctp_dispatcher:start_link(RegName, Options).

%%
%% raft_server_rpc
%%
-type endpoint() :: raft_server_rpc_sctp_dispatcher:endpoint().
-type peer    () :: raft_server_rpc_sctp_dispatcher:peer().

-spec send(mg_utils:gen_ref(), endpoint(), raft_server_rpc:message()) ->
    ok.
send(DispatcherRef, To, Message) ->
    ok = raft_server_rpc_sctp_dispatcher:send(DispatcherRef, To, Message).

-spec recv(mg_utils:gen_ref(), term()) ->
    raft_server_rpc:message().
recv(_DispatcherRef, Message) ->
    Message.
