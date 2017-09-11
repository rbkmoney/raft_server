%%%
%%% Протокол обмена сообщениями между raft серверами.
%%% Не подразумевает гарантию доставки и очерёдности сообщений.
%%% Общается с raft_server через сообщение вида {raft_server_rpc, Data}
%%%
-module(raft_server_rpc).

%% API
-export_type([rpc         /0]).
-export_type([endpoint    /0]).
-export_type([message     /0]).
-export_type([message_type/0]).
-export_type([message_body/0]).
-export([send/3]).
-export([recv/2]).

%%
%% API
%%
-type rpc() :: mg_utils:mod_ops().
-type endpoint() :: term().
-type message() :: {
    request | {response, _Success::boolean()},
    _Type       ::message_type(),
    _Body       ::message_body(),
    _CurrentTerm,
    _From       ::endpoint()
}.
-type message_type() ::
      request_vote
    | append_entries
.
-type message_body() :: term().

%%

-callback send(_Args, endpoint(), message()) ->
    ok.

-callback recv(_Args, _Data) ->
    message().

%%

-spec send(mg_utils:mod_opts(), endpoint(), message()) ->
    ok.
send(RPC, To, Message) ->
    mg_utils:apply_mod_opts(RPC, send, [To, Message]).

-spec recv(mg_utils:mod_opts(), _) ->
    message().
recv(RPC, Info) ->
    mg_utils:apply_mod_opts(RPC, recv, [Info]).
