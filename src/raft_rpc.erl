%%%
%%% Протокол обмена сообщениями между raft серверами.
%%% Не подразумевает гарантию доставки и очерёдности сообщений.
%%% Общается с raft через сообщение вида {raft_rpc, Data}
%%%
-module(raft_rpc).

%% API
-export_type([rpc                  /0]).
-export_type([endpoint             /0]).
-export_type([message              /0]).
-export_type([external_message     /0]).
-export_type([internal_message     /0]).
-export_type([external_message_type/0]).
-export_type([internal_message_type/0]).
-export_type([message_body         /0]).
-export([send           /3]).
-export([recv           /2]).
-export([get_nearest    /2]).
-export([self           /1]).

%%
%% API
%%
-type rpc() :: mg_utils:mod_opts().
-type endpoint() :: term().
-type message() ::
      {internal, internal_message()}
    | {external, external_message()}
.

-type external_message_type() ::
      sync_command
    | async_command
    | response_command
.
-type external_message() :: {
    _Type :: external_message_type(),
    _Body :: message_body()
}.

-type internal_message() :: {
    request | {response, _Success::boolean()},
    _Type       ::internal_message_type(),
    _Body       ::message_body(),
    _From       ::endpoint(),
    _CurrentTerm
}.
-type internal_message_type() ::
      request_vote
    | append_entries
.

-type message_body() :: term().

%%

-callback send(_, endpoint(), message()) ->
    ok.

-callback recv(_, _Data) ->
    message().

-callback get_nearest(_, [endpoint()]) ->
    endpoint().

-callback self(_) ->
    endpoint().

%%

-spec send(mg_utils:mod_opts(), endpoint(), message()) ->
    ok.
send(RPC, To, Message) ->
    mg_utils:apply_mod_opts(RPC, send, [To, Message]).

-spec recv(mg_utils:mod_opts(), _) ->
    message().
recv(RPC, Info) ->
    mg_utils:apply_mod_opts(RPC, recv, [Info]).

-spec get_nearest(mg_utils:mod_opts(), [endpoint()]) ->
    endpoint().
get_nearest(RPC, Endpoints) ->
    mg_utils:apply_mod_opts(RPC, get_nearest, [Endpoints]).

-spec self(mg_utils:mod_opts()) ->
    endpoint().
self(RPC) ->
    mg_utils:apply_mod_opts(RPC, self, []).
