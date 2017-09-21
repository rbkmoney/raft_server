%%%
%%% Протокол обмена сообщениями между raft серверами.
%%% Не подразумевает гарантию доставки и очерёдности сообщений.
%%% Общается с raft через сообщение вида {raft_rpc, Data}
%%%
-module(raft_rpc).

%% API
-export_type([rpc                  /0]).
-export_type([endpoint             /0]).
-export_type([request_id           /0]).
-export_type([message              /0]).
-export_type([external_message     /0]).
-export_type([internal_message     /0]).
-export_type([external_message_type/0]).
-export_type([internal_message_type/0]).
-export_type([message_body         /0]).
-export([send           /4]).
-export([recv           /2]).
-export([get_nearest    /2]).
-export([self           /1]).
-export([format_endpoint/1]).
-export([format_message /1]).

%%
%% API
%%
-type rpc() :: mg_utils:mod_opts().
-type endpoint() :: term().
-type request_id() :: term(). % mb integer?
-type message() ::
      {internal, internal_message()}
    | {external, ID::request_id(), external_message()}
.

-type external_message_type() ::
      sync_command
    | async_command
    | response_command
.
-type external_message() :: {
    external_message_type(),
    message_body()
}.

-type internal_message() :: {
    internal_direction(),
    internal_message_type(),
    message_body(),
    _CurrentTerm
}.
-type internal_direction() :: request | response.
-type internal_message_type() ::
      request_vote
    | append_entries
.

-type message_body() :: term().

%%

-callback send(_, endpoint(), endpoint(), message()) ->
    ok.

-callback recv(_, _Data) ->
    message().

-callback get_nearest(_, [endpoint()]) ->
    endpoint().

-callback self(_) ->
    endpoint().

%%

-spec send(mg_utils:mod_opts(), endpoint(), endpoint(), message()) ->
    ok.
send(RPC, From, To, Message) ->
    mg_utils:apply_mod_opts(RPC, send, [From, To, Message]).

-spec recv(mg_utils:mod_opts(), _) ->
    message().
recv(RPC, Data) ->
    mg_utils:apply_mod_opts(RPC, recv, [Data]).

-spec get_nearest(mg_utils:mod_opts(), [endpoint()]) ->
    endpoint().
get_nearest(RPC, Endpoints) ->
    mg_utils:apply_mod_opts(RPC, get_nearest, [Endpoints]).

-spec self(mg_utils:mod_opts()) ->
    endpoint().
self(RPC) ->
    mg_utils:apply_mod_opts(RPC, self, []).

%% TODO callback
-spec format_endpoint(endpoint()) ->
    list().
format_endpoint(Endpoint) ->
    io_lib:format("~9999p", [Endpoint]).

-spec format_message(message()) ->
    list().
format_message({internal, {Direction, Type, Body, Term}}) ->
    io_lib:format(
        "int:~s:~s:~9999p:~p",
        [format_int_direction(Direction), format_int_type(Type), Body, Term]
    );
format_message({external, ID, {Type, Body}}) ->
    io_lib:format("ext:~p:~s:~9999p", [ID, format_ext_type(Type), Body]).

-spec
format_int_direction(internal_direction()) -> list().
format_int_direction(request             ) -> "req";
format_int_direction(response            ) -> "resp".

-spec
format_int_type(internal_message_type()) -> list().
format_int_type(request_vote           ) -> "request_vote";
format_int_type(append_entries         ) -> "append_entries".

-spec
format_ext_type(external_message_type()) -> list().
format_ext_type(sync_command           ) -> "scmd" ;
format_ext_type(async_command          ) -> "ascmd";
format_ext_type(response_command       ) -> "rcmd" .
