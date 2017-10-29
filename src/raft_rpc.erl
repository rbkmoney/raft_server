%%%
%%% Copyright 2017 RBKmoney
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

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
-export([send              /4]).
-export([recv              /2]).
-export([get_nearest       /2]).
-export([get_reply_endpoint/1]).
-export([format_endpoint   /1]).
-export([format_message    /1]).

%%
%% API
%%
-type rpc() :: raft_utils:mod_opts().
-type endpoint() :: term().
-type request_id() :: term(). % mb integer?
-type message() ::
      {internal, internal_message()}
    | {external, ID::request_id(), external_message()}
.

-type external_message_type() ::
      command
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

-callback get_reply_endpoint(_) ->
    endpoint().

%%

-spec send(raft_utils:mod_opts(), endpoint(), endpoint(), message()) ->
    ok.
send(RPC, From, To, Message) ->
    raft_utils:apply_mod_opts(RPC, send, [From, To, Message]).

-spec recv(raft_utils:mod_opts(), _) ->
    message().
recv(RPC, Data) ->
    raft_utils:apply_mod_opts(RPC, recv, [Data]).

-spec get_nearest(raft_utils:mod_opts(), [endpoint()]) ->
    endpoint().
get_nearest(RPC, Endpoints) ->
    raft_utils:apply_mod_opts(RPC, get_nearest, [Endpoints]).

-spec get_reply_endpoint(raft_utils:mod_opts()) ->
    endpoint().
get_reply_endpoint(RPC) ->
    raft_utils:apply_mod_opts(RPC, get_reply_endpoint, []).

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
format_ext_type(command                ) -> "cmd" ;
format_ext_type(async_command          ) -> "ascmd";
format_ext_type(response_command       ) -> "rcmd" .
