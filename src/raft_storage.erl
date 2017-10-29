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
%%% Интерфейс для хранилища стейта.
%%%
-module(raft_storage).

-export_type([type   /0]).
-export_type([key    /0]).
-export_type([value  /0]).
-export_type([state  /0]).
-export_type([storage/0]).
-export([init   /2]).
-export([put    /3]).
-export([get    /3]).
-export([get_one/3]).
-export([remove /3]).

-type type   () :: system | log | handler | cmd_id.
-type key    () :: term().
-type value  () :: term().
-type state  () :: term().
-type storage() :: raft_utils:mod_opts().

%%

-callback init(_Opts, type()) ->
    state().

-callback put(_, [{key(), value()}], state()) ->
    state().

-callback get(_, [key()], state()) ->
    [value()].

-callback get_one(_, key(), state()) ->
    value() | undefined.

-callback remove(_, [key()], state()) ->
    state().

%%

-spec init(storage(), type()) ->
    state().
init(Storage, Type) ->
    raft_utils:apply_mod_opts(Storage, init, [Type]).

-spec put(storage(), [{key(), value()}], state()) ->
    state().
put(Storage, Values, State) ->
    raft_utils:apply_mod_opts(Storage, put, [Values, State]).

-spec get(storage(), [key()], state()) ->
    [value()].
get(Storage, Keys, State) ->
    raft_utils:apply_mod_opts(Storage, get, [Keys, State]).

-spec get_one(storage(), key(), state()) ->
    value().
get_one(Storage, Key, State) ->
    raft_utils:apply_mod_opts(Storage, get_one, [Key, State]).

-spec remove(storage(), [key()], state()) ->
    state().
remove(Storage, Keys, State) ->
    raft_utils:apply_mod_opts(Storage, remove, [Keys, State]).
