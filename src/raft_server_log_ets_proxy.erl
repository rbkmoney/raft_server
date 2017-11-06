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
%%% Прокси хранилище, которе не привязывается к процессу, а хранит в ets таблице
%%%
-module(raft_server_log_ets_proxy).

%% API
-export([create_table/1]).

%% raft_server_log callbacks
-behaviour(raft_server_log).
-export([init/1, indexes/2, entry/3, entries/4, append/4, commit/3]).

-type options() :: #{
    log        := raft_server_log:log(),
    id         := _,
    table_name := atom()
}.

-type state() :: raft_server_log:state().

%%
%% API
%%
-spec create_table(atom()) ->
    ok.
create_table(TableName) ->
    _ = ets:new(TableName, [set, public, named_table]),
    ok.

%%
%% raft_server_log callbacks
%%
-spec init(options()) ->
    state().
init(Options = #{log := Log}) ->
    case get_state(Options) of
        undefined ->
            store_state(Options, raft_server_log:init(Log));
        State ->
            State
    end.

-spec indexes(_, state()) ->
    {raft_server_log:maybe_index(), raft_server_log:maybe_index()}.
indexes(#{log := Log}, State) ->
    raft_server_log:indexes(Log, State).

-spec entry(options(), raft_server_log:index(), state()) ->
    raft_server_log:entry().
entry(#{log := Log}, Index, State) ->
    raft_server_log:entry(Log, Index, State).

-spec entries(options(), raft_server_log:index(), raft_server_log:index(), state()) ->
    [raft_server_log:entry()].
entries(#{log := Log}, From, To, State) ->
    raft_server_log:entries(Log, From, To, State).

-spec append(options(), raft_server_log:index(), [raft_server_log:entry()], state()) ->
    state().
append(Options = #{log := Log}, From, Entries, _) ->
    store_state(Options, raft_server_log:append(Log, From, Entries, get_state(Options))).

-spec commit(options(), raft_server_log:index(), state()) ->
    state().
commit(Options = #{log := Log}, NewCommitIndex, _) ->
    store_state(Options, raft_server_log:commit(Log, NewCommitIndex, get_state(Options))).

%%
%% local
%%
-spec get_state(options()) ->
    state().
get_state(#{table_name := TableName, id := ID}) ->
    case ets:lookup(TableName, ID) of
        [           ] -> undefined;
        [{ID, State}] -> State
    end.

-spec store_state(options(), state()) ->
    state().
store_state(#{table_name := TableName, id := ID}, LogState) ->
    true = ets:insert(TableName, {ID, LogState}),
    LogState.
