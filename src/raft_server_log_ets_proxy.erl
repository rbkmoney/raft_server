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

-type fake_state() :: fake_state.

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
    fake_state().
init(Options = #{log := Log}) ->
    _ = case get_state(Options) of
            undefined ->
                store_state(Options, raft_server_log:init(Log));
            _ ->
                ok
        end,
    fake_state.

-spec indexes(_, fake_state()) ->
    {raft_server_log:maybe_index(), raft_server_log:maybe_index()}.
indexes(Options = #{log := Log}, fake_state) ->
    raft_server_log:indexes(Log, get_state(Options)).

-spec entry(options(), raft_server_log:index(), fake_state()) ->
    raft_server_log:entry().
entry(Options = #{log := Log}, Index, fake_state) ->
    raft_server_log:entry(Log, Index, get_state(Options)).

-spec entries(options(), raft_server_log:index(), raft_server_log:index(), fake_state()) ->
    [raft_server_log:entry()].
entries(Options = #{log := Log}, From, To, fake_state) ->
    raft_server_log:entries(Log, From, To, get_state(Options)).

-spec append(options(), raft_server_log:index(), [raft_server_log:entry()], fake_state()) ->
    fake_state().
append(Options = #{log := Log}, From, Entries, fake_state) ->
    _ = store_state(Options, raft_server_log:append(Log, From, Entries, get_state(Options))),
    fake_state.

-spec commit(options(), raft_server_log:index(), fake_state()) ->
    fake_state().
commit(Options = #{log := Log}, NewCommitIndex, fake_state) ->
    _ = store_state(Options, raft_server_log:commit(Log, NewCommitIndex, get_state(Options))),
    fake_state.

%%
%% local
%%
-spec get_state(options()) ->
    raft_server_log:state().
get_state(#{table_name := TableName, id := ID}) ->
    case ets:lookup(TableName, ID) of
        [           ] -> undefined;
        [{ID, State}] -> State
    end.

-spec store_state(options(), raft_server_log:state()) ->
    _.
store_state(#{table_name := TableName, id := ID}, LogState) ->
    true = ets:insert(TableName, {ID, LogState}).
