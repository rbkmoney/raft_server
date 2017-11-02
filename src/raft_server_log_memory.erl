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
%%% Хранилище в памяти.
%%% Лог, хранится в виде списка.
%%% Самый простой вариант реализации лога.
%%%
-module(raft_server_log_memory).

%% raft_server_log callbacks
-behaviour(raft_server_log).
-export([init/1, entry/3, entries/4, append/4, commit/3]).

-type state() :: #{
    commit_index := raft_server_log:maybe_index(),
    log_list     := [raft_server_log:entry()]
}.

%%
%% raft_server_log callbacks
%%
-spec init(_) ->
    {raft_server_log:maybe_index(), raft_server_log:maybe_index(), state()}.
init(_) ->
    State = #{
        commit_index => 0,
        log_list     => []
    },
    {0, 0, State}.

-spec entry(_, raft_server_log:index(), state()) ->
    raft_server_log:entry() | undefined.
entry(_, Index, #{log_list := LogList}) ->
    case 0 < Index andalso Index =< erlang:length(LogList) of
        true  -> lists:nth(Index, LogList);
        false -> undefined
    end.

-spec entries(_, raft_server_log:index(), raft_server_log:index(), state()) ->
    [raft_server_log:entry()].
entries(_, From, To, #{log_list := LogList}) ->
    lists:sublist(LogList, From, To - From + 1).

-spec append(_, raft_server_log:index(), [raft_server_log:entry()], state()) ->
    state().
append(_, From, Entries, State = #{log_list := LogList}) ->
    State#{log_list := lists:sublist(LogList, 1, From) ++ Entries}.

-spec commit(_, raft_server_log:index(), state()) ->
    state().
commit(_, NewCommitIndex, State = #{commit_index := CommitIndex}) when NewCommitIndex > CommitIndex ->
    State#{commit_index := NewCommitIndex}.
