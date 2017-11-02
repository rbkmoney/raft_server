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
%%% Interface for raft server log.
%%% It can be implemented in varius way: in memory, in a SQL db, in a file, etc.
%%%
-module(raft_server_log).

%% API
-export_type([log        /0]).
-export_type([index      /0]).
-export_type([maybe_index/0]).
-export_type([entry      /0]).
-export_type([state      /0]).
-export([init   /1]).
-export([indexes/2]).
-export([entry  /3]).
-export([entries/4]).
-export([append /4]).
-export([commit /3]).

%%
%% API
%%
-type log        () :: raft_utils:mod_opts().
-type index      () :: pos_integer().
-type maybe_index() :: index() | 0.
-type entry      () :: _.
-type state      () :: _.
%%

-callback init(_) ->
    state().

-callback indexes(_, state()) ->
    {raft_server_log:maybe_index(), raft_server_log:maybe_index()}.

-callback entry(_, index(), state()) ->
    entry() | undefined.

-callback entries(_, index(), index(), state()) ->
    [entry()].

% !! дропнуть всё за From и добавить Entries
% !! если этого не сделать, то всё будет работать, но будут случаи, когда нарушится консистентность
-callback append(_, maybe_index(), [entry()], state()) ->
    state().

-callback commit(_, index(), state()) ->
    state().

%%

-spec init(log()) ->
    state().
init(Log) ->
    raft_utils:apply_mod_opts(Log, init, []).

-spec indexes(log(), state()) ->
    {raft_server_log:maybe_index(), raft_server_log:maybe_index()}.
indexes(Log, State) ->
    raft_utils:apply_mod_opts(Log, indexes, [State]).

-spec entry(log(), index(), state()) ->
    entry() | undefined.
entry(Log, Index, State) ->
    raft_utils:apply_mod_opts(Log, entry, [Index, State]).

-spec entries(log(), index(), index(), state()) ->
    [entry()].
entries(Log, From, To, State) ->
    raft_utils:apply_mod_opts(Log, entries, [From, To, State]).

% !! дропнуть всё за From и добавить Entries
% !! если этого не сделать, то всё будет работать, но будут случаи, когда нарушится консистентность
-spec append(log(), index(), [entry()], state()) ->
    state().
append(Log, From, Entries, State) ->
    raft_utils:apply_mod_opts(Log, append, [From, Entries, State]).

-spec commit(log(), index(), state()) ->
    state().
commit(Log, Index, State) ->
    raft_utils:apply_mod_opts(Log, commit, [Index, State]).

