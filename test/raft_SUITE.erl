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
%%% TODO
%%%  - заменить сторадж на общий, чтобы можно было останавливать и запускать с места остановки
%%%  - ловить эвенты от лидер-группы (например о перееизбрании)
%%%  -
%%%
-module(raft_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all           /0]).
-export([init_per_suite/1]).
-export([end_per_suite /1]).

%% tests
-export([base_test                /1]).
-export([cluster_simple_split_test/1]).

%% internal
-export([start_server_/2]).

%%
%% tests descriptions
%%
-type test_name () :: atom().
-type config    () :: [{atom(), _}].
-type name      () :: atom().
-type cluster   () :: [name()].

-spec all() ->
    [test_name()].
all() ->
    [
        base_test,
        cluster_simple_split_test
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    {ok, Apps} = application:ensure_all_started(raft),

    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({raft, 'schedule_election_timer', '_'}, x),
    % dbg:tpl({?MODULE, handle_sync_command, '_'}, x),

    [
          {cluster, [erlang:list_to_atom(erlang:integer_to_list(N)) || N <- lists:seq(1, 5)]}
        , {apps, Apps}
        | C
    ].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    [application:stop(App) || App <- ?config(apps, C)].

%%
%% tests
%%
-spec base_test(config()) ->
    _.
base_test(C) ->
    Cluster = [Self|_] = ?config(cluster, C),
    Options = raft_options(Cluster, Self),
    _ = start_cluster(Cluster),
    _ = read_not_found(Options, key),
    _ = write_success(Options, key, value),
    _ = read_success(Options, key, value),
    % [erlang:exit(raft_utils:gen_where(Name), kill) || Name <- ['3', '4', '5']],
    _ = remove_successfull(Options, key),
    _ = read_not_found(Options, key).

-spec cluster_simple_split_test(config()) ->
    _.
cluster_simple_split_test(C) ->
    Cluster = [Self|_] = ?config(cluster, C),
    Options = raft_options(Cluster, Self),
    _ = start_cluster(Cluster),
    _ = write_success(Options, key, value0),
    % не успевает отреплицироваться
    ok = raft_rpc_tester:split(['1', '2', '3'], ['4', '5']),

    % ждём пока отвалившаяся группа потеряет лидера
    ok = timer:sleep(80),
    _ = write_success(raft_options(['1'], '1'), key, value1),
    _ = write_fail(raft_options(['5'], '5'), key, bad_value),
    ok = raft_rpc_tester:restore(),

    % ждём пока соединится
    ok = timer:sleep(80),
    _ = read_success(raft_options(['5'], '5'), key, value1),
    _ = read_success(raft_options(['1'], '1'), key, value1).

-spec read_not_found(raft_server:options(), _Key) ->
    _.
read_not_found(Options, Key) ->
    {error, not_found} = raft_kv:get(Options, Key).

-spec write_success(raft_server:options(), _Key, _Value) ->
    _.
write_success(Options, Key, Value) ->
    ok = raft_kv:put(Options, Key, Value).

-spec write_fail(raft_server:options(), _Key, _Value) ->
    _.
write_fail(Options, Key, Value) ->
    {'EXIT', {{timeout, _, _, _}, _}} = (catch raft_kv:put(Options, Key, Value)).

-spec read_success(raft_server:options(), _Key, _Value) ->
    _.
read_success(Options, Key, Value) ->
    {ok, Value} = raft_kv:get(Options, Key).

-spec remove_successfull(raft_server:options(), _Key) ->
    _.
remove_successfull(Options, Key) ->
    ok = raft_kv:remove(Options, Key).


%%
%% raft cluster
%%
-spec start_cluster(cluster()) ->
    _.
start_cluster(Cluster) ->
    _ = raft_utils:throw_if_error(raft_rpc_tester:start_link()),
    _ = start_cluster_sup(),
    [start_server(Cluster, Self) || Self <- Cluster].

-spec start_cluster_sup() ->
    pid().
start_cluster_sup() ->
    Flags = #{strategy => simple_one_for_one},
    ChildsSpecs =
        [
            #{
                id       => raft,
                start    => {?MODULE, start_server_, []},
                shutdown => brutal_kill,
                restart  => temporary,
                type     => worker
            }
        ],
    raft_utils:throw_if_error(supervisor_wrapper:start_link({local, cluster_sup}, Flags, ChildsSpecs)).

-spec start_server(cluster(), name()) ->
    pid().
start_server(Cluster, Name) ->
    case supervisor:start_child(cluster_sup, [Cluster, Name]) of
        {ok, Pid} ->
            Pid;
        {error, {already_started, Pid}} ->
            Pid
    end.

-spec start_server_(cluster(), name()) ->
    raft_utils:gen_start_ret().
start_server_(Cluster, Name) ->
    raft_kv:start_link(
        {local, Name},
        raft_options(Cluster, Name)
    ).

-spec raft_options(cluster(), name()) ->
    raft_server:options().
raft_options(Cluster, Self) ->
    N = lists_index(Self, Cluster),
    #{
        self              => Self,
        cluster           => Cluster,
        election_timeout  => {20, 40},
        broadcast_timeout => 10,
        storage           => raft_storage_memory,
        rpc               => {raft_rpc_tester, Self},
        logger            => raft_rpc_logger_io_plant_uml,
        random_seed       => {0, N, N * 10}
    }.

-spec lists_index(_V, []) ->
    pos_integer() | undefined.
lists_index(V, L) ->
    lists_index(V, L, 1).

-spec lists_index(_V, [], pos_integer()) ->
    pos_integer() | undefined.
lists_index(_, [], _) ->
    undefined;
lists_index(V, [V | _], N) ->
    N;
lists_index(V, [_ | L], N) ->
    lists_index(V, L, N + 1).
