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

-module(raft_server_pt).
-include_lib("proper/include/proper.hrl").

-export([command/1, initial_state/0, next_state/3, precondition/2, postcondition/3]).
-export([read/1, write/2, remove/1, start_server/1, stop_server/1]).

%% internal
-export([start_server_/2]).


-define(CLUSTER, [a, b, c]).
% -define(CLUSTER, [a, b, c, d, e]).
-define(SELF, a).

prop_test() ->
    ?FORALL(
        Commands,
        commands(?MODULE),
        begin
            % Options = raft_options (Cluster, Self),
            Pids = start_cluster(?CLUSTER),

            {History, State, Result} = run_commands(?MODULE, Commands),

            ok = stop_cluster(Pids),

            ?WHENFAIL(
                io:format("History: ~p~nState: ~p~nResult: ~p~n", [History, State, Result]),
                aggregate(command_names(Commands), Result =:= ok)
            )
        end
    ).

%%

-type state() :: #{
    % cluster
    values := #{}

}.

initial_state() ->
    #{
        % all_nodes   => ?CLUSTER
        % cluster     => [],
        alive_nodes => ordsets:new(),
        values      => #{}
    }.

% join
% leave
% start node
% stop node
% write
% read
-define(has_quorum(ANodes), erlang:length(ANodes) >= (erlang:length(?CLUSTER) div 2 + 1)).

command(#{alive_nodes := ANodes}) when not ?has_quorum(ANodes) ->
    {call, ?MODULE, start_server, [oneof(?CLUSTER -- ANodes)]};
command(#{alive_nodes := ANodes}) ->
    oneof(
        [
            {call, ?MODULE, write , [key(), value()]},
            {call, ?MODULE, read  , [key()         ]},
            {call, ?MODULE, remove, [key()         ]}
        ]
        ++
        case ?CLUSTER -- ANodes of
            [] -> [];
            _  -> [{call, ?MODULE, start_server, [oneof(?CLUSTER -- ANodes)]}]
        end
        ++
        case ANodes of
            [] -> [];
            _  -> [{call, ?MODULE, stop_server , [oneof(ANodes)]}]
        end
    ).

key() ->
    oneof([a, b, c]).

value() ->
    oneof([<<"a">>, <<"b">>, <<"c">>]).

precondition(#{alive_nodes := ANodes}, {call, ?MODULE, write , _}) when ?has_quorum(ANodes) -> true;
precondition(#{alive_nodes := ANodes}, {call, ?MODULE, read  , _}) when ?has_quorum(ANodes) -> true;
precondition(#{alive_nodes := ANodes}, {call, ?MODULE, remove, _}) when ?has_quorum(ANodes) -> true;
precondition(_                       , {call, ?MODULE, start_server, _}) -> true;
precondition(#{alive_nodes := ANodes}, {call, ?MODULE, stop_server , _}) when ?has_quorum(ANodes) -> true;
precondition(#{}, {call, ?MODULE, _ , _}) ->
    false.

postcondition(_, {call, ?MODULE, write, _}, ok) ->
    true;
postcondition(#{values := Values}, {call, ?MODULE, read, [Key]}, Result) ->
    case maps:get(Key, Values, undefined) of
        undefined -> Result =:= {error, not_found};
        Value     -> Result =:= {ok, Value}
    end;
postcondition(_, {call, ?MODULE, remove, _}, ok) ->
    true;
postcondition(_, {call, ?MODULE, start_server, _}, _) ->
    true;
postcondition(_, {call, ?MODULE, stop_server , _}, _) ->
    true;
postcondition(_, {call, ?MODULE, _ , _}, _) ->
    false.

next_state(State = #{values := Values}, ok, {call, ?MODULE, write, [Key, Value]}) ->
    State#{values := maps:put(Key, Value, Values)};
next_state(State = #{}, _, {call, ?MODULE, read, _}) ->
    State;
next_state(State = #{values := Values}, _, {call, ?MODULE, remove, [Key]}) ->
    State#{values := maps:remove(Key, Values)};
next_state(State = #{alive_nodes := ANodes}, _, {call, ?MODULE, start_server, [Name]}) ->
    State#{alive_nodes := ordsets:add_element(Name, ANodes)};
next_state(State = #{alive_nodes := ANodes}, _, {call, ?MODULE, stop_server, [Name]}) ->
    State#{alive_nodes := ordsets:del_element(Name, ANodes)};
next_state(State, {var,_}, _) ->
    State.

%%

read(Key) ->
    raft_kv:get(raft_options(?CLUSTER, ?SELF), Key).

write(Key, Value) ->
    % {'EXIT', {timeout, _}}
    catch raft_kv:put(raft_options(?CLUSTER, ?SELF), Key, Value).

remove(Key) ->
    ok = raft_kv:remove(raft_options(?CLUSTER, ?SELF), Key).

start_server(Name) ->
    _ = start_server(?CLUSTER, Name),
    timer:sleep(24).

stop_server(Name) ->
    case raft_utils:gen_where_ref(Name) of
        undefined -> ok;
        Pid       -> erlang:exit(Pid, kill), timer:sleep(24)
    end.

%%
%% raft cluster
%%
-type name      () :: atom().
-type cluster   () :: [name()].

-spec start_cluster(cluster()) ->
    [pid()].
start_cluster(_Cluster) ->
    _ = raft_server_log_ets_proxy:create_table(test_log_storage),
    TesterPid = raft_utils:throw_if_error(raft_rpc_tester:start_link()),
    ClusterSupPid = start_cluster_sup(),
    % [start_server(Cluster, Self) || Self <- Cluster],
    [TesterPid, ClusterSupPid].

-spec stop_cluster([]) ->
    ok.
stop_cluster(Pids) ->
    ok = raft_utils:stop_wait_all(lists:reverse(Pids), shutdown, 5000),
    true = ets:delete(test_log_storage),
    ok.

-spec start_cluster_sup() ->
    pid().
start_cluster_sup() ->
    Flags = #{strategy => simple_one_for_one},
    ChildsSpecs =
        [
            #{
                id       => raft,
                start    => {?MODULE, start_server_, []},
                % shutdown => brutal_kill,
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
    LogStorage =
        {raft_server_log_ets_proxy,
            #{
                table_name => test_log_storage,
                id         => Self,
                log        => raft_server_log_memory
            }
        },
    #{
        self              => Self,
        cluster           => Cluster,
        election_timeout  => {6, 12},
        broadcast_timeout => 3,
        log               => LogStorage,
        % log               => raft_server_log_memory,
        rpc               => {raft_rpc_tester, Self},
        % rpc               => raft_rpc_erl,
        % logger            => raft_rpc_logger_io_plant_uml,
        logger            => undefined,
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
