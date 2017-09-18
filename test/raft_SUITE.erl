%% разделить тесты
-module(raft_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all             /0]).
-export([groups          /0]).
-export([init_per_suite  /1]).
-export([end_per_suite   /1]).
-export([init_per_group  /2]).
-export([end_per_group   /2]).

%% tests
-export([start_rpc_dispatchers/1]).
-export([start_cluster        /1]).
-export([election             /1]).
-export([reelection           /1]).
-export([test_write_read      /1]).
-export([sleep                /1]).

%% raft
-behaviour(raft).
-export([handle_async_command/3]).
-export([handle_sync_command/3]).

%% raft_logger
-behaviour(raft_logger).
-export([log/4]).

-export([start_rpc_ /2]).
-export([start_raft_/3]).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name () :: atom().
-type config    () :: [{atom(), _}].
-type rpc_config() ::
      raft_rpc_erl
    | {raft_rpc_sctp, one | many, sctp_rpc, raft_rpc_sctp:peer()}
.
-type name           () :: atom().
-type cluster_config () :: [name()].

-spec all() ->
    [test_name()].
all() ->
    [
       {group, sctp_one_dispatcher  },
       {group, sctp_many_dispatchers},
       {group, erl                  }
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [
        {sctp_one_dispatcher  , [sequence], test_workflow()},
        {sctp_many_dispatchers, [sequence], test_workflow()},
        {erl                  , [sequence], test_workflow()}
    ].

-spec test_workflow() ->
    [test_name()].
test_workflow() ->
    [
        % TODO периодически падает из-за отсутствия идемпотентоности команд
        start_rpc_dispatchers,

        % кластер стартует, выбирается лидер, проходит запись
        start_cluster,
        election,
        test_write_read,

        % кластер теряет лидера, выбирается лидер, проходит запись
        reelection,
        test_write_read,

        % кластер теряет ещё одного лидера, выбирается лидер, проходит запись
        reelection,
        test_write_read,

        % кластер находится в неактивности, всё работает (может эту стадию не надо?)
        sleep,
        test_write_read,

        % кластер получает обратно всех, теряет лидера, проходит запись
        start_cluster,
        reelection,
        test_write_read
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    {ok, Apps} = application:ensure_all_started(raft),

    dbg:tracer(), dbg:p(all, c),
    dbg:tpl({?MODULE, retry_strategy, '_'}, x),

    [
          % {cluster, [a, b, c]}
          {cluster, [a, b, c, d, e]}
        , {apps, Apps}
        | C
    ].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    [application:stop(App) || App <- ?config(apps, C)].

-spec init_per_group(group_name(), config()) ->
    config().
init_per_group(GroupName, C) ->
    RPC =
        case GroupName of
            erl                   ->  erl;
            sctp_one_dispatcher   -> {sctp, one , sctp_rpc, {{127, 0, 0, 1}, 1900}};
            sctp_many_dispatchers -> {sctp, many, sctp_rpc, {{127, 0, 0, 1}, 1900}}
        end,
    RPCSupPid = start_rpc_sup(),
    true = erlang:unlink(RPCSupPid),
    SupPid = start_raft_sup(),
    true = erlang:unlink(SupPid),
    [{rpc, RPC} | C].

-spec end_per_group(group_name(), config()) ->
    ok.
end_per_group(_, _C) ->
    ok = gen_server:stop(raft_sup),
    ok = gen_server:stop(rpc_sup).

%%
%% tests
%%
-spec start_rpc_dispatchers(config()) ->
    _.
start_rpc_dispatchers(C) ->
    _ = start_rpc(?config(rpc, C), ?config(cluster, C)).

-spec start_cluster(config()) ->
    _.
start_cluster(C) ->
    Cluster = ?config(cluster, C),
    lists:foreach(
        fun(Name) ->
            start_raft(?config(rpc, C), Cluster, Name)
        end,
        Cluster
    ).

-spec election(config()) ->
    _.
election(C) ->
    get_leader(?config(rpc, C), ?config(cluster, C)).

-spec reelection(config()) ->
    _.
reelection(C) ->
    kill_leader(?config(rpc, C), ?config(cluster, C)),
    election(C).

-spec test_write_read(config()) ->
    _.
test_write_read(C) ->
    Rand = rand:uniform(1000000),
    ok = write_value(?config(rpc, C), ?config(cluster, C), Rand),
    Rand = sync_read_value(?config(rpc, C), ?config(cluster, C)).
    % Rand = async_read_value(?config(rpc, C), ?config(cluster, C)).


-spec sleep(config()) ->
    _.
sleep(_) ->
    timer:sleep(1000).

%%
%% Raft servers
%%
-spec start_raft_sup() ->
    pid().
start_raft_sup() ->
    Flags = #{strategy => simple_one_for_one},
    ChildsSpecs =
        [
            #{
                id       => raft,
                start    => {?MODULE, start_raft_, []},
                shutdown => brutal_kill,
                restart  => temporary,
                type     => worker
            }
        ],
    mg_utils:throw_if_error(mg_utils_supervisor_wrapper:start_link({local, raft_sup}, Flags, ChildsSpecs)).

-spec start_raft(rpc_config(), cluster_config (), name()) ->
    pid().
start_raft(RPC, Cluster, Name) ->
    case supervisor:start_child(raft_sup, [RPC, Cluster, Name]) of
        {ok, Pid} ->
            Pid;
        {error, {already_started, Pid}} ->
            Pid
    end.

-spec start_raft_(rpc_config(), cluster_config (), name()) ->
    mg_utils:gen_start_ret().
start_raft_(RPC, Cluster, Name) ->
    raft:start_link(
        {local, Name},
        ?MODULE,
        raft_storage_memory,
        rpc_mod_opts(RPC, Name),
        ?MODULE,
        raft_options(RPC, Cluster, Name)
    ).

-spec raft_options(rpc_config(), cluster_config (), name()) ->
    raft:options().
raft_options(RPC, Cluster, Self) ->
    {ElectionTimeout, BroadcastTimeout} = raft_timeouts(RPC),
    #{
        self              => rpc_endpoint(RPC, Self),
        others            => cluster(RPC, Cluster -- [Self]),
        election_timeout  => ElectionTimeout,
        broadcast_timeout => BroadcastTimeout
    }.

-spec cluster(rpc_config(), cluster_config ()) ->
    [raft_rpc:endpoint()].
cluster(RPC, Cluster) ->
    ordsets:from_list([rpc_endpoint(RPC, Name) || Name <- Cluster]).

-spec raft_timeouts(rpc_config()) ->
    _.
raft_timeouts(_) ->
    {{200, 400}, 100}.

%%
%% RPC
%%
-spec start_rpc_sup() ->
    pid().
start_rpc_sup() ->
    Flags = #{strategy => simple_one_for_one},
    ChildsSpecs =
        [
            #{
                id       => rpc,
                start    => {?MODULE, start_rpc_, []},
                shutdown => brutal_kill,
                restart  => temporary,
                type     => worker
            }
        ],
    mg_utils:throw_if_error(mg_utils_supervisor_wrapper:start_link({local, rpc_sup}, Flags, ChildsSpecs)).

-spec start_rpc(rpc_config(), cluster_config ()) ->
    ok.
start_rpc(erl, _) ->
    ok;
start_rpc(RPC = {sctp, one, _, _}, _) ->
    mg_utils:throw_if_error(supervisor:start_child(rpc_sup, [RPC, undefined]));
start_rpc(RPC = {sctp, many, _, _}, Cluster) ->
    [mg_utils:throw_if_error(supervisor:start_child(rpc_sup, [RPC, Name])) || Name <- Cluster].

-spec start_rpc_(rpc_config(), name() | undefined) ->
    pid().
start_rpc_({sctp, Mode, BaseDispatcherRef, SCTPPeer}, Name) ->
    raft_rpc_sctp:start_link(
        {local, sctp_dispatcher_ref(Mode, BaseDispatcherRef, Name)},
        sctp_rpc_options(Mode, SCTPPeer, Name)
    ).

-spec sctp_rpc_options(one | many, raft_rpc_sctp:peer(), name()) ->
    raft_rpc_sctp:options().
sctp_rpc_options(Mode, Peer, Name) ->
    {IP, Port} = sctp_peer(Mode, Peer, Name),
    #{
        port           => Port,
        ip             => IP,
        socket_options => []
    }.

%%

-spec rpc_mod_opts(rpc_config(), name()) ->
    mg_utils:mod_opts().
rpc_mod_opts(erl, _) ->
    raft_rpc_erl;
rpc_mod_opts({sctp, Mode, DispatcherRef, Peer}, Name) ->
    {raft_rpc_sctp, {sctp_peer(Mode, Peer, Name), sctp_dispatcher_ref(Mode, DispatcherRef, Name)}}.

-spec sctp_dispatcher_ref(one | many, mg_utils:gen_ref(), name()) ->
    mg_utils:gen_ref().
sctp_dispatcher_ref(one, DispatcherRef, _) ->
    DispatcherRef;
sctp_dispatcher_ref(many, BaseDispatcherRef, Name) ->
    erlang:list_to_atom(erlang:atom_to_list(BaseDispatcherRef) ++ "_" ++ erlang:atom_to_list(Name)).

-spec rpc_endpoint(rpc_config(), name()) ->
    raft_rpc:endpoint().
rpc_endpoint(erl, Name) ->
    Name;
rpc_endpoint({sctp, Mode, _, Peer}, Name) ->
    {sctp_peer(Mode, Peer, Name), Name}.

-spec sctp_peer(one | many, raft_rpc_sctp:peer(), name()) ->
    raft_rpc_sctp:peer().
sctp_peer(one, {Host, Port}, _) ->
    {Host, Port};
sctp_peer(many, {Host, Port}, Name) ->
    {Host, Port + sctp_name_to_port(Name)}.

-spec
sctp_name_to_port(name()) -> pos_integer().
sctp_name_to_port(a     ) -> 1;
sctp_name_to_port(b     ) -> 2;
sctp_name_to_port(c     ) -> 3;
sctp_name_to_port(d     ) -> 4;
sctp_name_to_port(e     ) -> 5.

%%
%% raft_fsm
%%
-spec write_value(rpc_config(), cluster_config(), term()) ->
    ok.
write_value(RPCConfig, ClusterConfig, Value) ->
    Cluster = cluster(RPCConfig, ClusterConfig),
    write_value_(rpc_mod_opts(RPCConfig, a), Cluster, Cluster, Value).

-spec write_value_(raft_rpc:rpc(), [raft_rpc:endpoint()], [raft_rpc:endpoint()], term()) ->
    ok.
write_value_(_, [], [], _) ->
    exit(empty_cluster);
write_value_(RPC, [], AllCluster, Value) ->
    write_value_(RPC, AllCluster, AllCluster, Value);
write_value_(RPC, Cluster, AllCluster, Value) ->
    To = raft_rpc:get_nearest(RPC, Cluster),
    ok = raft:send_sync_command(RPC, To, {write_value, Value, raft_rpc:self(RPC)}),
    case raft:recv_response_command(RPC, 300) of
        {ok, ok} -> ok;
        timeout  -> write_value_(RPC, Cluster -- [To], AllCluster, Value)
    end.


-spec sync_read_value(rpc_config(), cluster_config()) ->
    term().
sync_read_value(RPCConfig, ClusterConfig) ->
    Cluster = cluster(RPCConfig, ClusterConfig),
    sync_read_value_(rpc_mod_opts(RPCConfig, a), Cluster, Cluster).

-spec sync_read_value_(raft_rpc:rpc(), [raft_rpc:endpoint()], [raft_rpc:endpoint()]) ->
    term().
sync_read_value_(_, [], []) ->
    exit(empty_cluster);
sync_read_value_(RPC, [], AllCluster) ->
    sync_read_value_(RPC, AllCluster, AllCluster);
sync_read_value_(RPC, Cluster, AllCluster) ->
    To = raft_rpc:get_nearest(RPC, Cluster),
    ok = raft:send_sync_command(RPC, To, {read_value, raft_rpc:self(RPC)}),
    case raft:recv_response_command(RPC, 300) of
        {ok, Value} -> Value;
        timeout  -> sync_read_value_(RPC, Cluster -- [To], AllCluster)
    end.

% пока не нужно
% -spec async_read_value(rpc_config(), cluster_config()) ->
%     term().
% async_read_value(RPCConfig, ClusterConfig) ->
%     RPC = rpc_mod_opts(RPCConfig, a),
%     Cluster = cluster(RPCConfig, ClusterConfig),
%     ok = raft:send_async_command(RPC, Cluster, {read_value, raft_rpc:self(RPC)}),
%     case raft:recv_response_command(RPC, 300) of
%         {ok, Value} -> Value;
%         timeout     -> async_read_value(RPCConfig, ClusterConfig)
%     end.

-spec get_leader(rpc_config(), cluster_config()) ->
    raft_rpc:endpoint().
get_leader(RPCConfig, ClusterConfig) ->
    Cluster = cluster(RPCConfig, ClusterConfig),
    get_leader_(rpc_mod_opts(RPCConfig, a), Cluster, Cluster).

-spec get_leader_(raft_rpc:rpc(), [raft_rpc:endpoint()], [raft_rpc:endpoint()]) ->
    raft_rpc:endpoint().
get_leader_(_, [], []) ->
    exit(empty_cluster);
get_leader_(RPC, [], AllCluster) ->
    get_leader_(RPC, AllCluster, AllCluster);
get_leader_(RPC, Cluster, AllCluster) ->
    To = raft_rpc:get_nearest(RPC, Cluster),
    ok = raft:send_sync_command(RPC, To, {get_leader, raft_rpc:self(RPC)}),
    case raft:recv_response_command(RPC, 300) of
        {ok, Leader} -> Leader;
        timeout      -> get_leader_(RPC, Cluster -- [To], AllCluster)
    end.

-spec kill_leader(rpc_config(), cluster_config()) ->
    ok.
kill_leader(RPCConfig, ClusterConfig) ->
    Leader = get_leader(RPCConfig, ClusterConfig),
    exit(Leader, kill).

%%

-spec handle_async_command(_, raft_storage:command(), raft:ext_state()) ->
    raft_storage:state().
handle_async_command(_, {read_value, From}, #{rpc := RPC, storage := #{storage := Storage, handler := State}}) ->
    raft:send_response_command(RPC, From, raft_storage:get_one(Storage, value, State)).

-spec handle_sync_command(_, _, raft:ext_state()) ->
    ok.
handle_sync_command(_, {get_leader, From}, #{rpc := RPC, storage := #{handler := State}, role := Role}) ->
    case Role of
        leader -> ok = raft:send_response_command(RPC, From, erlang:self());
        _      -> ok
    end,
    State;
handle_sync_command(_, {read_value, From}, #{rpc := RPC, storage := #{storage := Storage, handler := State}, role := Role}) ->
    case Role of
        leader -> ok = raft:send_response_command(RPC, From, raft_storage:get_one(Storage, value, State));
        _      -> ok
    end,
    State;
handle_sync_command(_, {write_value, Value, From}, #{rpc := RPC, storage := #{storage := Storage, handler := State}, role := Role}) ->
    NewState = raft_storage:put(Storage, [{value, Value}], State),
    case Role of
        leader -> ok = raft:send_response_command(RPC, From, ok);
        _      -> ok
    end,
    NewState.

%%
%% logger
%%
%% хочется больше эвентов в seq_dia и чтобы они были в одном файле
%%  - название теста
%%  - создание/удаление элементов
%%  - шедулинг таймера

% рендер http://www.plantuml.com/plantuml/uml/
-spec log(_, raft_logger:event(), raft:state(), raft:state()) ->
    ok.
log(_, timeout, StateBefore, StateAfter) ->
    io:format("->\"~s\" : timeout~n~s", [raft:format_id(StateBefore), format_state_transition(StateBefore, StateAfter)]);
log(_, {incoming_message, Message = {internal, {_, _, _, From, _}}}, StateBefore, StateAfter) ->
    io:format("\"~s\"->\"~s\" : ~s~n~s",
        [raft_rpc:format_endpoint(From), raft:format_id(StateBefore), raft_rpc:format_message(Message), format_state_transition(StateBefore, StateAfter)]);
log(_, {incoming_message, Message}, StateBefore, StateAfter) ->
    io:format("->\"~s\" : ~s~n~s",
        [raft:format_id(StateBefore), raft_rpc:format_message(Message), format_state_transition(StateBefore, StateAfter)]).

-spec format_state_transition(raft:state(), raft:state()) ->
    ok.
format_state_transition(StateBefore, StateAfter) ->
    io_lib:format("note left of \"~s\"~n\t~s~n\t~s~nend note",
        [raft:format_id(StateBefore), raft:format_state(StateBefore), raft:format_state(StateAfter)]).
