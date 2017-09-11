%% разделить тесты
-module(raft_server_SUITE).
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
-export([wait_for_leader      /1]).
% -export([test_write_master/1]).
% -export([test_read_master /1]).
% -export([test_read_slave  /1]).
% -export([test_write_slave /1]).
% -export([test_read_master /1]).
% -export([test_read_slave  /1]).

%% raft_server
-behaviour(raft_server_fsm).
-export([handle_command/3]).

-export([start_rpc_        /2]).
-export([start_raft_server_/3]).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name () :: atom().
-type config    () :: [{atom(), _}].
-type rpc() ::
      raft_server_rpc_erl
    | {raft_server_rpc_sctp, one | many, sctp_rpc, raft_server_rpc_sctp:peer()}
.
-type name      () :: atom().
-type cluster   () :: [name()].

-spec all() ->
    [test_name()].
all() ->
    [
       {group, erl                  },
       {group, sctp_one_dispatcher  },
       {group, sctp_many_dispatchers}
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [
        {erl                  , [sequence], test_workflow()},
        {sctp_one_dispatcher  , [sequence], test_workflow()},
        {sctp_many_dispatchers, [sequence], test_workflow()}
    ].

-spec test_workflow() ->
    [test_name()].
test_workflow() ->
    [
        start_rpc_dispatchers,
        start_cluster,
        wait_for_leader
        % kill_leader,
        % wait_for_leader,
        % test_write_master,
        % test_read_master,
        % test_read_slave,
        % test_write_slave,
        % test_read_master,
        % test_read_slave
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({raft_server, 'get_leader', '_'}, x),
    dbg:tpl({raft_server, 'become_leader', '_'}, x),
    % dbg:tpl({raft_server_rpc_erl, 'send', '_'}, x),
    % dbg:tpl({raft_server_rpc_erl, 'recv', '_'}, x),
    % dbg:tpl({raft_server_rpc_sctp, 'send', '_'}, x),
    % dbg:tpl({raft_server_rpc_sctp, 'recv', '_'}, x),
    % dbg:tpl({raft_server_rpc_sctp_dispatcher, 'handle_info', '_'}, x),
    % dbg:tpl({raft_server_rpc_sctp_dispatcher, '_', '_'}, x),
    % dbg:tpl({raft_server_rpc_sctp, '_', '_'}, x),
    % [{cluster, [a, b, c, d, e]} | C].
    [{cluster, [a, b, c]} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(_C) ->
    ok.

-spec init_per_group(group_name(), config()) ->
    config().
init_per_group(GroupName, C) ->
    RPC =
        case GroupName of
            erl                   ->  erl;
            sctp_one_dispatcher   -> {sctp, one , sctp_rpc, {{127, 0, 0, 1}, 1900}};
            sctp_many_dispatchers -> {sctp, many, sctp_rpc, {{127, 0, 0, 1}, 1900}}
        end,
    {ok, RPCSupPid} = start_rpc_sup(),
    true = erlang:unlink(RPCSupPid),
    {ok, SupPid} = start_raft_server_sup(),
    true = erlang:unlink(SupPid),
    [{rpc, RPC} | C].

-spec end_per_group(group_name(), config()) ->
    ok.
end_per_group(_, _C) ->
    ok = gen_server:stop(raft_server_sup),
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
    % ?config(rpc_type, C)
    lists:foreach(
        fun(Name) ->
            start_raft_server(?config(rpc, C), Cluster, Name)
        end,
        Cluster
    ).

-spec wait_for_leader(config()) ->
    _.
wait_for_leader(C) ->
    [FirstEndpoint|OtherEndpoints] = ?config(cluster, C),
    FirstLeader = raft_server:get_leader(FirstEndpoint),
    IsLeaderTheSame =
        lists:all(
            fun(Endpoint) ->
                raft_server:get_leader(Endpoint) =:= FirstLeader
            end,
            OtherEndpoints
        ),
    case FirstLeader =/= undefined andalso IsLeaderTheSame of
        true ->
            ok;
        false ->
            timer:sleep(100),
            wait_for_leader(C)
    end.

% kill_leader() ->
%     TODO
%     [Endpoint|_] = ?config(cluster, C),
%     {ok, Leader} = raft_server:get_leader(FirstEndpoint),

%     erlang:exit(shutdown, Leader),

%%
%% Raft servers
%%
-spec start_raft_server_sup() ->
    pid().
start_raft_server_sup() ->
    Flags = #{strategy => simple_one_for_one},
    ChildsSpecs =
        [
            #{
                id       => raft_server,
                start    => {?MODULE, start_raft_server_, []},
                shutdown => brutal_kill,
                restart  => temporary,
                type     => worker
            }
        ],
    mg_utils_supervisor_wrapper:start_link({local, raft_server_sup}, Flags, ChildsSpecs).

-spec start_raft_server(rpc(), cluster(), name()) ->
    pid().
start_raft_server(RPC, Cluster, Name) ->
    mg_utils:throw_if_error(supervisor:start_child(raft_server_sup, [RPC, Cluster, Name])).

-spec start_raft_server_(rpc(), cluster(), name()) ->
    mg_utils:gen_start_ret().
start_raft_server_(RPC, Cluster, Name) ->
    raft_server:start_link(
        {local, Name},
        ?MODULE,
        raft_server_storage_memory,
        rpc_mod_opts(RPC, Name),
        raft_server_options(RPC, Cluster, Name)
    ).

-spec raft_server_options(rpc(), cluster(), name()) ->
    raft_server:options().
raft_server_options(RPC, Cluster, Self) ->
    {ElectionTimeout, BroadcastTimeout} = raft_server_timeouts(RPC),
    #{
        self              => rpc_endpoint(RPC, Self),
        others            => ordsets:from_list([rpc_endpoint(RPC, Name) || Name <- Cluster -- [Self]]),
        election_timeout  => ElectionTimeout,
        broadcast_timeout => BroadcastTimeout
    }.

-spec raft_server_timeouts(rpc()) ->
    _.
raft_server_timeouts(erl) ->
    {{150, 300}, 50};
raft_server_timeouts({sctp, _, _, _}) ->
    {{150, 300}, 50}.
    % {{1500, 3000}, 500}.

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
    mg_utils_supervisor_wrapper:start_link({local, rpc_sup}, Flags, ChildsSpecs).

-spec start_rpc(rpc(), cluster()) ->
    ok.
start_rpc(erl, _) ->
    ok;
start_rpc(RPC = {sctp, one, _, _}, _) ->
    mg_utils:throw_if_error(supervisor:start_child(rpc_sup, [RPC, undefined]));
start_rpc(RPC = {sctp, many, _, _}, Cluster) ->
    [mg_utils:throw_if_error(supervisor:start_child(rpc_sup, [RPC, Name])) || Name <- Cluster].

-spec start_rpc_(rpc(), name() | undefined) ->
    pid().
start_rpc_({sctp, Mode, BaseDispatcherRef, SCTPPeer}, Name) ->
    raft_server_rpc_sctp:start_link(
        {local, sctp_dispatcher_ref(Mode, BaseDispatcherRef, Name)},
        sctp_rpc_options(Mode, SCTPPeer, Name)
    ).

-spec sctp_rpc_options(one | many, raft_server_rpc_sctp:peer(), name()) ->
    raft_server_rpc_sctp:options().
sctp_rpc_options(Mode, Peer, Name) ->
    {IP, Port} = sctp_peer(Mode, Peer, Name),
    #{
        port           => Port,
        ip             => IP,
        socket_options => []
    }.

%%

-spec rpc_mod_opts(rpc(), name()) ->
    mg_utils:mod_opts().
rpc_mod_opts(erl, _) ->
    raft_server_rpc_erl;
rpc_mod_opts({sctp, Mode, DispatcherRef, _}, Name) ->
    {raft_server_rpc_sctp, sctp_dispatcher_ref(Mode, DispatcherRef, Name)}.

-spec sctp_dispatcher_ref(one | many, mg_utils:gen_ref(), name()) ->
    mg_utils:gen_ref().
sctp_dispatcher_ref(one, DispatcherRef, _) ->
    DispatcherRef;
sctp_dispatcher_ref(many, BaseDispatcherRef, Name) ->
    erlang:list_to_atom(erlang:atom_to_list(BaseDispatcherRef) ++ "_" ++ erlang:atom_to_list(Name)).

-spec rpc_endpoint(rpc(), name()) ->
    raft_server_rpc:endpoint().
rpc_endpoint(erl, Name) ->
    Name;
rpc_endpoint({sctp, Mode, _, Peer}, Name) ->
    {sctp_peer(Mode, Peer, Name), Name}.

-spec sctp_peer(one | many, raft_server_rpc_sctp:peer(), name()) ->
    raft_server_rpc_sctp:peer().
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
%% raft_server_fsm
%%
-spec handle_command(_, raft_server_storage:command(), raft_server_storage:state()) ->
    raft_server_storage:state().
handle_command(_, V, State) ->
    ok = ct:pal("handle command: ~p", [V]),
    raft_server_storage:set(fsm, [{value, V}], State).
