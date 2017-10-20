%%%
%%% Супервизор, который запускает процесс сразу на всех элементах кластера.
%%% Не будет нормально работать с simple_one_for_one, т.к. использует функцию stop,
%%% которая выполняет одновременно terminate_child и delete_child
%%%
%%% TODO:
%%%  - timeouts
%%%
-module(raft_supervisor).

%% API
-export([start_link   /3]).
-export([start_child  /2]).
-export([stop_child   /2]).
-export([get_childspec/2]).
-export([is_started   /2]).

%% raft
-behaviour(raft).
-export([init/1, handle_election/2, handle_command/4, handle_async_command/4, apply_delta/4]).

%%
%% API
%%
-spec start_link(raft_utils:gen_reg_name(), raft_utils:gen_reg_name(), raft:options()) ->
    raft_utils:gen_start_ret().
start_link(RaftRegName, SupRegName, RaftOptions) ->
    raft_utils_supervisor_wrapper:start_link(
        #{strategy => one_for_all},
        [
            #{
                id       => sup,
                start    => {supervisor, start_link, [SupRegName, {#{strategy => one_for_one}, []}]},
                restart  => permanent,
                type     => supervisor
            },
            #{
                id       => sup,
                start    => {raft, start_link, [RaftRegName, {?MODULE, gen_reg_name_to_ref(SupRegName)}, RaftOptions]},
                restart  => permanent,
                type     => worker
            }
        ]
    ).

-spec start_child(raft:options(), supervisor:child_spec()) ->
    ok | {error, already_started}.
start_child(#{rpc := RPC, cluster := Cluster}, ChildSpec) ->
    raft:send_command(
        RPC,
        Cluster,
        undefined,
        {start_child, ChildSpec},
        genlib_retry:linear(10, 100)
    ).

-spec stop_child(raft:options(), _ID) ->
    ok | {error, not_found}.
stop_child(#{rpc := RPC, cluster := Cluster}, ID) ->
    raft:send_command(
        RPC,
        Cluster,
        undefined,
        {stop_child, ID},
        genlib_retry:linear(10, 100)
    ).

-spec get_childspec(raft:options(), _ID) ->
    {ok, supervisor:child_spec()} | {error, not_found}.
get_childspec(#{rpc := RPC, cluster := Cluster}, ID) ->
    raft:send_async_command(
        RPC,
        Cluster,
        undefined,
        {get_childspec, ID},
        genlib_retry:linear(10, 100)
    ).

-spec is_started(raft:options(), _ID) ->
    boolean().
is_started(#{rpc := RPC, cluster := Cluster}, ID) ->
    raft:send_async_command(
        RPC,
        Cluster,
        undefined,
        {is_started, ID},
        genlib_retry:linear(10, 100)
    ).


%%
%% raft
%%
-type async_command() :: {get_childspec, _ID} | {is_started, _ID}.
-type sync_command () :: {start_child, supervisor:child_spec()} | {stop_child, _ID}.
-type state() :: undefined.
-type delta() :: sync_command().

-spec init(_) ->
    state().
init(_) ->
    undefined.

-spec handle_election(_, state()) ->
    undefined.
handle_election(_, _) ->
    undefined.

-spec handle_async_command(raft_utils:gen_ref(), raft_rpc:request_id(), async_command(), state()) ->
    raft:reply_action().
handle_async_command(SupRef, _, {get_childspec, ID}, undefined) ->
    {reply, supervisor:get_childspec(SupRef, ID)};
handle_async_command(SupRef, _, {is_started, ID}, undefined) ->
    Reply =
        case supervisor:get_childspec(SupRef, ID) of
            {ok   , _        } -> true;
            {error, not_found} -> false
        end,
    {reply, Reply}.

-spec handle_command(raft_utils:gen_ref(), raft_rpc:request_id(), sync_command(), state()) ->
    {raft:reply_action(), delta() | undefined}.
handle_command(SupRef, _, {start_child, ChildSpec = #{id := ID}}, _State) ->
    case supervisor:get_childspec(SupRef, ID) of
        {ok, _} ->
            {{reply, {error, already_present}}, undefined};
        {error, not_found} ->
            {{reply, ok}, {start_child, ChildSpec}}
    end;
handle_command(SupRef, _, {stop_child, ID}, _State) ->
    case supervisor:get_childspec(SupRef, ID) of
        {ok, _} ->
            {{reply, ok}, {stop_child, ID}};
        Error = {error, not_found} ->
            {{reply, Error}, undefined}
    end.

-spec apply_delta(raft_utils:gen_ref(), raft_rpc:request_id(), delta(), state()) ->
    state().
apply_delta(SupRef, _, {start_child, ChildSpec}, undefined) ->
    {ok, _} = supervisor:start_child(SupRef, ChildSpec),
    undefined;
apply_delta(SupRef, _, {stop_child, ID}, undefined) ->
    ok = supervisor:terminate_child(SupRef, ID),
    ok = supervisor:delete_child(SupRef, ID),
    undefined.

%%

-spec
gen_reg_name_to_ref(raft_utils:gen_reg_name()) -> raft_utils:gen_ref().
gen_reg_name_to_ref({local, Name}            ) -> Name;
gen_reg_name_to_ref(Global = {global, _}     ) -> Global;
gen_reg_name_to_ref(Via    = {via, _, _}     ) -> Via.
