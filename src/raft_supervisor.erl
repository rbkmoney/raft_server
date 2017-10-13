-module(raft_supervisor).

%% API
-export([start_link     /3]).
-export([start_child    /2]).
-export([terminate_child/2]).

%% raft
-behaviour(raft).
-export([init/1, handle_command/4, handle_async_command/4, apply_delta/4]).

%%
%% API
%%
-spec start_link(mg_utils:gen_reg_name(), raft:options(), mg_utils:gen_ref()) ->
    mg_utils:gen_start_ret().
start_link(RegName, RaftOptions, SupRef) ->
    raft:start_link(RegName, {?MODULE, SupRef}, RaftOptions).

-spec start_child(raft:options(), supervisor:child_spec()) ->
    ok | already_started.
start_child(#{rpc := RPC, cluster := Cluster}, ChildSpec) ->
    raft:send_command(
        RPC,
        Cluster,
        undefined,
        {start_child, ChildSpec},
        genlib_retry:linear(10, 100)
    ).

-spec terminate_child(raft:options(), _ID) ->
    ok.
terminate_child(#{rpc := RPC, cluster := Cluster}, ID) ->
    raft:send_command(
        RPC,
        Cluster,
        undefined,
        {terminate_child, ID},
        genlib_retry:linear(10, 100)
    ).

%%
%% raft
%%
-type command() :: {start_child, supervisor:child_spec()} | {terminate_child, _ID}.
-type state() :: undefined.
-type delta() :: command().

-spec init(_) ->
    state().
init(_) ->
    undefined.

-spec handle_async_command(_, raft_rpc:request_id(), command(), state()) ->
    no_return().
handle_async_command(_, _, _, _) ->
    _ = exit(1),
    noreply.

-spec handle_command(mg_utils:gen_ref(), raft_rpc:request_id(), command(), state()) ->
    {raft:reply_action(), delta() | undefined}.
handle_command(SupRef, _, {start_child, ChildSpec = #{id := ID}}, _State) ->
    case supervisor:get_childspec(SupRef, ID) of
        {ok, _} ->
            {{reply, {error, already_present}}, undefined};
        {error, not_found} ->
            {{reply, ok}, {start_child, ChildSpec}}
    end;
handle_command(SupRef, _, {terminate_child, ID}, _State) ->
    case supervisor:get_childspec(SupRef, ID) of
        {ok, _} ->
            {{reply, ok}, {terminate_child, ID}};
        Error = {error, not_found} ->
            {{reply, Error}, undefined}
    end.

-spec apply_delta(mg_utils:gen_ref(), raft_rpc:request_id(), delta(), state()) ->
    state().
apply_delta(SupRef, _, {start_child, ChildSpec}, undefined) ->
    {ok, _} = supervisor:start_child(SupRef, ChildSpec),
    undefined;
apply_delta(SupRef, _, {terminate_child, ID}, undefined) ->
    ok = supervisor:terminate_child(SupRef, ID),
    ok = supervisor:delete_child(SupRef, ID),
    undefined.

% -spec create_or_get_session(RPC, Cluster, Self, AttemptTimeout) ->
%     raft_client:session().
% create_or_get_session(RPC, Cluster, Self, AttemptTimeout) ->
%     ClusterID = {RPC, Cluster},
%     case raft_sessions_cache:get(ClusterID) of
%         undefined ->
%             Session = raft_client:new_client_session(ClusterID),
%             ok = raft_sessions_cache:put(ClusterID, Session),
%             Session;
%         Session ->
%             Session
%     end.
