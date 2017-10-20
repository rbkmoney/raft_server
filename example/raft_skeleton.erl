-module(raft_skeleton).

%% API
-export([start_link   /2]).
-export([sync_command /1]).
-export([async_command/1]).

%% raft
-behaviour(raft).
-export([init/1, handle_election/2, handle_command/4, handle_async_command/4, apply_delta/4]).

%%
%% API
%%
-spec start_link(raft_utils:gen_reg_name(), raft:options()) ->
    raft_utils:gen_start_ret().
start_link(RegName, RaftOptions) ->
    raft:start_link(RegName, ?MODULE, RaftOptions).

-spec sync_command(raft:options()) ->
    ok.
sync_command(#{rpc := RPC, cluster := Cluster}) ->
    raft:send_command(
        RPC,
        Cluster,
        undefined,
        sync_command,
        genlib_retry:linear(10, 100)
    ).

-spec async_command(raft:options()) ->
    ok.
async_command(#{rpc := RPC, cluster := Cluster}) ->
    raft:send_async_command(
        RPC,
        Cluster,
        undefined,
        async_command,
        genlib_retry:linear(10, 100)
    ).

%%
%% raft
%%
-type async_command() :: async_command.
-type sync_command () :: sync_command.
-type state() :: state.
-type delta() :: delta.

-spec init(_) ->
    state().
init(_) ->
    state.

-spec handle_election(_, state()) ->
    {undefined, state()}.
handle_election(_, state) ->
    {undefined, state}.

-spec handle_async_command(_, raft_rpc:request_id(), async_command(), state()) ->
    {raft:reply_action(), state()}.
handle_async_command(_, _, async_command, state) ->
    {{reply, ok}, state}.

-spec handle_command(raft_utils:gen_ref(), raft_rpc:request_id(), sync_command(), state()) ->
    {raft:reply_action(), delta() | undefined, state()}.
handle_command(_, _, sync_command, state) ->
    {{reply, ok}, delta, state}.

-spec apply_delta(_, raft_rpc:request_id(), delta(), state()) ->
    state().
apply_delta(_, _, delta, state) ->
    state.
