-module(raft_kv).

%% API
-export([start_link/2]).
-export([get       /2]).
-export([get_dirty /2]).
-export([put       /3]).
-export([remove    /2]).

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

-spec get(raft:options(), _Key) ->
    {ok, _Value} | {error, not_found}.
get(#{rpc := RPC, cluster := Cluster}, Key) ->
    raft:send_command(
        RPC,
        Cluster,
        undefined,
        {get, Key},
        genlib_retry:linear(10, 100)
    ).

-spec get_dirty(raft:options(), _Key) ->
    {ok, _Value} | {error, not_found}.
get_dirty(#{rpc := RPC, cluster := Cluster}, Key) ->
    raft:send_async_command(
        RPC,
        Cluster,
        undefined,
        {get, Key},
        genlib_retry:linear(10, 100)
    ).

-spec put(raft:options(), _Key, _Value) ->
    ok.
put(#{rpc := RPC, cluster := Cluster}, Key, Value) ->
    raft:send_command(
        RPC,
        Cluster,
        undefined,
        {put, Key, Value},
        genlib_retry:linear(10, 100)
    ).

-spec remove(raft:options(), _Key) ->
    ok.
remove(#{rpc := RPC, cluster := Cluster}, Key) ->
    raft:send_command(
        RPC,
        Cluster,
        undefined,
        {remove, Key},
        genlib_retry:linear(10, 100)
    ).

%%
%% raft
%%
-type read_command() :: {get, _Key}.
-type write_command() :: {put, _Key, _Value} | {remove, _Key}.
-type async_command() :: read_command().
-type sync_command () :: read_command() | write_command().
-type state() :: #{_Key => _Value}.
-type delta() :: write_command().

-spec init(_) ->
    state().
init(_) ->
    #{}.

-spec handle_election(_, state()) ->
    undefined.
handle_election(_, _) ->
    undefined.

-spec handle_async_command(_, raft_rpc:request_id(), async_command(), state()) ->
    raft:reply_action().
handle_async_command(_, _, {get, Key}, State) ->
    {reply, do_get(Key, State)}.

-spec handle_command(raft_utils:gen_ref(), raft_rpc:request_id(), sync_command(), state()) ->
    {raft:reply_action(), delta() | undefined}.
handle_command(_, _, {get, Key}, State) ->
    {{reply, do_get(Key, State)}, undefined};
handle_command(_, _, Put = {put, _, _}, _State) ->
    {{reply, ok}, Put};
handle_command(_, _, Remove = {remove, _}, _State) ->
    {{reply, ok}, Remove}.

-spec apply_delta(_, raft_rpc:request_id(), delta(), state()) ->
    state().
apply_delta(_, _, {put, Key, Value}, State) ->
    do_put(Key, Value, State);
apply_delta(_, _, {remove, Key}, State) ->
    do_remove(Key, State).

%%

-spec do_get(_Key, state()) ->
    {ok, _Value} | {error, not_found}.
do_get(Key, State) ->
    try
        {ok, maps:get(Key, State)}
    catch error:{badkey, _} ->
        {error, not_found}
    end.

-spec do_put(_Key, _Value, state()) ->
    state().
do_put(Key, Value, State) ->
    maps:put(Key, Value, State).

-spec do_remove(_Key, state()) ->
    state().
do_remove(Key, State) ->
    maps:remove(Key, State).
