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

-module(raft_kv).

%% API
-export([start_link/2]).
-export([get       /2]).
-export([get_dirty /2]).
-export([put       /3]).
-export([remove    /2]).

%% raft
-behaviour(raft).
-export([init/1, handle_election/2, handle_command/4, handle_async_command/4, handle_info/3, apply_delta/4]).

%%
%% API
%%
-spec start_link(raft_utils:gen_reg_name(), raft:options()) ->
    raft_utils:gen_start_ret().
start_link(RegName, RaftOptions) ->
    raft:start_link(RegName, ?MODULE, RaftOptions).

-spec get(raft:options(), _Key) ->
    {ok, _Value} | {error, not_found}.
get(Options = #{rpc := RPC, cluster := Cluster}, Key) ->
    raft:send_command(
        RPC,
        Cluster,
        undefined,
        {get, Key},
        retry(Options)
    ).

-spec get_dirty(raft:options(), _Key) ->
    {ok, _Value} | {error, not_found}.
get_dirty(Options = #{rpc := RPC, cluster := Cluster}, Key) ->
    raft:send_async_command(
        RPC,
        Cluster,
        undefined,
        {get, Key},
        retry(Options)
    ).

-spec put(raft:options(), _Key, _Value) ->
    ok.
put(Options = #{rpc := RPC, cluster := Cluster}, Key, Value) ->
    raft:send_command(
        RPC,
        Cluster,
        undefined,
        {put, Key, Value},
        retry(Options)
    ).

-spec remove(raft:options(), _Key) ->
    ok.
remove(Options = #{rpc := RPC, cluster := Cluster}, Key) ->
    raft:send_command(
        RPC,
        Cluster,
        undefined,
        {remove, Key},
        retry(Options)
    ).

-spec retry(raft:options()) ->
    genlib_retry:strategy().
retry(#{election_timeout := ElectionTimeout, broadcast_timeout := BroadcastTimeout}) ->
    Timeout =
        case ElectionTimeout of
            {_, V} -> V;
            V      -> V
        end,
    genlib_retry:linear({max_total_timeout, Timeout * 2}, BroadcastTimeout).


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
    {delta() | undefined, state()}.
handle_election(_, State) ->
    {undefined, State}.

-spec handle_async_command(_, raft_rpc:request_id(), async_command(), state()) ->
    {raft:reply_action(), state()}.
handle_async_command(_, _, {get, Key}, State) ->
    {reply, do_get(Key, State), State}.

-spec handle_command(raft_utils:gen_ref(), raft_rpc:request_id(), sync_command(), state()) ->
    {raft:reply_action(), delta() | undefined, state()}.
handle_command(_, _, {get, Key}, State) ->
    {{reply, do_get(Key, State)}, undefined, State};
handle_command(_, _, Put = {put, _, _}, State) ->
    {{reply, ok}, Put, State};
handle_command(_, _, Remove = {remove, _}, State) ->
    {{reply, ok}, Remove, State}.

-spec handle_info(_, _Info, state()) ->
    {undefined, state()}.
handle_info(_, Info, State) ->
    ok = error_logger:error_msg("unexpected info received: ~p", [Info]),
    {undefined, State}.

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
