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

%% raft_server
-behaviour(raft_server).
-export([init/2, handle_election/3, handle_surrend/3, handle_command/5, handle_async_command/5, handle_info/4, apply_delta/5]).

%%
%% API
%%
-spec start_link(raft_utils:gen_reg_name(), raft_server:options()) ->
    raft_utils:gen_start_ret().
start_link(RegName, RaftOptions) ->
    raft_server:start_link(RegName, ?MODULE, RaftOptions).

-spec get(raft_server:options(), _Key) ->
    {ok, _Value} | {error, not_found}.
get(Options = #{rpc := RPC, cluster := Cluster}, Key) ->
    raft_server:send_command(
        RPC,
        Cluster,
        undefined,
        {get, Key},
        retry(Options)
    ).

-spec get_dirty(raft_server:options(), _Key) ->
    {ok, _Value} | {error, not_found}.
get_dirty(Options = #{rpc := RPC, cluster := Cluster}, Key) ->
    raft_server:send_async_command(
        RPC,
        Cluster,
        undefined,
        {get, Key},
        retry(Options)
    ).

-spec put(raft_server:options(), _Key, _Value) ->
    ok.
put(Options = #{rpc := RPC, cluster := Cluster}, Key, Value) ->
    raft_server:send_command(
        RPC,
        Cluster,
        undefined,
        {put, Key, Value},
        retry(Options)
    ).

-spec remove(raft_server:options(), _Key) ->
    ok.
remove(Options = #{rpc := RPC, cluster := Cluster}, Key) ->
    raft_server:send_command(
        RPC,
        Cluster,
        undefined,
        {remove, Key},
        retry(Options)
    ).

-spec retry(raft_server:options()) ->
    genlib_retry:strategy().
retry(#{election_timeout := ElectionTimeout, broadcast_timeout := BroadcastTimeout}) ->
    Timeout =
        case ElectionTimeout of
            {_, V} -> V;
            V      -> V
        end,
    genlib_retry:linear({max_total_timeout, Timeout * 2}, BroadcastTimeout).


%%
%% raft_server
%%
-type read_command() :: {get, _Key}.
-type write_command() :: {put, _Key, _Value} | {remove, _Key}.
-type async_command() :: read_command().
-type sync_command () :: read_command() | write_command().
-type state() :: #{_Key => _Value}.
-type delta() :: write_command().

-spec init(_, _) ->
    {raft_server:maybe_index(), state()}.
init(_, _) ->
    {0, #{}}.

-spec handle_election(_, _, state()) ->
    {delta() | undefined, state()}.
handle_election(_, _, State) ->
    {undefined, State}.

-spec handle_surrend(_, _, state()) ->
    state().
handle_surrend(_, _, State) ->
    State.

-spec handle_async_command(_, raft_rpc:request_id(), async_command(), _, state()) ->
    {raft_server:reply_action(), state()}.
handle_async_command(_, _, {get, Key}, _, State) ->
    {reply, do_get(Key, State), State}.

-spec handle_command(raft_utils:gen_ref(), raft_rpc:request_id(), sync_command(), _, state()) ->
    {raft_server:reply_action(), delta() | undefined, state()}.
handle_command(_, _, {get, Key}, _, State) ->
    {{reply, do_get(Key, State)}, undefined, State};
handle_command(_, _, Put = {put, _, _}, _, State) ->
    {{reply, ok}, Put, State};
handle_command(_, _, Remove = {remove, _}, _, State) ->
    {{reply, ok}, Remove, State}.

-spec handle_info(_, _Info, _, state()) ->
    {undefined, state()}.
handle_info(_, Info, _, State) ->
    ok = error_logger:error_msg("unexpected info received: ~p", [Info]),
    {undefined, State}.

-spec apply_delta(_, raft_rpc:request_id(), delta(), _, state()) ->
    state().
apply_delta(_, _, {put, Key, Value}, _, State) ->
    do_put(Key, Value, State);
apply_delta(_, _, {remove, Key}, _, State) ->
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
