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

-module(raft_skeleton).

%% API
-export([start_link   /2]).
-export([sync_command /1]).
-export([async_command/1]).

%% raft_server
-behaviour(raft_server).
-export([init/1, handle_election/2, handle_command/4, handle_async_command/4, handle_info/3, apply_delta/4]).

%%
%% API
%%
-spec start_link(raft_utils:gen_reg_name(), raft_server:options()) ->
    raft_utils:gen_start_ret().
start_link(RegName, RaftOptions) ->
    raft_server:start_link(RegName, ?MODULE, RaftOptions).

-spec sync_command(raft_server:options()) ->
    ok.
sync_command(#{rpc := RPC, cluster := Cluster}) ->
    raft_server:send_command(
        RPC,
        Cluster,
        undefined,
        sync_command,
        genlib_retry:linear(10, 100)
    ).

-spec async_command(raft_server:options()) ->
    ok.
async_command(#{rpc := RPC, cluster := Cluster}) ->
    raft_server:send_async_command(
        RPC,
        Cluster,
        undefined,
        async_command,
        genlib_retry:linear(10, 100)
    ).

%%
%% raft_server
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
    {raft_server:reply_action(), state()}.
handle_async_command(_, _, async_command, state) ->
    {{reply, ok}, state}.

-spec handle_command(raft_utils:gen_ref(), raft_rpc:request_id(), sync_command(), state()) ->
    {raft_server:reply_action(), delta() | undefined, state()}.
handle_command(_, _, sync_command, state) ->
    {{reply, ok}, delta, state}.

-spec handle_info(_, _Info, state()) ->
    {undefined, state()}.
handle_info(_, _Info, state) ->
    {undefined, state}.

-spec apply_delta(_, raft_rpc:request_id(), delta(), state()) ->
    state().
apply_delta(_, _, delta, state) ->
    state.
