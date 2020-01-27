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

-module(raft_rpc_logger).

-export_type([logger/0]).
-export_type([event /0]).
-export([log/4]).

-type logger() :: raft_utils:mod_opts() | undefined.
-type event() ::
      timeout
    | {incoming_message, raft_rpc:endpoint(), raft_rpc:message()}.

-callback log(_, event(), Before::raft_rpc_server:state(), After::raft_rpc_server:state()) ->
    ok.

-spec log(logger(), event(), raft_rpc_server:state(), raft_rpc_server:state()) ->
    ok.
log(undefined, _, _, _) ->
    ok;
log(Logger, Event, StateBefore, StateAfter) ->
    _ = raft_utils:apply_mod_opts(Logger, log, [Event, StateBefore, StateAfter]),
    ok.
