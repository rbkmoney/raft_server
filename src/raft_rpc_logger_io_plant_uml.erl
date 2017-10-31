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

-module(raft_rpc_logger_io_plant_uml).

%% raft_rpc_logger
-behaviour(raft_rpc_logger).
-export([log/4]).

%% хочется больше эвентов в seq_dia и чтобы они были в одном файле
%%  - название теста
%%  - создание/удаление элементов
%%  - шедулинг таймера

% рендер http://www.plantuml.com/plantuml/uml/
-spec log(_, raft_rpc_logger:event(), raft_rpc_server:state(), raft_rpc_server:state()) ->
    ok.
log(_, timeout, StateBefore, StateAfter) ->
    io:format("->\"~s\" : timeout~n~s", [raft_rpc_server:format_self_endpoint(StateBefore), format_state_transition(StateBefore, StateAfter)]);
log(_, {incoming_message, From, Message}, StateBefore, StateAfter) ->
    io:format("\"~s\"->\"~s\" : ~s~n~s",
        [raft_rpc:format_endpoint(From), raft_rpc_server:format_self_endpoint(StateBefore),
            raft_rpc:format_message(Message), format_state_transition(StateBefore, StateAfter)]);
log(_, {incoming_message, Message}, StateBefore, StateAfter) ->
    io:format("->\"~s\" : ~s~n~s",
        [raft_rpc_server:format_self_endpoint(StateBefore), raft_rpc:format_message(Message), format_state_transition(StateBefore, StateAfter)]).

-spec format_state_transition(raft_rpc_server:state(), raft_rpc_server:state()) ->
    list().
format_state_transition(StateBefore, StateAfter) ->
    io_lib:format("note left of \"~s\"~n\t~s~n\t~s~nend note~n",
        [raft_rpc_server:format_self_endpoint(StateBefore), raft_rpc_server:format_state(StateBefore), raft_rpc_server:format_state(StateAfter)]).

