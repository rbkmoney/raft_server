-module(raft_logger_io_plant_uml).

%% raft_logger
-behaviour(raft_logger).
-export([log/4]).

%% хочется больше эвентов в seq_dia и чтобы они были в одном файле
%%  - название теста
%%  - создание/удаление элементов
%%  - шедулинг таймера

% рендер http://www.plantuml.com/plantuml/uml/
-spec log(_, raft_logger:event(), raft:state(), raft:state()) ->
    ok.
log(_, timeout, StateBefore, StateAfter) ->
    io:format("->\"~s\" : timeout~n~s", [raft:format_self_endpoint(StateBefore), format_state_transition(StateBefore, StateAfter)]);
log(_, {incoming_message, From, Message}, StateBefore, StateAfter) ->
    io:format("\"~s\"->\"~s\" : ~s~n~s",
        [raft_rpc:format_endpoint(From), raft:format_self_endpoint(StateBefore),
            raft_rpc:format_message(Message), format_state_transition(StateBefore, StateAfter)]);
log(_, {incoming_message, Message}, StateBefore, StateAfter) ->
    io:format("->\"~s\" : ~s~n~s",
        [raft:format_self_endpoint(StateBefore), raft_rpc:format_message(Message), format_state_transition(StateBefore, StateAfter)]).

-spec format_state_transition(raft:state(), raft:state()) ->
    list().
format_state_transition(StateBefore, StateAfter) ->
    io_lib:format("note left of \"~s\"~n\t~s~n\t~s~nend note",
        [raft:format_self_endpoint(StateBefore), raft:format_state(StateBefore), raft:format_state(StateAfter)]).
