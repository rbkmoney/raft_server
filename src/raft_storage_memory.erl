%%
%% Хранилище в памяти.
%% Лог, хранится в виде списка.
%%
-module(raft_storage_memory).
-behaviour(raft_storage).

%% raft_storage
-export([init   /2]).
-export([put    /3]).
-export([get    /3]).
-export([get_one/3]).
-export([remove /3]).

-type state() :: #{
    raft_storage:key() => raft_storage:value()
}.

%%
%% raft_storage
%%
-spec init(_, raft_storage:type()) ->
    state().
init(_, _) ->
    #{}.

-spec put(_, [{raft_storage:key(), raft_storage:value()}], state()) ->
    state().
put(_, Values, State) ->
    maps:merge(State, maps:from_list(Values)).

-spec get(_, [raft_storage:key()], state()) ->
    [raft_storage:value()].
get(_, Keys, State) ->
    Values = [maps:get(Key, State, undefined) || Key <- Keys],
    [Value || Value <- Values, Value =/= undefined].

-spec get_one(_, raft_storage:key(), state()) ->
    raft_storage:value() | undefined.
get_one(_, Key, State) ->
    maps:get(Key, State, undefined).

-spec remove(_, [raft_storage:key()], state()) ->
    state().
remove(_, Keys, State) ->
    maps:without(Keys, State).
