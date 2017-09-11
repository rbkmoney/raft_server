%%
%% Хранилище в памяти.
%% Лог, хранится в виде списка.
%%
-module(raft_server_storage_memory).
-behaviour(raft_server_storage).

%% raft_server_storage
-export([init   /1]).
-export([put    /4]).
-export([get    /4]).
-export([get_one/4]).

-type state() :: #{
    raft_server_storage:table() => #{
        raft_server_storage:key() => raft_server_storage:value()
    }
}.

%%
%% raft_server_storage
%%
-spec init(_) ->
    state().
init(_) ->
    #{}.

-spec put(_, raft_server_storage:table(), [{raft_server_storage:key(), raft_server_storage:value()}], state()) ->
    ok.
put(_, TableName, Values, State) ->
    Table    = maps:get(TableName, State, #{}),
    NewTable = maps:merge(Table, maps:from_list(Values)),
    State#{TableName => NewTable}.

-spec get(_, raft_server_storage:table(), [raft_server_storage:key()], state()) ->
    [raft_server_storage:value()].
get(_, TableName, Keys, State) ->
    Table = maps:get(TableName, State, #{}),
    lists:map(
        fun(Key) ->
            maps:get(Key, Table, undefined)
        end,
        Keys
    ).

-spec get_one(_, raft_server_storage:table(), raft_server_storage:key(), state()) ->
    raft_server_storage:value().
get_one(_, TableName, Key, State) ->
    Table = maps:get(TableName, State, #{}),
    maps:get(Key, Table, undefined).
