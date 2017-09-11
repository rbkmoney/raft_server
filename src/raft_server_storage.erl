%%%
%%% Интерфейс для хранилища стейта.
%%%
-module(raft_server_storage).

-export_type([table  /0]).
-export_type([key    /0]).
-export_type([value  /0]).
-export_type([state  /0]).
-export_type([storage/0]).
-export([init   /1]).
-export([put    /4]).
-export([get    /4]).
-export([get_one/4]).

-type table  () :: main | log | fsm.
-type key    () :: term().
-type value  () :: term().
-type state  () :: term().
-type storage() :: mg_utils:mod_opts().

%%

-callback init(_Opts) ->
    state().

-callback put(_, table(), [{key(), value()}], state()) ->
    ok.

-callback get(_, table(), [key()], state()) ->
    [value()].

%%

-spec init(storage()) ->
    state().
init(Storage) ->
    mg_utils:apply_mod_opts(Storage, init, []).

-spec put(storage(), table(), [{key(), value()}], state()) ->
    ok.
put(Storage, Table, Values, State) ->
    mg_utils:apply_mod_opts(Storage, put, [Table, Values, State]).

-spec get(storage(), table(), [key()], state()) ->
    [value()].
get(Storage, Table, Keys, State) ->
    mg_utils:apply_mod_opts(Storage, get, [Table, Keys, State]).

-spec get_one(storage(), table(), key(), state()) ->
    value().
get_one(Storage, Table, Key, State) ->
    mg_utils:apply_mod_opts(Storage, get_one, [Table, Key, State]).
