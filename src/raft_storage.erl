%%%
%%% Интерфейс для хранилища стейта.
%%%
-module(raft_storage).

-export_type([type   /0]).
-export_type([key    /0]).
-export_type([value  /0]).
-export_type([state  /0]).
-export_type([storage/0]).
-export([init   /2]).
-export([put    /3]).
-export([get    /3]).
-export([get_one/3]).

-type type   () :: system | log | handler.
-type key    () :: term().
-type value  () :: term().
-type state  () :: term().
-type storage() :: mg_utils:mod_opts().

%%

-callback init(_Opts, type()) ->
    state().

-callback put(_, [{key(), value()}], state()) ->
    state().

-callback get(_, [key()], state()) ->
    [value()].

-callback get_one(_, key(), state()) ->
    value() | undefined.

%%

-spec init(storage(), type()) ->
    state().
init(Storage, Type) ->
    mg_utils:apply_mod_opts(Storage, init, [Type]).

-spec put(storage(), [{key(), value()}], state()) ->
    state().
put(Storage, Values, State) ->
    mg_utils:apply_mod_opts(Storage, put, [Values, State]).

-spec get(storage(), [key()], state()) ->
    [value()].
get(Storage, Keys, State) ->
    mg_utils:apply_mod_opts(Storage, get, [Keys, State]).

-spec get_one(storage(), key(), state()) ->
    value().
get_one(Storage, Key, State) ->
    mg_utils:apply_mod_opts(Storage, get_one, [Key, State]).
