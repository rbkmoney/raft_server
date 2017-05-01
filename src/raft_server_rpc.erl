-module(raft_server_rpc).

-export_type([endpoint    /0]).
-export_type([message     /0]).
-export_type([message_type/0]).
-export_type([message_body/0]).
-export_type([state       /0]).
-export([init/2]).
-export([send/4]).
-export([recv/3]).

-type endpoint() :: term().
-type message() :: {
    request | {response, _Success::boolean()},
    _Type       ::message_type(),
    _Body       ::message_body(),
    _CurrentTerm,
    _From       ::endpoint()
}.
-type message_type() ::
    %  command
      request_vote
    | append_entries
.
-type message_body() :: term().
-type state() :: term().

%%

-callback init(_Args) ->
    state().

-callback send(endpoint(), message(), state()) ->
    state().

-callback recv(_Data, state()) ->
    state() | invalid_data.

%%

-spec init(module(), _Args) ->
    state().
init(Module, Args) ->
    Module:init(Args).

-spec send(module(), endpoint(), message(), state()) ->
    state().
send(Module, To, Message, State) ->
    Module:send(To, Message, State).

-spec recv(module(), _, state()) ->
    {message(), state()} | invalid_data.
recv(Module, Info, State) ->
    Module:recv(Info, State).
