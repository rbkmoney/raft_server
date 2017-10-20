-module(raft_logger).

-export_type([logger/0]).
-export_type([event /0]).
-export([log/4]).

-type logger() :: raft_utils:mod_opts().
-type event() ::
      timeout
    | {incoming_message, raft_rpc:endpoint(), raft_rpc:message()}.

-callback log(_, event(), Before::raft:state(), After::raft:state()) ->
    ok.

-spec log(logger(), event(), Before::raft:state(), After::raft:state()) ->
    ok.
log(Logger, Event, StateBefore, StateAfter) ->
    _ = raft_utils:apply_mod_opts(Logger, log, [Event, StateBefore, StateAfter]),
    ok.
