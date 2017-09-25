%%
%% Тут попытка получить семантику аналогичную gen_server.
%%
-module(raft_server).

%% API
-export_type([handler      /0]).
-export_type([handler_state/0]).
-export_type([reply_action /0]).
-export([start_link/3]).
-export([sync_call /5]).
-export([reply     /4]).

%% raft
-behaviour(raft).
-export([handle_sync_command /5]).
-export([handle_async_command/5]).

%%
%% API
%%
-type handler() :: mg_utils:mod_opts().
-type handler_state() :: term().
-type reply_action() :: {reply, _Reply} | noreply.

%%

% -callback init(_) ->
%     raft_storage:state().

-callback handle_leader_sync_call(_, raft_rpc:endpoint() | undefined, raft_rpc:request_id(), _Call, handler_state()) ->
    {reply_action(), handler_state()}.

-callback handle_follower_sync_call(_, raft_rpc:request_id(), _Call, handler_state()) ->
    handler_state().

% -callback handle_async_call(raft_rpc:request_id(), _From, _Call, state()) ->
%     {reply, _Reply} | noreply.

% -callback handle_sync_cast(raft_rpc:request_id(), _From, _Call, state()) ->
%     {reply, _Reply} | noreply.

% -callback handle_async_cast(raft_rpc:request_id(), _From, _Call, state()) ->
%     {reply, _Reply} | noreply.

% -callback handle_info(_Info, state()) ->
%     noreply.

%%

-spec start_link(mg_utils:gen_reg_name(), handler(), raft:options()) ->
    mg_utils:gen_start_ret().
start_link(RegName, Handler, Options) ->
    raft:start_link(RegName, {?MODULE, Handler}, Options).

-spec sync_call(raft_rpc:rpc(), [raft_rpc:endpoint()], raft_rpc:request_id(), raft_storage:command(), genlib_retry:strategy()) ->
    ok.
sync_call(RPC, Cluster, ID, Call, Retry) ->
    request(RPC, Cluster, ID, Call, Retry).

% -spec async_call(raft_rpc:rpc(), [raft_rpc:endpoint()], raft_storage:command()) ->
%     ok.
% async_call(RPC, Cluster, ID, Call) ->
%     request(RPC, Cluster, {external, {async_call, Call}}).

-spec reply(raft_rpc:rpc(), raft_rpc:endpoint(), raft_rpc:request_id(), raft_storage:command()) ->
    ok.
reply(RPC, To, ID, Call) ->
    ok = raft:send_response_command(RPC, raft_rpc:self(RPC), To, ID, Call).

%%

-spec request(raft_rpc:rpc(), [raft_rpc:endpoint()], [raft_rpc:endpoint()], raft_rpc:request_id(), _, genlib_retry:strategy()) ->
    term().
request(RPC, Cluster = [], ID, Call, Retry) ->
    erlang:error(badarg, [RPC, ID, Call, Cluster, Retry]);
request(RPC, Cluster, ID, Call, Retry) ->
    request(RPC, Cluster, Cluster, ID, Call, Retry).

-spec request(raft_rpc:rpc(), raft_rpc:request_id(), _, [raft_rpc:endpoint()], [raft_rpc:endpoint()]) ->
    term().
request(RPC, [], AllCluster, ID, Call, Retry) ->
    request(RPC, AllCluster, AllCluster, ID, Call, Retry);
request(RPC, Cluster, AllCluster, ID, Call, Retry) ->
    To = raft_rpc:get_nearest(RPC, Cluster),
    ok = raft:send_sync_command(RPC, raft_rpc:self(RPC), To, ID, Call),
    case genlib_retry:next_step(Retry) of
        {wait, Timeout, NewRetry} ->
            case raft:recv_response_command(RPC, ID, Timeout) of
                {ok, _Leader, Value} ->
                    Value;
                timeout ->
                    request(RPC, Cluster -- [To], AllCluster, ID, Call, NewRetry)
            end;
        finish ->
            % TODO сделать аналогично gen_server
            erlang:error({timeout, Cluster, ID, Call})
    end.

%%

-spec handle_async_command(_, raft_rpc:endpoint(), raft_rpc:request_id(), raft_storage:command(), raft:ext_state()) ->
    ok.
handle_async_command(_, _From, _ID, _, _) ->
    % TODO
    ok.

-spec handle_sync_command(handler(), raft_rpc:endpoint() | undefined, raft_rpc:request_id(), _, raft:ext_state()) ->
    ok.
handle_sync_command(Handler, From, ID, Call, #{options := #{rpc := RPC, storage := Storage}, storage_states := #{handler := State}, role := leader}) ->
    {ReplyAction, NewHandlerState} =
        handle_leader_sync_call(Handler, From, ID, Call, raft_storage:get_one(Storage, state, State)),
    case ReplyAction of
        {reply, Reply} -> ok = raft:send_response_command(RPC, raft_rpc:self(RPC), From, ID, Reply);
        noreply        -> ok
    end,
    raft_storage:put(Storage, [{state, NewHandlerState}], State);
handle_sync_command(Handler, _From, ID, Call, #{options := #{storage := Storage}, storage_states := #{handler := State}, role := {follower, _}}) ->
    NewHandlerState = handle_follower_sync_call(Handler, ID, Call, raft_storage:get_one(Storage, state, State)),
    raft_storage:put(Storage, [{state, NewHandlerState}], State).

%%

-spec handle_leader_sync_call(handler(), raft_rpc:endpoint() | undefined, raft_rpc:request_id(), _, handler_state()) ->
    {reply_action(), handler_state()}.
handle_leader_sync_call(Handler, From, ID, Call, State) ->
    mg_utils:apply_mod_opts(Handler, handle_leader_sync_call, [From, ID, Call, State]).

-spec handle_follower_sync_call(handler(), raft_rpc:request_id(), _, handler_state()) ->
    handler_state().
handle_follower_sync_call(Handler, ID, Call, State) ->
    mg_utils:apply_mod_opts(Handler, handle_follower_sync_call, [ID, Call, State]).

