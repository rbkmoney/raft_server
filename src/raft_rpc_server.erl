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

-module(raft_rpc_server).

%% API
-export_type([handler    /0]).
-export_type([handler_ret/0]).
-export_type([timer      /0]).
-export_type([state      /0]).
-export_type([options    /0]).
-export([start_link          /3]).
-export([format_state        /1]).
-export([format_self_endpoint/1]).


%% internal
-export([init/4]).

% system callbacks
-export([system_code_change/4, system_continue/3, system_terminate/4]).

%%
%% API
%%
-type timeout_ms   () :: non_neg_integer().
-type timestamp_ms () :: non_neg_integer().
-type handler      () ::raft_utils:mod_opts().
-type handler_state() :: term().
-type timer        () :: timestamp_ms() | undefined.
-type handler_ret  () :: {[{raft_rpc:endpoint(), raft_rpc:message()}], timer(), handler_state()}.

-type options() :: #{
    rpc      := raft_rpc:rpc(),
    logger   := raft_rpc_logger:logger()
}.

-opaque state() :: #{
    parent        := pid(),
    reg_name      := raft_utils:gen_reg_name(),
    options       := options(),
    handler       := handler(),
    handler_state := handler_state(),
    timer         := timer()
}.

%%

-callback init(_) ->
    handler_ret().

-callback handle_timeout(_, timer(), handler_state()) ->
    handler_ret().

-callback handle_rpc_message(_, raft_rpc:endpoint(), raft_rpc:message(), timer(), handler_state()) ->
    handler_ret().

-callback handle_info(_, _Info, timer(), handler_state()) ->
    handler_ret().

-callback format_state(_, handler_state()) ->
    list().

%% TODO нужно убрать путём переноса self на уровень выше в rpc
-callback format_self_endpoint(_, handler_state()) ->
    list().

%%

-spec start_link(raft_utils:gen_reg_name(), handler(), options()) ->
   raft_utils:gen_start_ret().
start_link(RegName, Handler, Options) ->
    proc_lib:start_link(?MODULE, init, [self(), RegName, Handler, Options]).


-spec format_state(state()) ->
    list().
format_state(State) ->
    call_handler_format_state(State).

-spec format_self_endpoint(state()) ->
    list().
format_self_endpoint(State) ->
    call_handler_format_self_endpoint(State).

%%
%% gen_server callbacks
%%
-spec init(pid(), raft_utils:gen_reg_name(), handler(), options()) ->
   no_return().
init(Parent, RegName, Handler, Options) ->
    State = #{
        parent        => Parent,
        reg_name      => RegName,
        options       => Options,
        handler       => Handler,
        handler_state => undefined,
        timer         => undefined
    },
    Result = register(RegName),
    NewState = call_handler_init(State),
    ok = proc_lib:init_ack(Parent, Result),
    case Result of
        {ok, _} ->
            loop(NewState);
        {error, Reason} ->
            erlang:exit(Reason)
    end.

-spec register(raft_utils:gen_reg_name()) ->
    {ok, pid()} | {error, {already_started, pid()}}.
register(RegName) ->
    case raft_utils:gen_register_name(RegName) of
         true        -> {ok   , erlang:self()         };
        {false, Pid} -> {error, {already_started, Pid}}
    end.

-spec loop(state()) ->
    no_return().
loop(State = #{parent := Parent}) ->
    process_flag(trap_exit, true),
    Timeout = get_timer_timeout(State),
    receive
        {raft_rpc, From, Message} ->
            NewState = call_handler_handle_rpc_message(From, Message, State),
            ok = log_incoming_message(From, Message, State, NewState),
            loop(NewState);
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, [], State);
        {'EXIT', Parent, Reason} ->
            terminate(Reason, State);
        Info ->
            loop(call_handler_handle_info(Info, State))
    after Timeout ->
        NewState = call_handler_handle_timeout(State),
        ok = log_timeout(State, NewState),
        loop(NewState)
    end.

%%
%% system callbacks
%%
-spec system_code_change(state(), _, _, _) ->
    {ok, state()}.
system_code_change(State, _, _, _) ->
    {ok, State}.

-spec system_continue(pid(), _, state()) ->
    none().
system_continue(_, _, State) ->
    loop(State).

-spec system_terminate(_Reason, pid(), _Debug, state()) ->
    no_return().
system_terminate(Reason, _, _, State) ->
    terminate(Reason, State).

-spec terminate(_Reason, state()) ->
    no_return().
terminate(Reason, State) ->
    ok = log_error(Reason, State),
    exit(Reason).

-spec log_error(_Reason, state()) ->
    ok.
log_error(Reason, State = #{reg_name := RegName}) ->
    case terminate_status(Reason) of
        ok ->
            ok;
        error ->
            ok = error_logger:format(
                "** Raft server ~tp terminating~n"
                "** When Server state == ~ts~n"
                "** Reason for termination == ~n"
                "** ~tp~n", [RegName, format_state_safe(State), Reason])
    end.

-spec format_state_safe(state()) ->
    list().
format_state_safe(State = #{handler_state := HandlerState}) ->
    try
        call_handler_format_state(State)
    catch _:_ ->
        io_lib:format("~tp", [HandlerState])
    end.

-spec
terminate_status(_Reason     ) -> ok | error.
terminate_status(normal      ) -> ok;
terminate_status(shutdown    ) -> ok;
terminate_status({shutdown,_}) -> ok;
terminate_status(_           ) -> error.

%%
%% handler
%%
-spec call_handler_init(state()) ->
    state().
call_handler_init(State) ->
    call_handler(init, [], State).

-spec call_handler_handle_timeout(state()) ->
    state().
call_handler_handle_timeout(State = #{timer := Timer, handler_state := HandlerState}) ->
    call_handler(handle_timeout, [Timer, HandlerState], State).

-spec call_handler_handle_rpc_message(raft_rpc:endpoint(), raft_rpc:message(), state()) ->
    state().
call_handler_handle_rpc_message(From, Message, State = #{timer := Timer, handler_state := HandlerState}) ->
    call_handler(handle_rpc_message, [From, Message, Timer, HandlerState], State).

-spec call_handler_handle_info(_Info, state()) ->
    state().
call_handler_handle_info(Info, State = #{timer := Timer, handler_state := HandlerState}) ->
    call_handler(handle_info, [Info, Timer, HandlerState], State).

-spec call_handler_format_state(state()) ->
    list().
call_handler_format_state(#{handler := Handler, handler_state := HandlerState}) ->
    raft_utils:apply_mod_opts(Handler, format_state, [HandlerState]).

-spec call_handler_format_self_endpoint(state()) ->
    list().
call_handler_format_self_endpoint(#{handler := Handler, handler_state := HandlerState}) ->
   raft_utils:apply_mod_opts(Handler, format_self_endpoint, [HandlerState]).

-spec call_handler(atom(), list(), state()) ->
    state().
call_handler(Fun, Args, State = #{handler := Handler, options := #{rpc := RPC}}) ->
    try
        {MessagesToSend, NewTimer, NewHandlerState} =
            raft_utils:apply_mod_opts(Handler, Fun, Args),
        ok = lists:foreach(
                fun({From, To, Message}) ->
                    raft_rpc:send(RPC, From, To, Message)
                end,
                MessagesToSend
            ),
        State#{
            timer         => NewTimer,
            handler_state => NewHandlerState
        }
    catch Class:Reason ->
        terminate({'handler exception', {Class, Reason, erlang:get_stacktrace()}}, State)
    end.

%%
%% logging
%%
-spec log_timeout(state(), state()) ->
    ok.
log_timeout(StateBefore = #{options := #{logger := Logger}}, StateAfter) ->
    raft_rpc_logger:log(Logger, timeout, StateBefore, StateAfter).

-spec log_incoming_message(raft_rpc:endpoint(), raft_rpc:message(), state(), state()) ->
    ok.
log_incoming_message(From, Message, StateBefore = #{options := #{logger := Logger}}, StateAfter) ->
    raft_rpc_logger:log(Logger, {incoming_message, From, Message}, StateBefore, StateAfter).

%%

-spec get_timer_timeout(state()) ->
    timeout_ms().
get_timer_timeout(#{timer := TimerTimestampMS}) ->
    erlang:max(TimerTimestampMS - erlang:system_time(millisecond), 0).
