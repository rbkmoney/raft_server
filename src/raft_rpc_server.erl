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

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

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
    rpc      => raft_rpc:rpc(),
    logger   => raft_rpc_logger:logger()
}.

-opaque state() :: #{
    options       => options(),
    handler       => handler(),
    handler_state => handler_state(),
    timer         => timer()
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
    gen_server:start_link(RegName, ?MODULE, {Handler, Options}, []).

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
-spec init(_) ->
   raft_utils:gen_server_init_ret(state()).
init({Handler, Options}) ->
    State = #{
        options       => Options,
        handler       => Handler,
        handler_state => undefined,
        timer         => undefined
    },
    NewState = call_handler_init(State),
    {ok, NewState, get_timer_timeout(NewState)}.

-spec handle_call(_Call,raft_utils:gen_server_from(), state()) ->
   raft_utils:gen_server_handle_call_ret(state()).
handle_call(_, _From, State) ->
    {noreply, State, get_timer_timeout(State)}.

-spec handle_cast(_, state()) ->
   raft_utils:gen_server_handle_cast_ret(state()).
handle_cast(_, State) ->
    {noreply, State, get_timer_timeout(State)}.

-spec handle_info(_Info, state()) ->
   raft_utils:gen_server_handle_info_ret(state()).
handle_info(timeout, State) ->
    NewState = call_handler_handle_timeout(State),
    ok = log_timeout(State, NewState),
    {noreply, NewState, get_timer_timeout(NewState)};
handle_info({raft_rpc, From, Message}, State) ->
    NewState = call_handler_handle_rpc_message(From, Message, State),
    ok = log_incoming_message(From, Message, State, NewState),
    {noreply, NewState, get_timer_timeout(NewState)};
handle_info(Info, State) ->
    NewState = call_handler_handle_info(Info, State),
    {noreply, NewState, get_timer_timeout(NewState)}.

-spec code_change(_, state(), _) ->
   raft_utils:gen_server_code_change_ret(state()).
code_change(_, State, _) ->
    {ok, State}.

-spec terminate(_Reason, state()) ->
    ok.
terminate(_, _) ->
    ok.

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
    {MessagesToSend, NewTimer, NewHandlerState} =raft_utils:apply_mod_opts(Handler, Fun, Args),
    ok = lists:foreach(
            fun({From, To, Message}) ->
                raft_rpc:send(RPC, From, To, Message)
            end,
            MessagesToSend
        ),
    State#{
        timer         => NewTimer,
        handler_state => NewHandlerState
    }.

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
