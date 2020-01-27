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

%%%
%%% То, чего не хватает в OTP.
%%% TODO перенести в genlib
%%%
-module(raft_utils).

%% API
%% OTP
-export_type([reason                    /0]).
-export_type([gen_timeout               /0]).
-export_type([gen_start_ret             /0]).
-export_type([gen_ref                   /0]).
-export_type([gen_reg_name              /0]).
-export_type([gen_server_from           /0]).
-export_type([gen_server_init_ret       /1]).
-export_type([gen_server_handle_call_ret/1]).
-export_type([gen_server_handle_cast_ret/1]).
-export_type([gen_server_handle_info_ret/1]).
-export_type([gen_server_code_change_ret/1]).
-export_type([supervisor_ret            /0]).
-export([gen_where          /1]).
-export([gen_where_ref      /1]).
-export([gen_register_name  /1]).
-export([gen_unregister_name/1]).
-export([gen_send           /2]).
-export([get_msg_queue_len  /1]).

%% deadlines
-export_type([deadline/0]).
-export([timeout_to_deadline/1]).
-export([deadline_to_timeout/1]).
-export([is_deadline_reached/1]).
-export([default_deadline   /0]).

%% Other
-export_type([mod_opts/0]).
-export_type([mod_opts/1]).
-export([apply_mod_opts            /2]).
-export([apply_mod_opts            /3]).
-export([apply_mod_opts_if_defined /3]).
-export([apply_mod_opts_if_defined /4]).
-export([separate_mod_opts         /1]).

-export([throw_if_error    /1]).
-export([throw_if_error    /2]).
-export([throw_if_undefined/2]).
-export([exit_if_undefined /2]).

-export_type([exception   /0]).
-export([raise            /1]).
-export([format_exception /1]).

-export([join/2]).

-export_type([genlib_retry_policy/0]).
-export([genlib_retry_new/1]).

-export([stop_wait_all/3]).
-export([stop_wait    /3]).

-export([lists_random/1]).

%%
%% API
%% OTP
%%
-type reason() ::
      normal
    | shutdown
    | {shutdown, _}
    | _
.
-type gen_timeout() ::
      'hibernate'
    | timeout()
.

-type gen_start_ret() ::
      {ok, pid()}
    | ignore
    | {error, _}
.

-type gen_ref() ::
      atom()
    | {atom(), atom()}
    | {global, atom()}
    | {via, atom(), term()}
    | pid()
.
-type gen_reg_name() ::
      {local , atom()}
    | {global, term()}
    | {via, module(), term()}
.

-type gen_server_from() :: {pid(), _}.

-type gen_server_init_ret(State) ::
       ignore
    | {ok  , State   }
    | {stop, reason()}
    | {ok  , State   , gen_timeout()}
.

-type gen_server_handle_call_ret(State) ::
      {noreply, State   }
    | {noreply, State   , gen_timeout()}
    | {reply  , _Reply  , State        }
    | {stop   , reason(), State        }
    | {reply  , _Reply  , State        , gen_timeout()}
    | {stop   , reason(), _Reply       , State        }
.

-type gen_server_handle_cast_ret(State) ::
      {noreply, State   }
    | {noreply, State   , gen_timeout()}
    | {stop   , reason(), State        }
.

-type gen_server_handle_info_ret(State) ::
      {noreply, State   }
    | {noreply, State   , gen_timeout()}
    | {stop   , reason(), State        }
.

-type gen_server_code_change_ret(State) ::
      {ok   , State}
    | {error, _    }
.

-type supervisor_ret() ::
      ignore
    | {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}
.

-spec gen_where(gen_reg_name()) ->
    pid() | undefined.
gen_where({global, Name}) ->
    global:whereis_name(Name);
gen_where({via, Module, Name}) ->
    Module:whereis_name(Name);
gen_where({local, Name}) when is_atom(Name) ->
    erlang:whereis(Name).

-spec gen_where_ref(gen_ref()) ->
    pid() | undefined.
gen_where_ref({global, Name}) ->
    global:whereis_name(Name);
gen_where_ref({via, Module, Name}) ->
    Module:whereis_name(Name);
gen_where_ref(Name) when is_atom(Name) ->
    erlang:whereis(Name);
gen_where_ref({Node, Name}) when is_atom(Node) andalso is_atom(Name) ->
    exit(todo);
gen_where_ref(Pid) when is_pid(Pid) ->
    Pid.

-spec gen_register_name(gen_reg_name()) ->
    true | {false, pid()}.
gen_register_name({local, Name} = LN) ->
    try register(Name, self()) of
        true -> true
    catch
        error:_ ->
            {false, gen_where(LN)}
    end;
gen_register_name({global, Name} = GN) ->
    case global:register_name(Name, self()) of
        yes -> true;
        no  -> {false, gen_where(GN)}
    end;
gen_register_name({via, Module, Name} = GN) ->
    case Module:register_name(Name, self()) of
        yes -> true;
        no  -> {false, gen_where(GN)}
    end.

-spec gen_unregister_name(gen_reg_name()) ->
    ok.
gen_unregister_name({local, Name}) ->
    try unregister(Name) of
        _ -> ok
    catch
        _:_ -> ok
    end;
gen_unregister_name({global, Name}) ->
    _ = global:unregister_name(Name),
    ok;
gen_unregister_name({via, Mod, Name}) ->
    _ = Mod:unregister_name(Name),
    ok.

-spec gen_send(gen_ref(), Msg::term()) ->
    ok.
gen_send({global, Name}, Msg) ->
    _ = global:send(Name, Msg), ok;
gen_send({via, Module, Name}, Msg) ->
    _ = Module:send(Name, Msg), ok;
gen_send(Dest, Msg) ->
    _ = erlang:send(Dest, Msg), ok.

-spec get_msg_queue_len(gen_reg_name()) ->
    pos_integer() | undefined.
get_msg_queue_len(Name) ->
    Pid = exit_if_undefined(gen_where(Name), noproc),
    {message_queue_len, Len} = exit_if_undefined(erlang:process_info(Pid, message_queue_len), noproc),
    Len.

%%
%% deadlines
%%
-type deadline() :: undefined | pos_integer().

-spec timeout_to_deadline(timeout()) ->
    deadline().
timeout_to_deadline(infinity) ->
    undefined;
timeout_to_deadline(Timeout) ->
    now_ms() + Timeout.

-spec deadline_to_timeout(deadline()) ->
    timeout().
deadline_to_timeout(undefined) ->
    infinity;
deadline_to_timeout(Deadline) ->
    erlang:max(Deadline - now_ms(), 0).

-spec is_deadline_reached(deadline()) ->
    boolean().
is_deadline_reached(undefined) ->
    false;
is_deadline_reached(Deadline) ->
    Deadline - now_ms() =< 0.

-spec default_deadline() ->
    deadline().
default_deadline() ->
    timeout_to_deadline(5000).

%%

-spec now_ms() ->
    pos_integer().
now_ms() ->
    erlang:system_time(1000).

%%
%% Other
%%
-type mod_opts() :: mod_opts(term()).
-type mod_opts(Options) :: {module(), Options} | module().

-spec apply_mod_opts(mod_opts(), atom()) ->
    _Result.
apply_mod_opts(ModOpts, Function) ->
    apply_mod_opts(ModOpts, Function, []).

-spec apply_mod_opts(mod_opts(), atom(), list(_Arg)) ->
    _Result.
apply_mod_opts(ModOpts, Function, Args) ->
    {Mod, Arg} = separate_mod_opts(ModOpts),
    erlang:apply(Mod, Function, [Arg | Args]).

-spec apply_mod_opts_if_defined(mod_opts(), atom(), _Default) ->
    _Result.
apply_mod_opts_if_defined(ModOpts, Function, Default) ->
    apply_mod_opts_if_defined(ModOpts, Function, Default, []).

-spec apply_mod_opts_if_defined(mod_opts(), atom(), _Default, list(_Arg)) ->
    _Result.
apply_mod_opts_if_defined(ModOpts, Function, Default, Args) ->
    {Mod, Arg} = separate_mod_opts(ModOpts),
    FunctionArgs = [Arg | Args],
    case erlang:function_exported(Mod, Function, length(FunctionArgs)) of
        true ->
            erlang:apply(Mod, Function, FunctionArgs);
        false ->
            Default
    end.

-spec separate_mod_opts(mod_opts()) ->
    {module(), _Arg}.
separate_mod_opts(ModOpts={_, _}) ->
    ModOpts;
separate_mod_opts(Mod) ->
    {Mod, undefined}.

-spec throw_if_error
    (ok             ) -> ok;
    ({ok   , Result}) -> Result;
    ({error, _Error}) -> no_return().
throw_if_error(ok) ->
    ok;
throw_if_error({ok, R}) ->
    R;
throw_if_error({error, Error}) ->
    erlang:throw(Error).

-spec throw_if_error
    (ok             , _ExceptionTag) -> ok;
    ({ok   , Result}, _ExceptionTag) -> Result;
    ({error, _Error}, _ExceptionTag) -> no_return().
throw_if_error(ok, _) ->
    ok;
throw_if_error({ok, R}, _) ->
    R;
throw_if_error(error, Exception) ->
    erlang:throw(Exception);
throw_if_error({error, Error}, Exception) ->
    erlang:throw({Exception, Error}).

-spec throw_if_undefined(Result, _Reason) ->
    Result | no_return().
throw_if_undefined(undefined, Reason) ->
    erlang:throw(Reason);
throw_if_undefined(Value, _) ->
    Value.

-spec exit_if_undefined(Result, _Reason) ->
    Result.
exit_if_undefined(undefined, Reason) ->
    erlang:exit(Reason);
exit_if_undefined(Value, _) ->
    Value.

-type exception() :: {exit | error | throw, term(), list()}.

-spec raise(exception()) ->
    no_return().
raise({Class, Reason, Stacktrace}) ->
    erlang:raise(Class, Reason, Stacktrace).

-spec format_exception(exception()) ->
    iodata().
format_exception({Class, Reason, Stacktrace}) ->
    io_lib:format("~s:~p ~s", [Class, Reason, genlib_format:format_stacktrace(Stacktrace, [newlines])]).


-spec join(D, list(E)) ->
    list(D | E).
join(_    , []   ) -> [];
join(_    , [H]  ) ->  H;
join(Delim, [H|T]) -> [H, Delim, join(Delim, T)].

-type retries_num() :: pos_integer() | infinity.
-type genlib_retry_policy() ::
      {linear, retries_num() | {max_total_timeout, pos_integer()}, pos_integer()}
    | {exponential, retries_num() | {max_total_timeout, pos_integer()}, number(), pos_integer()}
    | {exponential, retries_num() | {max_total_timeout, pos_integer()}, number(), pos_integer(), timeout()}
    | {intervals, [pos_integer(), ...]}
    | {timecap, timeout(), genlib_retry_policy()}
.
-spec genlib_retry_new(genlib_retry_policy()) ->
    genlib_retry:strategy().
genlib_retry_new({linear, Retries, Timeout}) ->
    genlib_retry:linear(Retries, Timeout);
genlib_retry_new({exponential, Retries, Factor, Timeout}) ->
    genlib_retry:exponential(Retries, Factor, Timeout);
genlib_retry_new({exponential, Retries, Factor, Timeout, MaxTimeout}) ->
    genlib_retry:exponential(Retries, Factor, Timeout, MaxTimeout);
genlib_retry_new({intervals, Array}) ->
    genlib_retry:intervals(Array);
genlib_retry_new({timecap, Timeout, Policy}) ->
    genlib_retry:timecap(Timeout, genlib_retry_new(Policy));
genlib_retry_new(BadPolicy) ->
    erlang:error(badarg, [BadPolicy]).

-spec stop_wait_all([pid()], _Reason, timeout()) ->
    ok.
stop_wait_all(Pids, Reason, Timeout) ->
    lists:foreach(
        fun(Pid) ->
            case stop_wait(Pid, Reason, Timeout) of
                ok      -> ok;
                timeout -> exit({timeout, {?MODULE, stop_wait_all, [Pids, Reason, Timeout]}})
            end
        end,
        Pids
    ).

-spec stop_wait(pid(), _Reason, timeout()) ->
    ok | timeout.
stop_wait(Pid, Reason, Timeout) ->
    OldTrap = process_flag(trap_exit, true),
    erlang:exit(Pid, Reason),
    R =
        receive
            {'EXIT', Pid, Reason} -> ok
        after
            Timeout -> timeout
        end,
    process_flag(trap_exit, OldTrap),
    R.

-spec lists_random(list(T)) ->
    T.
lists_random(List) ->
    lists:nth(rand:uniform(length(List)), List).
