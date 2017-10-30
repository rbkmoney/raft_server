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
%%% Основная идея в том, чтобы сделать максимально лёгкую в стиле OTP реализацию Raft.
%%% В процессе написания была попытка усидеть на 3х стульях сразу:
%%%  - с одной стороны по максимуму придерживаться терминологии орининального документа (https://raft.github.io/raft.pdf);
%%%  - с другой делать в стили OTP.
%%%  - с третьей сделать наиболее обобщённо и гибко
%%%
%%% TODO:
%%%  - обязательное:
%%%   - ответить всем
%%%  - доработки:
%%%   - сделать регистрацию в RPC
%%%   - убрать gen_server и переделать на proc_lib
%%%   - timeout на хендлер
%%%   - привести в порядок таймауты запросов к кластеру
%%%   - лимит на длинну очереди команд
%%%   - компактизация стейта и оптимизация наливки свежего елемента группы
%%%   - внешние сериализаторы для rpc и msgpack реализация
%%%   - асинхронная обработка запроса с чеком лидерства
%%%   - сессия обращения
%%%   - ресайз кластера
%%%   -
%%%  - проблемы:
%%%   - нет обработки потери лидерства (а такое возможно) (нужно добавить колбек на это переход)
%%%   - не удалять последующие элементы лога если нет конфликта (переделать логику добавления в лог)
%%%   - репликация команды из прошлой эпохи (что это?)
%%%   - нет проверки лидерства при обработке команды (нужно сделать проверку фиктивным коммитом)
%%%   - неправильная работа с next/match index (переделать логику репликации)
%%%   -
%%%  - рефакторинг:
%%%   - вынести отдельно raft_rpc_server и переименовать в raft_server
%%%   - переделать работу со storage (и придумать как, но то, что есть — хрень :-\ )
%%%   - убрать raft_utils и перенести всё в genlib
%%%
-module(raft).

%% API
-export_type([timeout_ms   /0]).
-export_type([timestamp_ms /0]).
-export_type([options      /0]).
-export_type([raft_term    /0]).
-export_type([index        /0]).
-export_type([command      /0]).
-export_type([delta        /0]).
-export_type([maybe_delta  /0]).
-export_type([reply        /0]).
-export_type([reply_action /0]).
-export_type([log_entry    /0]).
-export_type([handler      /0]).
-export_type([handler_state/0]).
-export_type([storage      /0]).
-export_type([state        /0]).
-export([start_link           /3]).
-export([send_command         /5]).
-export([send_async_command   /5]).
-export([format_state         /1]).
-export([format_self_endpoint /1]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% API
%%
-type timeout_ms  () :: non_neg_integer().
-type timestamp_ms() :: non_neg_integer().

-type options() :: #{
    % состав рафт-группы
    self              => raft_rpc:endpoint(),
    cluster           => ordsets:ordset(raft_rpc:endpoint()),

    % таймауты raft протокола (это важная настройка!)
    % election_timeout << broadcast_timeout << mean_time_between_failures
    % election_timeout ~ 2 * broadcast_timeout
    election_timeout  => timeout_ms() | {From::timeout_ms(), To::timeout_ms()},
    broadcast_timeout => timeout_ms(),

    % хранилище состояния (TODO нужно переделать)
    storage           => raft_storage:storage(),

    % протокол общения между серверами
    rpc               => raft_rpc:rpc(),

    % логгирование эвентов
    logger            => raft_logger:logger(),

    random_seed       := {integer(), integer(), integer()} | undefined
}.

-type raft_term    () :: non_neg_integer().
-type index        () :: non_neg_integer().
-type command      () :: _.
-type delta        () :: _.
-type maybe_delta  () :: delta() | undefined.
-type reply        () :: _.
-type log_entry    () :: {raft_term(), raft_rpc:request_id(), delta()}.
-type handler      () :: raft_utils:mod_opts().
-type handler_state() :: _.
-type reply_action () :: {reply, reply()} | noreply.

-type storage() :: #{
    system  => raft_storage:state(),
    log     => raft_storage:state()
}.
-type ext_role() :: leader | {follower, undefined | raft_rpc:endpoint()} | candidate.


%%
%% behaviour
%%
-callback init(_) ->
    handler_state().

-callback handle_election(_, handler_state()) ->
    {maybe_delta(), handler_state()}.

-callback handle_async_command(_, raft_rpc:request_id(), command(), handler_state()) ->
    {reply_action(), handler_state()}.

-callback handle_command(_, raft_rpc:request_id(), command(), handler_state()) ->
    {reply_action(), maybe_delta(), handler_state()}.

%% only leader
-callback handle_info(_, _Info, handler_state()) ->
    {maybe_delta(), handler_state()}.

%% применение происходит только после консенсусного принятия этого изменения
-callback apply_delta(_, raft_rpc:request_id(), delta(), handler_state()) ->
    handler_state().

%%

%% Версия без регистрации не имеет смысла (или я не прав? похоже, что прав, работа по пидам смысла не имеет).
%% А как же общение по RPC? Тут похоже обратное — версия с регистрацией не нужна
%% (но нужно всё-таки как-то зарегистрировать процесс при erl rpc).
-spec start_link(raft_utils:gen_reg_name(), handler(), options()) ->
    raft_utils:gen_start_ret().
start_link(RegName, Handler, Options) ->
    gen_server:start_link(RegName, ?MODULE, {Handler, Options}, []).

%% TODO sessions
-spec send_command(raft_rpc:rpc(), [raft_rpc:endpoint()], raft_rpc:request_id() | undefined, command(), genlib_retry:strategy()) ->
    term().
send_command(RPC, Cluster, ID, Command, Retry) ->
    send_ext_command(RPC, Cluster, ID, {command, Command}, Retry).

-spec send_async_command(raft_rpc:rpc(), [raft_rpc:endpoint()], raft_rpc:request_id() | undefined, command(), genlib_retry:strategy()) ->
    term().
send_async_command(RPC, Cluster, ID, Command, Retry) ->
    send_ext_command(RPC, Cluster, ID, {async_command, Command}, Retry).

%%

-spec send_ext_command(raft_rpc:rpc(), [raft_rpc:endpoint()], raft_rpc:request_id() | undefined, _, genlib_retry:strategy()) ->
    term().
send_ext_command(RPC, Cluster = [], ID, Command, Retry) ->
    erlang:error(badarg, [RPC, ID, Command, Cluster, Retry]);
send_ext_command(RPC, Cluster, undefined, Command, Retry) ->
    send_ext_command(RPC, Cluster, generate_request_id(), Command, Retry);
send_ext_command(RPC, Cluster, ID, Command, Retry) ->
    send_ext_command(RPC, Cluster, Cluster, ID, Command, Retry).

-spec generate_request_id() ->
    raft_rpc:request_id().
generate_request_id() ->
    rand:uniform(1000000).

-spec send_ext_command(raft_rpc:rpc(), [raft_rpc:endpoint()], [raft_rpc:endpoint()], raft_rpc:request_id(), command(), genlib_retry:strategy()) ->
    term().
send_ext_command(RPC, [], AllCluster, ID, Command, Retry) ->
    send_ext_command(RPC, AllCluster, AllCluster, ID, Command, Retry);
send_ext_command(RPC, Cluster, AllCluster, ID, Command, Retry) ->
    To = raft_rpc:get_nearest(RPC, Cluster),
    ok = raft_rpc:send(RPC, raft_rpc:get_reply_endpoint(RPC), To, {external, ID, Command}),
    case genlib_retry:next_step(Retry) of
        {wait, Timeout, NewRetry} ->
            case recv_response_command(RPC, ID, Timeout) of
                {ok, _Leader, Value} ->
                    Value;
                timeout ->
                    send_ext_command(RPC, Cluster -- [To], AllCluster, ID, Command, NewRetry)
            end;
        finish ->
            erlang:error({timeout, AllCluster, ID, Command})
    end.

-spec recv_response_command(raft_rpc:rpc(), raft_rpc:request_id(), timeout()) ->
    {ok, raft_rpc:endpoint(), command()} | timeout.
recv_response_command(RPC, ID, Timeout) ->
    receive
        {raft_rpc, From, Data} ->
            case raft_rpc:recv(RPC, Data) of
                {external, ID, {response_command, Command}} ->
                    {ok, From, Command};
                _ ->
                    recv_response_command(RPC, ID, Timeout)
            end
    after Timeout ->
        timeout
    end.

%%
%% gen_server callbacks
%%
-record(follower_state, {
    heartbeat    :: timestamp_ms(),
    rpc_timeout  :: timestamp_ms(),
    next_index   :: index(),
    match_index  :: index(),
    commit_index :: index()
}).
-type follower_state () :: #follower_state{}.
-type followers_state() ::#{raft_rpc:endpoint() => follower_state()}.
-type role() ::
      {leader   , followers_state()}
    | {follower , MyLeader::(raft_rpc:endpoint() | undefined)}
    | {candidate, VotedFrom::ordsets:ordset(raft_rpc:endpoint())}
.

-opaque state() :: #{
    options => options(),
    timer => timestamp_ms() | undefined,

    % текущая роль и специфичные для неё данные
    role => role(),

    % текущий терм (вообще, имхо, слово "эпоха" тут более подходящее)
    current_term  => raft_term(),
    handler       => handler(),
    handler_state => handler_state(),

    % состояние хранилища
    storage_states => storage(),

    % все полученные запросы, которые ожидают ответа
    commands => [{raft_rpc:request_id(), raft_rpc:endpoint(), command()}],

    % ответ на последний обработанный запрос
    reply => {raft_rpc:endpoint(), raft_rpc:request_id(), reply()} | undefined
}.

-spec init({handler(), options()}) ->
    raft_utils:gen_server_init_ret(state()).
init(Args = {_, Options}) ->
    NewState = new_state(Args),
    ok = random_seed(Options),
    {ok, NewState, get_timer_timeout(NewState)}.

%%
%% Calls
%%
-spec handle_call(_Call, raft_utils:gen_server_from(), state()) ->
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
    NewState = handle_timeout(State),
    ok = log_timeout(State, NewState),
    {noreply, NewState, get_timer_timeout(NewState)};
handle_info({raft_rpc, From, Data}, State = #{options := #{rpc := RPC}}) ->
    Message = raft_rpc:recv(RPC, Data),
    NewState = handle_rpc(From, Message, State),
    ok = log_incoming_message(From, Message, State, NewState),
    {noreply, NewState, get_timer_timeout(NewState)};
handle_info(Info, State) ->
    {noreply, handler_handle_info(Info, State)}.

-spec code_change(_, state(), _) ->
    raft_utils:gen_server_code_change_ret(state()).
code_change(_, State, _) ->
    {ok, State}.

-spec terminate(_Reason, state()) ->
    ok.
terminate(_, _) ->
    ok.

%%
%% common handlers
%%
-spec new_state({handler(), options()}) ->
    state().
new_state({Handler, Options}) ->
    State =
        #{
            options        => Options,
            timer          => undefined,
            role           => {follower, undefined},
            current_term   => 0,
            handler        => Handler,
            handler_state  => handler_init(Handler),
            storage_states => init_storage(Options),
            commands       => [],
            reply          => undefined
        },
    become_follower(State#{current_term := get_term_from_log(last_log_index(State), State)}).


-spec init_storage(options()) ->
    storage().
init_storage(#{storage := Storage}) ->
    #{
        system  => raft_storage:init(Storage, system),
        log     => raft_storage:init(Storage, log   )
    }.

-spec random_seed(options()) ->
    ok.
random_seed(Options) ->
    Algo = exsplus,
    case maps:get(random_seed, Options, undefined) of
        undefined -> rand:seed(Algo);
        Seed      -> rand:seed(Algo, Seed)
    end,
    ok.

-spec handle_timeout(state()) ->
    state().
handle_timeout(State = #{role := {leader, _}}) ->
    try_send_append_entries(State);
handle_timeout(State = #{role := _}) ->
    become_candidate(State).

-spec handle_rpc(raft_rpc:endpoint(), raft_rpc:message(), state()) ->
    state().
handle_rpc(From, {external, ID, Msg}, State) ->
    handle_external_rpc(From, ID, Msg, State);
handle_rpc(From, {internal, Msg}, State) ->
    handle_internal_rpc(From, Msg, State).

%%
%% external rpc handlers
%%
-spec handle_external_rpc(raft_rpc:endpoint(), raft_rpc:request_id(), raft_rpc:external_message(), state()) ->
    state().
handle_external_rpc(From, ID, {command, Command}, State) ->
    handle_command_rpc(From, ID, Command, State);
handle_external_rpc(From, ID, {async_command, Command}, State) ->
    handler_handle_async_command(From, ID, Command, State).

-spec handle_command_rpc(raft_rpc:endpoint(), raft_rpc:request_id(), command(), state()) ->
    state().
handle_command_rpc(From, ID, Command, State = #{role := {leader, _}}) ->
    try_commit(try_handle_next_command(append_command(ID, From, Command, State)));
handle_command_rpc(From, ID, Command, State = #{options := #{rpc := RPC}, role := {follower, Leader}})
    when Leader =/= undefined ->
    ok = raft_rpc:send(RPC, From, Leader, {external, ID, {command, Command}}),
    State;
handle_command_rpc(_, _, _, State = #{role := _}) ->
    State.

%%
%% internal rpc handlers
%%
-spec handle_internal_rpc(raft_rpc:endpoint(), raft_rpc:internal_message(), state()) ->
    state().
handle_internal_rpc(From, Msg = {_, _, _, Term}, State = #{current_term := CurrentTerm}) when Term > CurrentTerm ->
    handle_internal_rpc(From, Msg, become_follower(State#{current_term := Term}));
handle_internal_rpc(_, {_, _, _, Term}, State = #{current_term := CurrentTerm}) when Term < CurrentTerm ->
    State;
handle_internal_rpc(From, {request, Type, Body, _}, State) ->
    {Result, NewState} = handle_rpc_request(Type, Body, From, State),
    ok = send_int_response(From, Type, Result, NewState),
    NewState;
handle_internal_rpc(From, {response, Type, Succeed, _}, State) ->
    handle_rpc_response(Type, From, Succeed, State).

-spec handle_rpc_request(raft_rpc:internal_message_type(), raft_rpc:message_body(), raft_rpc:endpoint(), state()) ->
    {boolean(), state()}.
handle_rpc_request(request_vote, {ReqLastLogIndex, ReqLastLogTerm}, Candidate, State = #{role := {follower, undefined}}) ->
    MyLastLogIndex = last_log_index(State),
    MyLastLogTerm  = get_term_from_log(MyLastLogIndex, State),
    case MyLastLogTerm =< ReqLastLogTerm andalso MyLastLogIndex =< ReqLastLogIndex of
        true ->
            % Голосую!
            {true, schedule_election_timer(update_follower(Candidate, State))};
        false ->
            % Вы слишком стары для меня!
            {false, State}
    end;
handle_rpc_request(request_vote, _, _, State = #{role := _}) ->
    % Извините, я уже проголосовал. :-\
    {false, State};
handle_rpc_request(append_entries, Body, Leader, State = #{role := {follower, undefined}}) ->
    % За короля!
    % лидер появился
    handle_rpc_request(append_entries, Body, Leader, update_follower(Leader, State));
handle_rpc_request(append_entries, Body, Leader, State = #{role := {candidate, _}}) ->
    % Выбрали другого... ;-(
    handle_rpc_request(append_entries, Body, Leader, update_follower(Leader, become_follower(State)));
handle_rpc_request(append_entries, {Prev, Entries, CommitIndex}, _, State0 = #{role := {follower, _}}) ->
    {Result, State1} = try_append_to_log(Prev, Entries, State0),
    State2 =
        case {Result, CommitIndex =< last_log_index(State1)} of
            {true , true} -> commit(CommitIndex, State1);
            {_    , _   } -> State1
        end,
    {Result, schedule_election_timer(State2)}.

-spec handle_rpc_response(raft_rpc:internal_message_type(), raft_rpc:endpoint(), boolean(), state()) ->
    state().
handle_rpc_response(request_vote, From, true, State = #{role := {candidate, _}}) ->
    % за меня проголосовали 8-)
    try_become_leader(add_vote(From, State));
handle_rpc_response(request_vote, _, _, State = #{role := _}) ->
    % за меня проголосовали когда мне уже эти голоса не нужны
    % я уже либо лидер, либо фолловер
    State;
handle_rpc_response(append_entries, From, Succeed, State = #{role := {leader, FollowersState}}) ->
    #follower_state{next_index = NextIndex, match_index = MatchIndex} = FollowerState = maps:get(From, FollowersState),
    NewMatchIndex =
        case Succeed of
            true  -> NextIndex - 1;
            false -> MatchIndex - 1
        end,
    NewFollowerState =
        FollowerState#follower_state{
            match_index = NewMatchIndex,
            rpc_timeout = 0
        },
    try_send_append_entries(try_commit(update_leader_follower(From, NewFollowerState, State)));
handle_rpc_response(append_entries, _, _, State = #{role := _}) ->
    % что-то уже устаревшее
    State.

%%

-spec try_append_to_log({index(), index()}, [log_entry()], state()) ->
    {boolean(), state()}.
try_append_to_log({PrevTerm, PrevIndex}, Entries, State) ->
    case (last_log_index(State) >= PrevIndex) andalso get_term_from_log(PrevIndex, State) of
        PrevTerm ->
            {true, append_log_entries(PrevIndex, Entries, State)};
        _ ->
            {false, State}
    end.

-spec add_vote(raft_rpc:endpoint(), state()) ->
    state().
add_vote(Vote, State = #{role := {candidate, Votes}}) ->
    set_role({candidate, ordsets:add_element(Vote, Votes)}, State).

-spec try_become_leader(state()) ->
    state().
try_become_leader(State = #{role := {candidate, Votes}}) ->
    case has_quorum(erlang:length(Votes) + 1, State) of
        true ->
            become_leader(State);
        false ->
            State
    end.

-spec try_commit(state()) ->
    state().
try_commit(State) ->
    try_commit(last_log_index(State), commit_index(State), State).

-spec try_commit(index(), index(), state()) ->
    state().
try_commit(IndexN, CommitIndex, State = #{current_term := CurrentTerm}) ->
    % If there exists an N such that N > commitIndex, a majority
    % of matchIndex[i] ≥ N, and log[N].term == currentTerm:
    % set commitIndex = N (§5.3, §5.4)
    case IndexN > CommitIndex andalso get_term_from_log(IndexN, State) =:= CurrentTerm of
        true ->
            case is_replicated(IndexN, State) of
                true  -> send_last_reply(commit(IndexN, State));
                false -> try_commit(IndexN - 1, CommitIndex, State)
            end;
        false ->
            State
    end.

-spec is_replicated(index(), state()) ->
    boolean().
is_replicated(Index, State = #{role := {leader, FollowersState}}) ->
    NumberOfReplicas =
        erlang:length(
            lists:filter(
                fun(#follower_state{match_index = MatchIndex}) ->
                    MatchIndex >= Index
                end,
                maps:values(FollowersState)
            )
        ),
    has_quorum(NumberOfReplicas + 1, State).

-spec has_quorum(non_neg_integer(), state()) ->
    boolean().
has_quorum(N, #{options := #{cluster := Cluster}}) ->
    N >= (erlang:length(Cluster) div 2 + 1).

-spec send_last_reply(state()) ->
    state().
send_last_reply(State = #{reply := undefined}) ->
    State;
send_last_reply(State = #{reply := {To, ID, Reply}}) ->
    ok = send_reply(To, ID, Reply, State),
    State#{reply := undefined}.

-spec send_reply(raft_rpc:endpoint(), raft_rpc:request_id(), reply_action(), state()) ->
    ok.
send_reply(_, _, noreply, _) ->
    ok;
send_reply(To, ID, {reply, Reply}, State) ->
    send_ext_response(To, ID, Reply, State).

-spec new_followers_state(state()) ->
    followers_state().
new_followers_state(State = #{options := #{cluster := Cluster, self := Self}}) ->
    maps:from_list([{To, new_follower_state(State)} || To <- (Cluster -- [Self])]).

-spec new_follower_state(state()) ->
    follower_state().
new_follower_state(State) ->
    #follower_state{
        heartbeat    = 0,
        rpc_timeout  = 0,
        next_index   = last_log_index(State) + 1,
        match_index  = erlang:max(last_log_index(State) - 1, 0),
        commit_index = 0
    }.

-spec append_command(raft_rpc:request_id(), raft_rpc:endpoint(), command(), state()) ->
    state().
append_command(ID, From, Command, State = #{commands := Commands}) ->
    State#{commands := lists:keystore(ID, 3, Commands, {ID, From, Command})}.

%%
%% role changing
%%
-spec
become_follower(state()                          ) -> state().
become_follower(State = #{role := {follower , _}}) -> become_follower_(State);
become_follower(State = #{role := {candidate, _}}) -> become_follower_(State);
become_follower(State = #{role := {leader   , _}}) -> become_follower_(State).

-spec become_follower_(state()) ->
    state().
become_follower_(State) ->
    schedule_election_timer(
        set_role({follower, undefined}, State)
    ).

-spec
become_candidate(state()                          ) -> state().
become_candidate(State = #{role := {follower , _}}) -> become_candidate_(State);
become_candidate(State = #{role := {candidate, _}}) -> become_candidate_(State).

-spec become_candidate_(state()) ->
    state().
become_candidate_(State) ->
    NewState =
        schedule_election_timer(
            increment_current_term(
                set_role({candidate, ordsets:new()}, State)
            )
        ),
    ok = send_request_votes(NewState),
    try_become_leader(NewState).

-spec
become_leader(state()                          ) -> state().
become_leader(State = #{role := {candidate, _}}) -> become_leader_(State).

-spec become_leader_(state()) ->
    state().
become_leader_(State) ->
    handler_handle_election(set_role({leader, new_followers_state(State)}, State)).

-spec append_and_send_log_entries(raft_rpc:request_id(), delta(), state()) ->
    state().
append_and_send_log_entries(ID, Delta, State = #{current_term := CurrentTerm}) ->
    try_send_append_entries(
        append_log_entries(
            last_log_index(State),
            [{CurrentTerm, ID, Delta}],
            State
        )
    ).

-spec set_role(role(), state()) ->
    state().
set_role(NewRole, State) ->
    State#{role := NewRole}.

-spec increment_current_term(state()) ->
    state().
increment_current_term(State = #{current_term := CurrentTerm}) ->
    State#{current_term := CurrentTerm + 1}.

-spec update_leader(followers_state(), state()) ->
    state().
update_leader(FollowersState, State = #{role := {leader, _}}) ->
    set_role({leader, FollowersState}, State).

-spec update_leader_follower(raft_rpc:endpoint(), follower_state(), state()) ->
    state().
update_leader_follower(Follower, NewFollowerState, State = #{role := {leader, FollowersState}}) ->
    update_leader(FollowersState#{Follower := NewFollowerState}, State).

-spec update_follower(raft_rpc:endpoint(), state()) ->
    state().
update_follower(Leader, State = #{role := {follower, _}}) ->
    set_role({follower, Leader}, State).

%%

-spec send_request_votes(state()) ->
    ok.
send_request_votes(State = #{options := #{cluster := Cluster, self := Self}}) ->
    LastIndex = last_log_index(State),
    lists:foreach(
        fun(To) ->
            ok = send_int_request(To, request_vote, {LastIndex, get_term_from_log(LastIndex, State)}, State)
        end,
        Cluster -- [Self]
    ).

%% Послать в том случае если:
%%  - пришло время heartbeat
%%  - есть новые записи, но нет текущих запросов (NextIndex < LastLogIndex) & Match =:= NextIndex - 1
-spec try_send_append_entries(state()) ->
    state().
try_send_append_entries(State = #{role := {leader, FollowersState}}) ->
    NewFollowersState =
        maps:map(
            fun(To, FollowerState) ->
                try_send_one_append_entries(To, FollowerState, State)
            end,
            FollowersState
        ),
    schedule_next_heartbeat_timer(update_leader(NewFollowersState, State)).

-spec try_send_one_append_entries(raft_rpc:endpoint(), follower_state(), state()) ->
    follower_state().
try_send_one_append_entries(To, FollowerState, State) ->
    case is_follower_obsolate(FollowerState, State) of
        true ->
            #follower_state{next_index = NextIndex, match_index = MatchIndex} =
                NewFollowersState = update_follower_state(FollowerState, State),
            ok = send_append_entries(To, NextIndex, MatchIndex, State),
            NewFollowersState;
        false ->
            FollowerState
    end.

-spec is_follower_obsolate(follower_state(), state()) ->
    boolean().
is_follower_obsolate(FollowerState, State) ->
    #follower_state{
        heartbeat    = HeartbeatDate,
        rpc_timeout  = RPCTimeoutDate,
        next_index   = NextIndex,
        commit_index = FollowerCommitIndex
    } = FollowerState,
    Now = now_ms(),
    CommitIndex = commit_index(State),
    LastLogIndex = last_log_index(State),

           HeartbeatDate =< Now
    orelse NextIndex =< LastLogIndex andalso RPCTimeoutDate =< Now
    orelse FollowerCommitIndex =/= CommitIndex.

-spec update_follower_state(follower_state(), state()) ->
    follower_state().
update_follower_state(FollowerState, State = #{options := #{broadcast_timeout := Timeout}}) ->
    #follower_state{
        next_index  = NextIndex,
        match_index = MatchIndex
    } = FollowerState,
    Now = now_ms(),

    NewNextIndex =
        case MatchIndex =:= (NextIndex - 1) of
            true  -> last_log_index(State) + 1;
            false -> NextIndex
        end,

    FollowerState#follower_state{
        next_index   = NewNextIndex,
        commit_index = commit_index(State),
        heartbeat    = Now + Timeout,
        rpc_timeout  = Now + Timeout
    }.

-spec send_append_entries(raft_rpc:endpoint(), index(), index(), state()) ->
    ok.
send_append_entries(To, NextIndex, MatchIndex, State) ->
    Prev = {get_term_from_log(MatchIndex, State), MatchIndex},
    Body = {Prev, log_entries(MatchIndex + 1, NextIndex, State), commit_index(State)},
    send_int_request(To, append_entries, Body, State).

-spec send_int_request(raft_rpc:endpoint(), raft_rpc:internal_message_type(), raft_rpc:message_body(), state()) ->
    ok.
send_int_request(To, Type, Body, State = #{current_term := Term}) ->
    send(To, {internal, {request, Type, Body, Term}}, State).

-spec send_int_response(raft_rpc:endpoint(), raft_rpc:internal_message_type(), boolean(), state()) ->
    ok.
send_int_response(To, Type, Succeed, State = #{current_term := CurrentTerm}) ->
    send(To, {internal, {response, Type, Succeed, CurrentTerm}}, State).

-spec send_ext_response(raft_rpc:endpoint(), raft_rpc:request_id(), command(), state()) ->
    ok.
send_ext_response(To, ID, Response, State) ->
    send(To, {external, ID, {response_command, Response}}, State).

-spec send(raft_rpc:endpoint(), raft_rpc:message(), state()) ->
    ok.
send(To, Message, #{options := #{rpc := RPC, self := Self}}) ->
    raft_rpc:send(RPC, Self, To, Message).

-spec get_term_from_log(index(), state()) ->
    term().
get_term_from_log(Index, State) ->
    element(1, log_entry(Index, State)).

%%
%% Timers
%%
-spec schedule_election_timer(state()) ->
    state().
schedule_election_timer(State = #{options := #{election_timeout := Timeout}}) ->
    schedule_timer(now_ms() + randomize_timeout(Timeout), State).

-spec schedule_next_heartbeat_timer(state()) ->
    state().
schedule_next_heartbeat_timer(State = #{options := #{broadcast_timeout := BroadcastTimeout}, role := {leader, FollowersState}}) ->
    NextHeardbeat =
        lists:min(
            [now_ms() + BroadcastTimeout] ++
            lists_unzip_element(#follower_state.heartbeat, maps:values(FollowersState))
        ),
    State#{timer := NextHeardbeat}.

-spec schedule_timer(timestamp_ms(), state()) ->
    state().
schedule_timer(TimestampMS, State) ->
    State#{timer := TimestampMS}.

-spec get_timer_timeout(state()) ->
    timeout_ms().
get_timer_timeout(#{timer := TimerTimestampMS}) ->
    erlang:max(TimerTimestampMS - now_ms(), 0).

-spec now_ms() ->
    timeout_ms().
now_ms() ->
    erlang:system_time(millisecond).

-spec randomize_timeout(timeout_ms() | {From::timeout_ms(), To::timeout_ms()}) ->
    timeout_ms().
randomize_timeout({From, To}) ->
    rand:uniform(erlang:abs(To - From)) + From;
randomize_timeout(Const) ->
    Const.

%%

-spec commit(index(), state()) ->
    state().
commit(Index, State) ->
    try_apply_commited(set_commit_index(Index, State)).

-spec try_apply_commited(state()) ->
    state().
try_apply_commited(State = #{role := _}) ->
    LastApplied = last_applied(State),
    case LastApplied < commit_index(State) of
        true ->
            try_apply_commited(apply_commited(LastApplied + 1, State));
        false ->
            try_handle_next_command(State)
    end.

-spec apply_commited(index(), state()) ->
    state().
apply_commited(Index, State) ->
    {_, ID, Delta} = log_entry(Index, State),
    set_last_applied(Index, handler_apply_delta(ID, Delta, State)).

-spec try_handle_next_command(state()) ->
    state().
try_handle_next_command(State = #{commands := [NextRequest|RemainRequests], role := {leader, _}}) ->
    case last_log_index(State) =:= commit_index(State) of
        true ->
            {ID, From, Command} = NextRequest,
            handler_handle_command(ID, From, Command, State#{commands := RemainRequests});
        false ->
            State
    end;
try_handle_next_command(State) ->
    State.

-spec last_applied(state()) ->
    index().
last_applied(State) ->
    set_default(storage_get_one(system, last_applied, State), 0).

-spec set_last_applied(index(), state()) ->
    state().
set_last_applied(Index, State) ->
    storage_put(system, [{last_applied, Index}], State).

-spec commit_index(state()) ->
    index().
commit_index(State) ->
    set_default(storage_get_one(system, commit_index, State), 0).

-spec set_commit_index(index(), state()) ->
    state().
set_commit_index(Index, State) ->
    storage_put(system, [{commit_index, Index}], State).

-spec last_log_index(state()) ->
    index().
last_log_index(State) ->
    set_default(storage_get_one(system, last_log_index, State), 0).

-spec set_last_log_index(index(), state()) ->
    state().
set_last_log_index(Index, State) ->
    storage_put(system, [{last_log_index, Index}], State).

-spec try_set_last_log_index(index(), state()) ->
    state().
try_set_last_log_index(Index, State) ->
    set_last_log_index(erlang:max(Index, last_log_index(State)), State).

-spec log_entry(index(), state()) ->
    log_entry().
log_entry(Index, State) ->
    set_default(storage_get_one(log, Index, State), {0, undefined}).

-spec log_entries(index(), index(), state()) ->
    [log_entry()].
log_entries(From, To, State) ->
    storage_get(log, lists:seq(From, To), State).

-spec append_log_entries(index(), [log_entry()], state()) ->
    state().
append_log_entries(PrevIndex, Entries, State) ->
    LastIndex = PrevIndex + erlang:length(Entries),
    Values    = lists:zip(lists:seq(PrevIndex + 1, LastIndex), Entries),
    try_set_last_log_index(LastIndex, storage_put(log, Values, State)).

-spec last_log_entry(state()) ->
    log_entry() | undefined.
last_log_entry(State) ->
    storage_get_one(log, last_log_index(State), State).

-spec set_default(undefined | Value, Value) ->
    Value.
set_default(undefined, Default) ->
    Default;
set_default(Value, _) ->
    Value.

%%
%% interaction with storage
%%
-spec storage_put(raft_storage:type(), [{raft_storage:key(), raft_storage:value()}], state()) ->
    state().
storage_put(Type, Values, State = #{options := #{storage := Storage}, storage_states := StorageStates}) ->
    StorageState = maps:get(Type, StorageStates),
    NewStorageState = raft_storage:put(Storage, Values, StorageState),
    State#{storage_states := StorageStates#{Type := NewStorageState}}.

-spec storage_get(raft_storage:type(), [raft_storage:key()], state()) ->
    [raft_storage:value()].
storage_get(Type, Keys, #{options := #{storage := Storage}, storage_states := StorageStates}) ->
    raft_storage:get(Storage, Keys, maps:get(Type, StorageStates)).

-spec storage_get_one(raft_storage:type(), raft_storage:key(), state()) ->
    raft_storage:value().
storage_get_one(Type, Key, #{options := #{storage := Storage}, storage_states := StorageStates}) ->
    raft_storage:get_one(Storage, Key, maps:get(Type, StorageStates)).

%%
%% interaction with handler
%%
-spec handler_init(handler()) ->
    handler_state().
handler_init(Handler) ->
    raft_utils:apply_mod_opts(Handler, init, []).

-spec handler_handle_election(state()) ->
    state().
handler_handle_election(State = #{handler := Handler, handler_state := HandlerState}) ->
    {Delta, NewHandlerState} = raft_utils:apply_mod_opts(Handler, handle_election, [HandlerState]),
    NewState = State#{handler_state := NewHandlerState},
    case Delta of
        undefined ->
            NewState;
        Delta ->
            % TODO подумать про request_id
            append_and_send_log_entries(undefined, Delta, NewState)
    end.

%%
%% исполняется на лидере, может сгенерить изменение стейта — дельту
%% которая потом будет реплицироваться на остальные ноды,
%% и после репликации отошлётся ответ
%%
%% задача обработки идемпотентости лежит на обработчике
%%
-spec handler_handle_command(raft_rpc:request_id(), raft_rpc:endpoint(), command(), state()) ->
    state().
handler_handle_command(ID, From, Command, State = #{handler := Handler, handler_state := HandlerState}) ->
    {Reply, Delta, NewHandlerState} = raft_utils:apply_mod_opts(Handler, handle_command, [ID, Command, HandlerState]),
    NewState = State#{handler_state := NewHandlerState},
    case Delta of
        undefined ->
            ok = send_reply(From, ID, Reply, NewState),
            NewState;
        _ ->
            append_and_send_log_entries(ID, Delta, NewState#{reply := {From, ID, Reply}})
    end.

%% исполняется на любой ноде, очерёдность не определена, не может менять стейт
-spec handler_handle_async_command(raft_rpc:endpoint(), raft_rpc:request_id(), command(), state()) ->
    state().
handler_handle_async_command(From, ID, Command, State = #{handler := Handler, handler_state := HandlerState}) ->
    {Reply, NewHandlerState} = raft_utils:apply_mod_opts(Handler, handle_async_command, [ID, Command, HandlerState]),
    NewState = State#{handler_state := NewHandlerState},
    ok = send_reply(From, ID, Reply, NewState),
    NewState.

-spec handler_handle_info(_Info, state()) ->
    state().
handler_handle_info(Info, State = #{handler := Handler, handler_state := HandlerState}) ->
    {Delta, NewHandlerState} = raft_utils:apply_mod_opts(Handler, handle_info, [Info, HandlerState]),
    NewState = State#{handler_state := NewHandlerState},
    case Delta of
        undefined ->
            NewState;
        _ ->
            append_and_send_log_entries(undefined, Delta, NewState)
    end.

-spec handler_apply_delta(raft_rpc:request_id(), delta(), state()) ->
    state().
handler_apply_delta(ID, Delta, State = #{handler := Handler, handler_state := HandlerState}) ->
    NewHandlerState = raft_utils:apply_mod_opts(Handler, apply_delta, [ID, Delta, HandlerState]),
    State#{handler_state := NewHandlerState}.

%%
%% utils
%%
-spec lists_unzip_element(pos_integer(), [tuple()]) ->
    _.
lists_unzip_element(N, List) ->
    lists:map(
        fun(Tuple) ->
            element(N, Tuple)
        end,
        List
    ).

%%
%% logging
%%
-spec log_timeout(state(), state()) ->
    ok.
log_timeout(StateBefore = #{options := #{logger := Logger}}, StateAfter) ->
    raft_logger:log(Logger, timeout, StateBefore, StateAfter).

-spec log_incoming_message(raft_rpc:endpoint(), raft_rpc:message(), state(), state()) ->
    ok.
log_incoming_message(From, Message, StateBefore = #{options := #{logger := Logger}}, StateAfter) ->
    raft_logger:log(Logger, {incoming_message, From, Message}, StateBefore, StateAfter).

%%
%% formatting
%%
-spec format_state(state()) ->
    list().
format_state(State = #{current_term := Term, role := Role}) ->
    Commit      = commit_index(State),
    LastApplied = last_applied(State),
    LastLog     = last_log_index(State),
    LastLogEntry = last_log_entry(State),
    io_lib:format("~9999p ~9999p ~9999p ~9999p ~9999p ~9999p", [ext_role(Role), Term, LastLog, Commit, LastApplied, LastLogEntry]).

-spec format_self_endpoint(state()) ->
    list().
format_self_endpoint(#{options := #{self := Self}}) ->
    raft_rpc:format_endpoint(Self).

-spec
ext_role(role()             ) -> ext_role().
ext_role({leader   , _     }) -> leader;
ext_role({follower , Leader}) -> {follower, Leader};
ext_role({candidate, _     }) -> candidate.
