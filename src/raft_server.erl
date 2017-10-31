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
%%%   - внешнее ревью
%%%   -
%%%  - доработки:
%%%   - сделать регистрацию в RPC
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
%%%   - переделать работу со storage (и придумать как, но то, что есть — хрень :-\ )
%%%   - убрать raft_utils и перенести всё в genlib
%%%   -
%%%  - тестирование:
%%%   - тестовый сторадж
%%%   - цепи маркова
%%%   - отдельные тесты для rpc
%%%   -
%%%
-module(raft_server).

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
-export([start_link        /3]).
-export([send_command      /5]).
-export([send_async_command/5]).

%% raft_rpc_server callbacks
-behaviour(raft_rpc_server).
-export([init/1, handle_timeout/3, handle_rpc_message/5, handle_info/4, format_state/2, format_self_endpoint/2]).


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
    logger            => raft_rpc_logger:logger(),

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
    raft_rpc_server:start_link(RegName, {?MODULE, {Handler, Options}}, rpc_server_options(Options)).

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
            % TODO initial Retry and fun name
            erlang:exit({timeout, {?MODULE, send_ext_command, [RPC, AllCluster, ID, Command, Retry]}})
    end.

-spec recv_response_command(raft_rpc:rpc(), raft_rpc:request_id(), timeout()) ->
    {ok, raft_rpc:endpoint(), command()} | timeout.
recv_response_command(RPC, ID, Timeout) ->
    case raft_rpc:recv(RPC, Timeout) of
        {ok, From, {external, ID, {response_command, Command}}} ->
            {ok, From, Command};
        {ok, _, _} ->
            recv_response_command(RPC, ID, Timeout);
        timeout ->
            timeout
    end.

%%
%% raft_rpc_server callbacks
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
    % текущая роль и специфичные для неё данные
    role => role(),

    % текущий терм (вообще, имхо, слово "эпоха" тут более подходящее)
    current_term  => raft_term(),
    handler_state => handler_state(),

    % состояние хранилища
    storage_states => storage(),

    % все полученные запросы, которые ожидают ответа
    commands => [{raft_rpc:request_id(), raft_rpc:endpoint(), command()}],

    % ответ на последний обработанный запрос
    reply => {raft_rpc:endpoint(), raft_rpc:request_id(), reply()} | undefined
}.

% handling state
-type hstate() :: #{
    handler  => handler(),
    options  => options(),
    timer    => raft_rpc_server:timer(),
    messages => [{raft_rpc:endpoint(), raft_rpc:message()}],
    state    => state()
}.

-spec init({handler(), options()}) ->
    raft_rpc_server:handler_ret().
init({Handler, Options}) ->
    ok = random_seed(Options),
    handle_result(init_(hstate(Handler, Options, undefined, new_state(Handler, Options)))).

-spec handle_timeout({handler(), options()}, raft_rpc_server:timer(), state()) ->
    raft_rpc_server:handler_ret().
handle_timeout({Handler, Options}, Timer, State) ->
    handle_result(handle_timeout_(hstate(Handler, Options, Timer, State))).

-spec handle_rpc_message({handler(), options()}, raft_rpc:endpoint(), raft_rpc:message(), raft_rpc_server:timer(), state()) ->
    raft_rpc_server:handler_ret().
handle_rpc_message({Handler, Options}, From, Message, Timer, State) ->
    handle_result(handle_rpc_message_(From, Message, hstate(Handler, Options, Timer, State))).

-spec handle_info({handler(), options()}, _Info, raft_rpc_server:timer(), state()) ->
    raft_rpc_server:handler_ret().
handle_info({Handler, Options}, Info, Timer, State) ->
    handle_result(handler_handle_info(Info, hstate(Handler, Options, Timer, State))).

-spec hstate(handler(), options(), raft_rpc_server:timer(), state()) ->
    hstate().
hstate(Handler, Options, Timer, State) ->
    #{
        handler  => Handler,
        options  => Options,
        timer    => Timer,
        messages => [],
        state    => State
    }.

-spec handle_result(hstate()) ->
    raft_rpc_server:handler_ret().
handle_result(#{timer := Timer, messages := Messages, state := State}) ->
    {Messages, Timer, State}.

-spec random_seed(options()) ->
    ok.
random_seed(Options) ->
    Algo = exsplus,
    _ = case maps:get(random_seed, Options, undefined) of
            undefined -> rand:seed(Algo);
            Seed      -> rand:seed(Algo, Seed)
        end,
    ok.

%%
%% common handlers
%%
-define(role     (Role     ), #{state := #{role := Role}}  ).
-define(any_role            , ?role(_                     )).
-define(leader              , ?role({leader   , _        })).
-define(leader   (Leader   ), ?role({leader   , Leader   })).
-define(candidate           , ?role({candidate, _        })).
-define(candidate(Candidate), ?role({candidate, Candidate})).
-define(follower            , ?role({follower , _        })).
-define(follower (Follower ), ?role({follower , Follower })).
-define(current_term(CurrentTerm), #{state := #{current_term := CurrentTerm}}).

-spec new_state(handler(), options()) ->
    state().
new_state(Handler, Options) ->
    #{
        role           => {follower, undefined},
        current_term   => 0,
        handler_state  => handler_init(Handler),
        storage_states => init_storage(Options),
        commands       => [],
        reply          => undefined
    }.

-spec init_(hstate()) ->
    hstate().
init_(HState = #{state := State}) ->
    become_follower(HState#{state := State#{current_term := get_term_from_log(last_log_index(HState), HState)}}).

-spec init_storage(options()) ->
    storage().
init_storage(#{storage := Storage}) ->
    #{
        system  => raft_storage:init(Storage, system),
        log     => raft_storage:init(Storage, log   )
    }.

-spec handle_timeout_(hstate()) ->
    hstate().
handle_timeout_(HState = ?leader) ->
    try_send_append_entries(HState);
handle_timeout_(HState = ?any_role) ->
    become_candidate(HState).

-spec handle_rpc_message_(raft_rpc:endpoint(), raft_rpc:message(), hstate()) ->
    hstate().
handle_rpc_message_(From, {external, ID, Msg}, State) ->
    handle_external_rpc(From, ID, Msg, State);
handle_rpc_message_(From, {internal, Msg}, State) ->
    handle_internal_rpc(From, Msg, State).

%%
%% external rpc handlers
%%
-spec handle_external_rpc(raft_rpc:endpoint(), raft_rpc:request_id(), raft_rpc:external_message(), hstate()) ->
    hstate().
handle_external_rpc(From, ID, {command, Command}, HState) ->
    handle_command_rpc(From, ID, Command, HState);
handle_external_rpc(From, ID, {async_command, Command}, HState) ->
    handler_handle_async_command(From, ID, Command, HState).

-spec handle_command_rpc(raft_rpc:endpoint(), raft_rpc:request_id(), command(), hstate()) ->
    hstate().
handle_command_rpc(From, ID, Command, HState = ?leader) ->
    try_commit(try_handle_next_command(append_command(ID, From, Command, HState)));
handle_command_rpc(From, ID, Command, HState = ?follower(Leader))
    when Leader =/= undefined ->
    send(From, Leader, {external, ID, {command, Command}}, HState);
handle_command_rpc(_, _, _, HState = ?any_role) ->
    HState.

%%
%% internal rpc handlers
%%
-spec handle_internal_rpc(raft_rpc:endpoint(), raft_rpc:internal_message(), hstate()) ->
    hstate().
handle_internal_rpc(From, Msg = {_, _, _, Term}, HState = #{state := State} = ?current_term(CurrentTerm))
    when Term > CurrentTerm ->
    handle_internal_rpc(From, Msg, become_follower(HState#{state := State#{current_term := Term}}));
handle_internal_rpc(_, {_, _, _, Term}, HState = ?current_term(CurrentTerm))
    when Term < CurrentTerm ->
    HState;
handle_internal_rpc(From, {request, Type, Body, _}, HState) ->
    {Result, NewHState} = handle_rpc_request(Type, Body, From, HState),
    send_int_response(From, Type, Result, NewHState);
handle_internal_rpc(From, {response, Type, Succeed, _}, HState) ->
    handle_rpc_response(Type, From, Succeed, HState).

-spec handle_rpc_request(raft_rpc:internal_message_type(), raft_rpc:message_body(), raft_rpc:endpoint(), hstate()) ->
    {boolean(), hstate()}.
handle_rpc_request(request_vote, {ReqLastLogIndex, ReqLastLogTerm}, Candidate, HState = ?follower(undefined)) ->
    MyLastLogIndex = last_log_index(HState),
    MyLastLogTerm  = get_term_from_log(MyLastLogIndex, HState),
    case MyLastLogTerm =< ReqLastLogTerm andalso MyLastLogIndex =< ReqLastLogIndex of
        true ->
            % Голосую!
            {true, schedule_election_timer(update_follower(Candidate, HState))};
        false ->
            % Вы слишком стары для меня!
            {false, HState}
    end;
handle_rpc_request(request_vote, _, _, HState = ?any_role) ->
    % Извините, я уже проголосовал. :-\
    {false, HState};
handle_rpc_request(append_entries, Body, Leader, HState = ?follower(undefined)) ->
    % За короля!
    % лидер появился
    handle_rpc_request(append_entries, Body, Leader, update_follower(Leader, HState));
handle_rpc_request(append_entries, Body, Leader, HState = ?candidate) ->
    % Выбрали другого... ;-(
    handle_rpc_request(append_entries, Body, Leader, update_follower(Leader, become_follower(HState)));
handle_rpc_request(append_entries, {Prev, Entries, CommitIndex}, _, HState0 = ?follower) ->
    {Result, HState1} = try_append_to_log(Prev, Entries, HState0),
    HState2 =
        case {Result, CommitIndex =< last_log_index(HState1)} of
            {true , true} -> commit(CommitIndex, HState1);
            {_    , _   } -> HState1
        end,
    {Result, schedule_election_timer(HState2)}.

-spec handle_rpc_response(raft_rpc:internal_message_type(), raft_rpc:endpoint(), boolean(), hstate()) ->
    hstate().
handle_rpc_response(request_vote, From, true, HState = ?candidate) ->
    % за меня проголосовали 8-)
    try_become_leader(add_vote(From, HState));
handle_rpc_response(request_vote, _, _, HState = ?any_role) ->
    % за меня проголосовали когда мне уже эти голоса не нужны
    % я уже либо лидер, либо фолловер
    HState;
handle_rpc_response(append_entries, From, Succeed, HState = ?leader(FollowersState)) ->
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
    try_send_append_entries(try_commit(update_leader_follower(From, NewFollowerState, HState)));
handle_rpc_response(append_entries, _, _, HState = ?any_role) ->
    % что-то уже устаревшее
    HState.

%%

-spec try_append_to_log({index(), index()}, [log_entry()], hstate()) ->
    {boolean(), hstate()}.
try_append_to_log({PrevTerm, PrevIndex}, Entries, HState) ->
    case (last_log_index(HState) >= PrevIndex) andalso get_term_from_log(PrevIndex, HState) of
        PrevTerm ->
            {true, append_log_entries(PrevIndex, Entries, HState)};
        _ ->
            {false, HState}
    end.

-spec add_vote(raft_rpc:endpoint(), hstate()) ->
    hstate().
add_vote(Vote, HState = ?candidate(Votes)) ->
    set_role({candidate, ordsets:add_element(Vote, Votes)}, HState).

-spec try_become_leader(hstate()) ->
    hstate().
try_become_leader(HState = ?candidate(Votes)) ->
    case has_quorum(erlang:length(Votes) + 1, HState) of
        true ->
            become_leader(HState);
        false ->
            HState
    end.

-spec try_commit(hstate()) ->
    hstate().
try_commit(HState) ->
    try_commit(last_log_index(HState), commit_index(HState), HState).

-spec try_commit(index(), index(), hstate()) ->
    hstate().
try_commit(IndexN, CommitIndex, HState = ?current_term(CurrentTerm)) ->
    % If there exists an N such that N > commitIndex, a majority
    % of matchIndex[i] ≥ N, and log[N].term == currentTerm:
    % set commitIndex = N (§5.3, §5.4)
    case IndexN > CommitIndex andalso get_term_from_log(IndexN, HState) =:= CurrentTerm of
        true ->
            case is_replicated(IndexN, HState) of
                true  -> send_last_reply(commit(IndexN, HState));
                false -> try_commit(IndexN - 1, CommitIndex, HState)
            end;
        false ->
            HState
    end.

-spec is_replicated(index(), hstate()) ->
    boolean().
is_replicated(Index, HState = ?leader(FollowersState)) ->
    NumberOfReplicas =
        erlang:length(
            lists:filter(
                fun(#follower_state{match_index = MatchIndex}) ->
                    MatchIndex >= Index
                end,
                maps:values(FollowersState)
            )
        ),
    has_quorum(NumberOfReplicas + 1, HState).

-spec has_quorum(non_neg_integer(), hstate()) ->
    boolean().
has_quorum(N, #{options := #{cluster := Cluster}}) ->
    N >= (erlang:length(Cluster) div 2 + 1).

-spec send_last_reply(hstate()) ->
    hstate().
send_last_reply(HState = #{state := #{reply := undefined}}) ->
    HState;
send_last_reply(HState = #{state := State = #{reply := {To, ID, Reply}}}) ->
    send_reply(To, ID, Reply, HState#{state := State#{reply := undefined}}).

-spec send_reply(raft_rpc:endpoint(), raft_rpc:request_id(), reply_action(), hstate()) ->
    hstate().
send_reply(_, _, noreply, HState) ->
    HState;
send_reply(To, ID, {reply, Reply}, HState) ->
    send_ext_response(To, ID, Reply, HState).

-spec new_followers_state(hstate()) ->
    followers_state().
new_followers_state(HState = #{options := #{cluster := Cluster, self := Self}}) ->
    maps:from_list([{To, new_follower_state(HState)} || To <- (Cluster -- [Self])]).

-spec new_follower_state(hstate()) ->
    follower_state().
new_follower_state(HState) ->
    #follower_state{
        heartbeat    = 0,
        rpc_timeout  = 0,
        next_index   = last_log_index(HState) + 1,
        match_index  = erlang:max(last_log_index(HState) - 1, 0),
        commit_index = 0
    }.

-spec append_command(raft_rpc:request_id(), raft_rpc:endpoint(), command(), hstate()) ->
    hstate().
append_command(ID, From, Command, HState = #{state := State = #{commands := Commands}}) ->
    HState#{state := State#{commands := lists:keystore(ID, 3, Commands, {ID, From, Command})}}.

%%
%% role changing
%%
-spec
become_follower(hstate()           ) -> hstate().
become_follower(HState = ?follower ) -> become_follower_(HState);
become_follower(HState = ?candidate) -> become_follower_(HState);
become_follower(HState = ?leader   ) -> become_follower_(HState).

-spec become_follower_(hstate()) ->
    hstate().
become_follower_(HState) ->
    schedule_election_timer(
        set_role({follower, undefined}, HState)
    ).

-spec
become_candidate(hstate()           ) -> hstate().
become_candidate(HState = ?follower ) -> become_candidate_(HState);
become_candidate(HState = ?candidate) -> become_candidate_(HState).

-spec become_candidate_(hstate()) ->
    hstate().
become_candidate_(HState) ->
    try_become_leader(
        send_request_votes(
            schedule_election_timer(
                increment_current_term(
                    set_role({candidate, ordsets:new()}, HState)
                )
            )
        )
    ).

-spec
become_leader(hstate()           ) -> hstate().
become_leader(HState = ?candidate) -> become_leader_(HState).

-spec become_leader_(hstate()) ->
    hstate().
become_leader_(HState) ->
    handler_handle_election(set_role({leader, new_followers_state(HState)}, HState)).

-spec append_and_send_log_entries(raft_rpc:request_id(), delta(), hstate()) ->
    hstate().
append_and_send_log_entries(ID, Delta, HState = ?current_term(CurrentTerm)) ->
    try_send_append_entries(
        append_log_entries(
            last_log_index(HState),
            [{CurrentTerm, ID, Delta}],
            HState
        )
    ).

-spec set_role(role(), hstate()) ->
    hstate().
set_role(NewRole, HState = #{state := State}) ->
    HState#{state := State#{role := NewRole}}.

-spec increment_current_term(hstate()) ->
    hstate().
increment_current_term(HState = #{state := State = #{current_term := CurrentTerm}}) ->
    HState#{state := State#{current_term := CurrentTerm + 1}}.

-spec update_leader(followers_state(), hstate()) ->
    hstate().
update_leader(FollowersState, HState = ?leader) ->
    set_role({leader, FollowersState}, HState).

-spec update_leader_follower(raft_rpc:endpoint(), follower_state(), hstate()) ->
    hstate().
update_leader_follower(Follower, NewFollowerState, HState = ?leader(FollowersState)) ->
    update_leader(FollowersState#{Follower := NewFollowerState}, HState).

-spec update_follower(raft_rpc:endpoint(), hstate()) ->
    hstate().
update_follower(Leader, HState = ?follower) ->
    set_role({follower, Leader}, HState).

%%

-spec send_request_votes(hstate()) ->
    hstate().
send_request_votes(HState = #{options := #{cluster := Cluster, self := Self}}) ->
    LastIndex = last_log_index(HState),
    lists:foldl(
        fun(To, HStateAcc) ->
            send_int_request(To, request_vote, {LastIndex, get_term_from_log(LastIndex, HStateAcc)}, HStateAcc)
        end,
        HState,
        Cluster -- [Self]
    ).

%% Послать в том случае если:
%%  - пришло время heartbeat
%%  - есть новые записи, но нет текущих запросов (NextIndex < LastLogIndex) & Match =:= NextIndex - 1
-spec try_send_append_entries(hstate()) ->
    hstate().
try_send_append_entries(HState = ?leader(FollowersState)) ->
    {NewFollowersState, NewHState} = maps_mapfoldl(fun try_send_one_append_entries/3, HState, FollowersState),
    schedule_next_heartbeat_timer(update_leader(NewFollowersState, NewHState)).

-spec try_send_one_append_entries(raft_rpc:endpoint(), follower_state(), hstate()) ->
    {follower_state(), hstate()}.
try_send_one_append_entries(To, FollowerState, HState) ->
    case is_follower_obsolate(FollowerState, HState) of
        true ->
            #follower_state{next_index = NextIndex, match_index = MatchIndex} =
                NewFollowersState = update_follower_state(FollowerState, HState),
            NewHState = send_append_entries(To, NextIndex, MatchIndex, HState),
            {NewFollowersState, NewHState};
        false ->
            {FollowerState, HState}
    end.

-spec is_follower_obsolate(follower_state(), hstate()) ->
    boolean().
is_follower_obsolate(FollowerState, HState) ->
    #follower_state{
        heartbeat    = HeartbeatDate,
        rpc_timeout  = RPCTimeoutDate,
        next_index   = NextIndex,
        commit_index = FollowerCommitIndex
    } = FollowerState,
    Now = now_ms(),
    CommitIndex = commit_index(HState),
    LastLogIndex = last_log_index(HState),

           HeartbeatDate =< Now
    orelse NextIndex =< LastLogIndex andalso RPCTimeoutDate =< Now
    orelse FollowerCommitIndex =/= CommitIndex.

-spec update_follower_state(follower_state(), hstate()) ->
    follower_state().
update_follower_state(FollowerState, HState = #{options := #{broadcast_timeout := Timeout}}) ->
    #follower_state{
        next_index  = NextIndex,
        match_index = MatchIndex
    } = FollowerState,
    Now = now_ms(),

    NewNextIndex =
        case MatchIndex =:= (NextIndex - 1) of
            true  -> last_log_index(HState) + 1;
            false -> NextIndex
        end,

    FollowerState#follower_state{
        next_index   = NewNextIndex,
        commit_index = commit_index(HState),
        heartbeat    = Now + Timeout,
        rpc_timeout  = Now + Timeout
    }.

-spec send_append_entries(raft_rpc:endpoint(), index(), index(), hstate()) ->
    hstate().
send_append_entries(To, NextIndex, MatchIndex, HState) ->
    Prev = {get_term_from_log(MatchIndex, HState), MatchIndex},
    Body = {Prev, log_entries(MatchIndex + 1, NextIndex, HState), commit_index(HState)},
    send_int_request(To, append_entries, Body, HState).

-spec send_int_request(raft_rpc:endpoint(), raft_rpc:internal_message_type(), raft_rpc:message_body(), hstate()) ->
    hstate().
send_int_request(To, Type, Body, HState = ?current_term(Term)) ->
    send(To, {internal, {request, Type, Body, Term}}, HState).

-spec send_int_response(raft_rpc:endpoint(), raft_rpc:internal_message_type(), boolean(), hstate()) ->
    hstate().
send_int_response(To, Type, Succeed, HState = ?current_term(Term)) ->
    send(To, {internal, {response, Type, Succeed, Term}}, HState).

-spec send_ext_response(raft_rpc:endpoint(), raft_rpc:request_id(), command(), hstate()) ->
    hstate().
send_ext_response(To, ID, Response, HState) ->
    send(To, {external, ID, {response_command, Response}}, HState).

-spec send(raft_rpc:endpoint(), raft_rpc:message(), hstate()) ->
    hstate().
send(To, Message, HState = #{options := #{self := Self}}) ->
    send(Self, To, Message, HState).

-spec send(raft_rpc:endpoint(), raft_rpc:endpoint(), raft_rpc:message(), hstate()) ->
    hstate().
send(From, To, Message, HState = #{messages := Messages}) ->
    HState#{messages := [{From, To, Message}|Messages]}.

-spec get_term_from_log(index(), hstate()) ->
    term().
get_term_from_log(Index, HState) ->
    element(1, log_entry(Index, HState)).

%%
%% Timers
%%
-spec schedule_election_timer(hstate()) ->
    hstate().
schedule_election_timer(HState = #{options := #{election_timeout := Timeout}}) ->
    schedule_timer(now_ms() + randomize_timeout(Timeout), HState).

-spec schedule_next_heartbeat_timer(hstate()) ->
    hstate().
schedule_next_heartbeat_timer(HState = #{options := #{broadcast_timeout := BroadcastTimeout}} = ?leader(FollowersState)) ->
    NextHeardbeat =
        lists:min(
            [now_ms() + BroadcastTimeout] ++
            lists_unzip_element(#follower_state.heartbeat, maps:values(FollowersState))
        ),
    schedule_timer(NextHeardbeat, HState).

-spec schedule_timer(timestamp_ms(), hstate()) ->
    hstate().
schedule_timer(TimestampMS, HState) ->
    HState#{timer := TimestampMS}.

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

-spec commit(index(), hstate()) ->
    hstate().
commit(Index, HState) ->
    try_apply_commited(set_commit_index(Index, HState)).

-spec try_apply_commited(hstate()) ->
    hstate().
try_apply_commited(HState) ->
    LastApplied = last_applied(HState),
    case LastApplied < commit_index(HState) of
        true ->
            try_apply_commited(apply_commited(LastApplied + 1, HState));
        false ->
            try_handle_next_command(HState)
    end.

-spec apply_commited(index(), hstate()) ->
    hstate().
apply_commited(Index, HState) ->
    {_, ID, Delta} = log_entry(Index, HState),
    set_last_applied(Index, handler_apply_delta(ID, Delta, HState)).

-spec try_handle_next_command(hstate()) ->
    hstate().
try_handle_next_command(HState = #{state := State = #{commands := [NextRequest|RemainRequests]}} = ?leader) ->
    case last_log_index(HState) =:= commit_index(HState) of
        true ->
            {ID, From, Command} = NextRequest,
            handler_handle_command(ID, From, Command, HState#{state := State#{commands := RemainRequests}});
        false ->
            HState
    end;
try_handle_next_command(HState) ->
    HState.

-spec last_applied(hstate()) ->
    index().
last_applied(HState) ->
    set_default(storage_get_one(system, last_applied, HState), 0).

-spec set_last_applied(index(), hstate()) ->
    hstate().
set_last_applied(Index, HState) ->
    storage_put(system, [{last_applied, Index}], HState).

-spec commit_index(hstate()) ->
    index().
commit_index(HState) ->
    set_default(storage_get_one(system, commit_index, HState), 0).

-spec set_commit_index(index(), hstate()) ->
    hstate().
set_commit_index(Index, HState) ->
    storage_put(system, [{commit_index, Index}], HState).

-spec last_log_index(hstate()) ->
    index().
last_log_index(HState) ->
    set_default(storage_get_one(system, last_log_index, HState), 0).

-spec set_last_log_index(index(), hstate()) ->
    hstate().
set_last_log_index(Index, HState) ->
    storage_put(system, [{last_log_index, Index}], HState).

-spec try_set_last_log_index(index(), hstate()) ->
    hstate().
try_set_last_log_index(Index, HState) ->
    set_last_log_index(erlang:max(Index, last_log_index(HState)), HState).

-spec log_entry(index(), hstate()) ->
    log_entry().
log_entry(Index, HState) ->
    set_default(storage_get_one(log, Index, HState), {0, undefined}).

-spec log_entries(index(), index(), hstate()) ->
    [log_entry()].
log_entries(From, To, HState) ->
    storage_get(log, lists:seq(From, To), HState).

-spec append_log_entries(index(), [log_entry()], hstate()) ->
    hstate().
append_log_entries(PrevIndex, Entries, HState) ->
    LastIndex = PrevIndex + erlang:length(Entries),
    Values    = lists:zip(lists:seq(PrevIndex + 1, LastIndex), Entries),
    try_set_last_log_index(LastIndex, storage_put(log, Values, HState)).

-spec last_log_entry(hstate()) ->
    log_entry() | undefined.
last_log_entry(HState) ->
    storage_get_one(log, last_log_index(HState), HState).

-spec set_default(undefined | Value, Value) ->
    Value.
set_default(undefined, Default) ->
    Default;
set_default(Value, _) ->
    Value.

%%
%% interaction with storage
%%
-spec storage_put(raft_storage:type(), [{raft_storage:key(), raft_storage:value()}], hstate()) ->
    hstate().
storage_put(Type, Values, HState = #{options := #{storage := Storage}, state := State = #{storage_states := StorageStates}}) ->
    StorageState = maps:get(Type, StorageStates),
    NewStorageState = raft_storage:put(Storage, Values, StorageState),
    HState#{state := State#{storage_states := StorageStates#{Type := NewStorageState}}}.

-spec storage_get(raft_storage:type(), [raft_storage:key()], hstate()) ->
    [raft_storage:value()].
storage_get(Type, Keys, #{options := #{storage := Storage}, state := #{storage_states := StorageStates}}) ->
    raft_storage:get(Storage, Keys, maps:get(Type, StorageStates)).

-spec storage_get_one(raft_storage:type(), raft_storage:key(), hstate()) ->
    raft_storage:value().
storage_get_one(Type, Key, #{options := #{storage := Storage}, state := #{storage_states := StorageStates}}) ->
    raft_storage:get_one(Storage, Key, maps:get(Type, StorageStates)).

%%
%% interaction with handler
%%
-spec handler_init(handler()) ->
    handler_state().
handler_init(Handler) ->
    raft_utils:apply_mod_opts(Handler, init, []).

-define(handler(Handler, HandlerState), #{handler := Handler, state := #{handler_state := HandlerState}}).

-spec handler_handle_election(hstate()) ->
    hstate().
handler_handle_election(HState = ?handler(Handler, HandlerState)) ->
    {Delta, NewHandlerState} = raft_utils:apply_mod_opts(Handler, handle_election, [HandlerState]),
    NewState = update_handler_state(HState, NewHandlerState),
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
-spec handler_handle_command(raft_rpc:request_id(), raft_rpc:endpoint(), command(), hstate()) ->
    hstate().
handler_handle_command(ID, From, Command, HState = ?handler(Handler, HandlerState)) ->
    {Reply, Delta, NewHandlerState} = raft_utils:apply_mod_opts(Handler, handle_command, [ID, Command, HandlerState]),
    NewHState = #{state := NewState} = update_handler_state(HState, NewHandlerState),
    case Delta of
        undefined ->
            send_reply(From, ID, Reply, NewHState);
        _ ->
            append_and_send_log_entries(ID, Delta, NewHState#{state := NewState#{reply := {From, ID, Reply}}})
    end.

%% исполняется на любой ноде, очерёдность не определена, не может менять стейт
-spec handler_handle_async_command(raft_rpc:endpoint(), raft_rpc:request_id(), command(), hstate()) ->
    hstate().
handler_handle_async_command(From, ID, Command, HState = ?handler(Handler, HandlerState)) ->
    {Reply, NewHandlerState} = raft_utils:apply_mod_opts(Handler, handle_async_command, [ID, Command, HandlerState]),
    send_reply(From, ID, Reply, update_handler_state(HState, NewHandlerState)).

-spec handler_handle_info(_Info, hstate()) ->
    hstate().
handler_handle_info(Info, HState = ?handler(Handler, HandlerState)) ->
    {Delta, NewHandlerState} = raft_utils:apply_mod_opts(Handler, handle_info, [Info, HandlerState]),
    NewState = update_handler_state(HState, NewHandlerState),
    case Delta of
        undefined ->
            NewState;
        _ ->
            append_and_send_log_entries(undefined, Delta, NewState)
    end.

-spec handler_apply_delta(raft_rpc:request_id(), delta(), hstate()) ->
    hstate().
handler_apply_delta(ID, Delta, HState = ?handler(Handler, HandlerState)) ->
    NewHandlerState = raft_utils:apply_mod_opts(Handler, apply_delta, [ID, Delta, HandlerState]),
    update_handler_state(HState, NewHandlerState).


-spec update_handler_state(hstate(), handler_state()) ->
    hstate().
update_handler_state(HState = #{state := State}, NewHandlerState) ->
    HState#{state := State#{handler_state := NewHandlerState}}.

%%
%% utils
%%
-spec rpc_server_options(options()) ->
    raft_rpc_server:options().
rpc_server_options(Options) ->
    maps:with([rpc, logger], Options).

-spec lists_unzip_element(pos_integer(), [tuple()]) ->
    _.
lists_unzip_element(N, List) ->
    lists:map(
        fun(Tuple) ->
            element(N, Tuple)
        end,
        List
    ).

-spec maps_mapfoldl(fun((Key, Value, Acc) -> {NewValue, Acc}), Acc, #{Key => Value}) ->
    {#{Key => NewValue}, Acc}.
maps_mapfoldl(Fun, InitialAcc, Map) ->
    maps_mapfoldl_to_map(
        lists:mapfoldl(
            fun({Key, Value}, Acc) ->
                {NewValue, NewAcc} = Fun(Key, Value, Acc),
                {{Key, NewValue}, NewAcc}
            end,
            InitialAcc,
            maps:to_list(Map)
        )
    ).

-spec maps_mapfoldl_to_map({[{Key, Value}], Acc}) ->
    {#{Key => Value}, Acc}.
maps_mapfoldl_to_map({List, Acc}) ->
    {maps:from_list(List), Acc}.

%%
%% formatting
%%
-spec format_state({handler(), options()}, state()) ->
    list().
format_state({Handler, Options}, State = #{current_term := Term, role := Role}) ->
    HState      = hstate(Handler, Options, undefined, State),
    Commit      = commit_index(HState),
    LastApplied = last_applied(HState),
    LastLog     = last_log_index(HState),
    LastLogEntry = last_log_entry(HState),
    io_lib:format("~9999p ~9999p ~9999p ~9999p ~9999p ~9999p", [ext_role(Role), Term, LastLog, Commit, LastApplied, LastLogEntry]).

-spec format_self_endpoint({handler(), options()}, state()) ->
    list().
format_self_endpoint({_, #{self := Self}}, _) ->
    raft_rpc:format_endpoint(Self).

-spec
ext_role(role()             ) -> ext_role().
ext_role({leader   , _     }) -> leader;
ext_role({follower , Leader}) -> {follower, Leader};
ext_role({candidate, _     }) -> candidate.
