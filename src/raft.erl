%%%
%%% Основная идея в том, чтобы сделать максимально лёгкую в стиле OTP реализацию Raft.
%%% В процессе написания была попытка усидеть на 3х стульях сразу:
%%%  - с одной стороны по максимуму придерживаться терминологии орининального документа (https://raft.github.io/raft.pdf);
%%%  - с другой делать в стили OTP.
%%%  - с третьей сделать наиболее обобщённо и гибко
%%%
%%% TODO:
%%%  - RPC на command и проксирование его (и откуда вернётся ответ туда и посылается следующий запрос)
%%%  - идемпотентость команд
%%%  - убрать gen_server
%%%  - handle info + timeout
%%%  -
%%%
%%% Вопросы:
%%%  - нужен ли тут дедлайн или его перенести на уровень дальше
%%%  -
-module(raft).

%% API
-export_type([timeout_ms  /0]).
-export_type([timestamp_ms/0]).
-export_type([options     /0]).
-export_type([raft_term   /0]).
-export_type([index       /0]).
-export_type([command     /0]).
-export_type([log_entry   /0]).
-export_type([handler     /0]).
-export_type([storage     /0]).
-export_type([ext_role    /0]).
-export_type([ext_state   /0]).
-export([start_link/5]).
-export([send_sync_command /3]).
-export([send_async_command/3]).
-export([send_response_command/3]).
-export([recv_response_command/2]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% API
%%
-type timeout_ms  () :: non_neg_integer().
-type timestamp_ms() :: non_neg_integer().

% ниже важный коментарий!
% election_timeout << broadcast_timeout << mean_time_between_failures
% election_timeout ~ 2 * broadcast_timeout
-type options() ::#{
    self              => raft_rpc:endpoint(),
    others            => ordsets:ordset(raft_rpc:endpoint()),
    election_timeout  => timeout_ms() | {From::timeout_ms(), To::timeout_ms()},
    broadcast_timeout => timeout_ms()
}.

-type raft_term() :: non_neg_integer().
-type index    () :: non_neg_integer().
-type command  () :: raft_storage:command().
-type log_entry() :: {raft_term(), command()}.
-type handler  () :: mg_utils:mod_opts().

-type storage() :: #{
    storage => raft_storage:storage(),
    system  => raft_storage:state(),
    handler => raft_storage:state(),
    log     => raft_storage:state()
}.
-type ext_role() :: leader | {follower, undefined | raft_rpc:endpoint()} | candidate.
-type ext_state() :: #{
    rpc     => raft_rpc:rpc(),
    options => options(),
    storage => storage(),
    role    => ext_role()
}.


%%
%% behaviour
%%
-callback handle_sync_command(_, raft_storage:command(), ext_state()) ->
    raft_storage:state().

-callback handle_async_command(_, raft_storage:command(), ext_state()) ->
    ok.

%%

%% Версия без регистрации не имеет смысла (или я не прав? похоже, что прав, работа по пидам смысла не имеет).
%% А как же общение по RPC? Тут похоже обратное — версия с регистрацией не нужна
%% (но нужно всё-таки как-то зарегистрировать процесс при erl rpc).
-spec start_link(mg_utils:gen_reg_name(), handler(), raft_storage:storage(), raft_rpc:rpc(), options()) ->
    mg_utils:gen_start_ret().
start_link(RegName, Handler, Storage, RPC, Options) ->
    gen_server:start_link(RegName, ?MODULE, {Handler, Storage, RPC, Options}, []).

%% TODO sessions
-spec send_sync_command(raft_rpc:rpc(), [raft_rpc:endpoint()], raft_storage:command()) ->
    ok.
send_sync_command(RPC, Cluster, Command) ->
    raft_rpc:send(RPC, raft_rpc:get_nearest(RPC, Cluster), {external, {sync_command, Command}}).

-spec send_async_command(raft_rpc:rpc(), [raft_rpc:endpoint()], raft_storage:command()) ->
    ok.
send_async_command(RPC, Cluster, Command) ->
    raft_rpc:send(RPC, raft_rpc:get_nearest(RPC, Cluster), {external, {async_command, Command}}).

-spec send_response_command(raft_rpc:rpc(), raft_rpc:endpoint(), command()) ->
    ok.
send_response_command(RPC, To, Command) ->
    raft_rpc:send(RPC, To, {external, {response_command, Command}}).

-spec recv_response_command(raft_rpc:rpc(), timeout()) ->
    {ok, command()} | timeout.
recv_response_command(RPC, Timeout) ->
    receive
        {raft_rpc, Data} ->
            {external, {response_command, Command}} = raft_rpc:recv(RPC, Data),
            {ok, Command}
    after Timeout ->
        timeout
    end.

%%
%% gen_server callbacks
%%
% нужно написать как работают heartbeat
-type follower_state () :: {Next::index(), Match::index(), Heartbeat::timestamp_ms(), timestamp_ms()}.
-type followers_state() :: #{raft_rpc:endpoint() => follower_state()}.
-type role() ::
      {leader   , followers_state()}
    | {follower , MyLeader::(raft_rpc:endpoint() | undefined)}
    | {candidate, VotedFrom::ordsets:ordset(raft_rpc:endpoint())}
.

-type state() :: #{
    options       => options(),
    timer         => timestamp_ms() | undefined,

    % текущая роль и специфичные для неё данные
    role          => role(),

    % текущий терм (вообще, имхо, слово "эпоха" тут более подходящее)
    current_term  => raft_term(),

    rpc           => raft_rpc:rpc(),
    handler       => handler(),

    % модуль и состояния хранилища
    storage       => storage()
}.

-spec init(_) ->
    mg_utils:gen_server_init_ret(state()).
init(Args) ->
    NewState = new_state(Args),
    {ok, NewState, get_timer_timeout(NewState)}.

%%
%% Calls
%%
-spec handle_call(_Call, mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).
handle_call(_, _From, State) ->
    {noreply, State, get_timer_timeout(State)}.

-spec handle_cast(_, state()) ->
    mg_utils:gen_server_handle_cast_ret(state()).
handle_cast(_, State) ->
    {noreply, State, get_timer_timeout(State)}.

-spec handle_info(_Info, state()) ->
    mg_utils:gen_server_handle_info_ret(state()).
handle_info(timeout, State) ->
    NewState = handle_timeout(State),
    ok = log_timeout(State, NewState),
    {noreply, NewState, get_timer_timeout(NewState)};
handle_info({raft_rpc, Data}, State = #{rpc := RPC}) ->
    Message = raft_rpc:recv(RPC, Data),
    NewState = handle_rpc(Message, State),
    ok = log_income_message(Message, State, NewState),
    {noreply, NewState, get_timer_timeout(NewState)}.

-spec code_change(_, state(), _) ->
    mg_utils:gen_server_code_change_ret(state()).
code_change(_, State, _) ->
    {ok, State}.

-spec terminate(_Reason, state()) ->
    ok.
terminate(_, _) ->
    ok.

%%
%% common handlers
%%
-spec new_state({handler(), raft_storage:storage(), raft_rpc:rpc(), options()}) ->
    state().
new_state({Handler, StorageModOpts, RPC, Options}) ->
    State =
        #{
            options       => Options,
            timer         => undefined,
            role          => {follower, undefined},
            current_term  => 0,
            rpc           => RPC,
            handler       => Handler,
            storage       => init_storage(StorageModOpts)
        },
    become_follower(State#{current_term := get_term_from_log(last_log_index(State), State)}).


-spec init_storage(raft_storage:storage()) ->
    storage().
init_storage(StorageModOpts) ->
    #{
        storage => StorageModOpts,
        system  => raft_storage:init(StorageModOpts, system ),
        handler => raft_storage:init(StorageModOpts, handler),
        log     => raft_storage:init(StorageModOpts, log    )
    }.

-spec handle_timeout(state()) ->
    state().
handle_timeout(State = #{role := {leader, _}}) ->
    try_send_append_entries(State);
handle_timeout(State = #{role := _}) ->
    become_candidate(State).

-spec handle_rpc(raft_rpc:message(), state()) ->
    state().
handle_rpc({external, Msg}, State) ->
    handle_external_rpc(Msg, State);
handle_rpc({internal, Msg}, State) ->
    handle_internal_rpc(Msg, State).

%%
%% external rpc handlers
%%
-spec handle_external_rpc(raft_rpc:external_message(), state()) ->
    state().
handle_external_rpc({sync_command, Command}, State) ->
    handle_sync_command_rpc(Command, State);
handle_external_rpc({async_command, Command}, State) ->
    ok = handle_async_command_rpc(Command, State),
    State.

-spec handle_sync_command_rpc(raft_storage:command(), state()) ->
    state().
handle_sync_command_rpc(Command, State = #{role := {leader, _}, current_term := CurrentTerm}) ->
    NewState = append_log_entries(last_log_index(State), [{CurrentTerm, Command}], State),
    try_send_append_entries(NewState);
handle_sync_command_rpc(Command, State = #{role := {follower, Leader}}) when Leader =/= undefined ->
    ok = send(Leader, {external, {sync_command, Command}}, State),
    State;
handle_sync_command_rpc(_, State = #{role := _}) ->
    State.

-spec handle_async_command_rpc(raft_storage:command(), state()) ->
    ok.
handle_async_command_rpc(Command, State) ->
    handle_async_command(Command, State).

%%
%% internal rpc handlers
%%
-spec handle_internal_rpc(raft_rpc:internal_message(), state()) ->
    state().
handle_internal_rpc(Msg = {_, _, _, _, Term}, State = #{current_term := CurrentTerm}) when Term > CurrentTerm ->
    handle_internal_rpc(Msg, become_follower(State#{current_term := Term}));
handle_internal_rpc({_, _, _, _, Term}, State = #{current_term := CurrentTerm}) when Term < CurrentTerm ->
    State;
handle_internal_rpc({request, Type, Body, From, _}, State) ->
    {Result, NewState} = handle_rpc_request(Type, Body, From, State),
    ok = send_int_response(From, Type, Result, NewState),
    NewState;
handle_internal_rpc({{response, Succeed}, Type, _, From, _}, State) ->
    handle_rpc_response(Type, From, Succeed, State).

-spec handle_rpc_request(raft_rpc:internal_message_type(), raft_rpc:message_body(), raft_rpc:endpoint(), state()) ->
    {boolean(), state()}.
handle_rpc_request(request_vote, _, Candidate, State = #{role := {follower, undefined}}) ->
    % Голосую!
    {true, schedule_election_timer(update_follower(Candidate, State))};
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
        case {Result, CommitIndex > last_log_index(State1)} of
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
    {NextIndex, MatchIndex, Heartbeat, _} = maps:get(From, FollowersState),
    NewFollowerState =
        case Succeed of
            true ->
                {NextIndex, NextIndex - 1 , Heartbeat, 0};
            false ->
                {NextIndex - 1, MatchIndex, Heartbeat, 0}
        end,
    try_send_append_entries(
        try_commit(
            last_log_index(State),
            commit_index  (State),
            update_leader_follower(From, NewFollowerState, State)
        )
    ).

%%

-spec try_append_to_log(undefined | log_entry(), [log_entry()], state()) ->
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
    case has_quorum(erlang:length(Votes), State) of
        true ->
            become_leader(State);
        false ->
            State
    end.

-spec try_commit(index(), index(), state()) ->
    state().
try_commit(IndexN, CommitIndex, State = #{current_term := CurrentTerm}) ->
    % If there exists an N such that N > commitIndex, a majority
    % of matchIndex[i] ≥ N, and log[N].term == currentTerm:
    % set commitIndex = N (§5.3, §5.4)
    case IndexN > CommitIndex andalso get_term_from_log(IndexN, State) =:= CurrentTerm of
        true ->
            case is_replicated(IndexN, State) of
                true  -> commit(IndexN, State);
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
                fun({_, MatchIndex, _, _}) ->
                    MatchIndex >= Index
                end,
                maps:values(FollowersState)
            )
        ),
    has_quorum(NumberOfReplicas, State).

-spec has_quorum(non_neg_integer(), state()) ->
    boolean().
has_quorum(N, #{options := #{others := Others}}) ->
    N >= (erlang:length(Others) + 1) div 2 + 1.

-spec new_followers_state(state()) ->
    followers_state().
new_followers_state(State = #{options := #{others := Others}}) ->
    maps:from_list([{To, new_follower_state(State)} || To <- Others]).

-spec new_follower_state(state()) ->
    follower_state().
new_follower_state(State) ->
    {last_log_index(State) + 1, 0, 0, 0}.

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
    NewState.

-spec
become_leader(state()                          ) -> state().
become_leader(State = #{role := {candidate, _}}) -> become_leader_(State).

-spec become_leader_(state()) ->
    state().
become_leader_(State) ->
    try_send_append_entries(set_role({leader, new_followers_state(State)}, State)).

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
send_request_votes(State = #{options := #{others := Others}}) ->
    LastIndex = last_log_index(State),
    lists:foreach(
        fun(To) ->
            ok = send_int_request(To, request_vote, {LastIndex, get_term_from_log(LastIndex, State)}, State)
        end,
        Others
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
try_send_one_append_entries(To, FollowerState, State = #{options := #{broadcast_timeout := Timeout}}) ->
    LastLogIndex = last_log_index(State),
    Now = now_ms(),
    case FollowerState of
        {NextIndex, MatchIndex, Heartbeat, _}
            when Heartbeat =< Now
            ->
                ok = send_append_entries(To, NextIndex, MatchIndex, State),
                {NextIndex, MatchIndex, Now + Timeout, Now + Timeout};
        {NextIndex, MatchIndex, _, RPCTimeout}
            when    NextIndex   =<  LastLogIndex
            andalso RPCTimeout  =<  Now
            ->
                NewNextIndex =
                    case MatchIndex =:= (NextIndex - 1) of
                        true  -> LastLogIndex + 1;
                        false -> NextIndex
                    end,
                ok = send_append_entries(To, NewNextIndex, MatchIndex, State),
                {NewNextIndex, MatchIndex, Now + Timeout, Now + Timeout};
        {_, _, _, _}   ->
            FollowerState
    end.

-spec send_append_entries(raft_rpc:endpoint(), index(), index(), state()) ->
    ok.
send_append_entries(To, NextIndex, MatchIndex, State) ->
    Prev = {get_term_from_log(MatchIndex, State), MatchIndex},
    Body = {Prev, log_entries(MatchIndex, NextIndex, State), commit_index(State)},
    send_int_request(To, append_entries, Body, State).

-spec send_int_request(raft_rpc:endpoint(), raft_rpc:internal_message_type(), raft_rpc:message_body(), state()) ->
    ok.
send_int_request(To, Type, Body, State = #{options := #{self := Self}, current_term := Term}) ->
    send(To, {internal, {request, Type, Body, Self, Term}}, State).

-spec send_int_response(raft_rpc:endpoint(), raft_rpc:internal_message_type(), boolean(), state()) ->
    ok.
send_int_response(To, Type, Result, State = #{options := #{self := Self}, current_term := CurrentTerm}) ->
    send(To, {internal, {{response, Result}, Type, undefined, Self, CurrentTerm}}, State).

-spec send(raft_rpc:endpoint(), raft_rpc:message(), state()) ->
    ok.
send(To, Message, State = #{rpc := RPC}) ->
    ok = log_outgoing_message(Message, To, State),
    raft_rpc:send(RPC, To, Message).

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
schedule_next_heartbeat_timer(State = #{role := {leader, FollowersState}}) ->
    NextHeardbeat = lists:min(element(3, lists_unzip4(maps:values(FollowersState)))),
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
    rand:uniform(To - From) + From;
randomize_timeout(Const) ->
    Const.

%%

-spec commit(index(), state()) ->
    state().
commit(Index, State) ->
    try_apply_commited(set_commit_index(Index, State)).

-spec try_apply_commited(state()) ->
    state().
try_apply_commited(State) ->
    LastApplied = last_applied(State),
    case LastApplied < commit_index(State) of % больше он быть не может!
        true ->
            try_apply_commited(apply_commited(LastApplied + 1, State));
        false ->
            State
    end.

-spec apply_commited(index(), state()) ->
    state().
apply_commited(Index, State) ->
    Command = element(2, log_entry(Index, State)),
    set_last_applied(Index, handle_sync_command(Command, State)).

-spec last_applied(state()) ->
    index().
last_applied(#{storage := Storage}) ->
    set_default(storage_get_one(system, last_applied, Storage), 0).

-spec set_last_applied(index(), state()) ->
    state().
set_last_applied(Index, State = #{storage := Storage}) ->
    State#{storage := storage_put(system, [{last_applied, Index}], Storage)}.

-spec commit_index(state()) ->
    index().
commit_index(#{storage := Storage}) ->
    set_default(storage_get_one(system, commit_index, Storage), 0).

-spec set_commit_index(index(), state()) ->
    state().
set_commit_index(Index, State = #{storage := Storage}) ->
    State#{storage := storage_put(system, [{commit_index, Index}], Storage)}.

-spec last_log_index(state()) ->
    index().
last_log_index(#{storage := Storage}) ->
    set_default(storage_get_one(system, last_log_index, Storage), 0).

-spec set_last_log_index(index(), state()) ->
    state().
set_last_log_index(Index, State = #{storage := Storage}) ->
    State#{storage := storage_put(system, [{last_log_index, Index}], Storage)}.

-spec try_set_last_log_index(index(), state()) ->
    state().
try_set_last_log_index(Index, State) ->
    set_last_log_index(erlang:max(Index, last_log_index(State)), State).

-spec log_entry(index(), state()) ->
    log_entry().
log_entry(Index, #{storage := Storage}) ->
    set_default(storage_get_one(log, Index, Storage), {0, undefined}).

-spec log_entries(index(), index(), state()) ->
    [log_entry()].
log_entries(From, To, #{storage := Storage}) ->
    storage_get(log, lists:seq(From, To), Storage).

-spec append_log_entries(index(), [log_entry()], state()) ->
    state().
append_log_entries(PrevIndex, Entries, State = #{storage := Storage}) ->
    LastIndex = PrevIndex + erlang:length(Entries),
    Values    = lists:zip(lists:seq(PrevIndex + 1, LastIndex), Entries),
    try_set_last_log_index(LastIndex, State#{storage := storage_put(log, Values, Storage)}).

-spec set_default(undefined | Value, Value) ->
    Value.
set_default(undefined, Default) ->
    Default;
set_default(Value, _) ->
    Value.

%%
%% interaction with storage
%%
-spec storage_put(raft_storage:type(), [{raft_storage:key(), raft_storage:value()}], storage()) ->
    storage().
storage_put(Type, Values, Storage = #{storage := StorageModOpts}) ->
    StorageState = maps:get(Type, Storage),
    NewStorageState = raft_storage:put(StorageModOpts, Values, StorageState),
    Storage#{Type := NewStorageState}.

-spec storage_get(raft_storage:type(), [raft_storage:key()], storage()) ->
    [raft_storage:value()].
storage_get(Type, Keys, Storage = #{storage := StorageModOpts}) ->
    raft_storage:get(StorageModOpts, Keys, maps:get(Type, Storage)).

-spec storage_get_one(raft_storage:type(), raft_storage:key(), storage()) ->
    raft_storage:value().
storage_get_one(Type, Key, Storage = #{storage := StorageModOpts}) ->
    raft_storage:get_one(StorageModOpts, Key, maps:get(Type, Storage)).

%%
%% interaction with handler
%%
%% исполняется на всех в определённой последовательности и может менять стейт
-spec handle_sync_command(raft_storage:command(), state()) ->
    state().
handle_sync_command(Command, State = #{handler := Handler, storage := Storage}) ->
    NewHandlerSState = mg_utils:apply_mod_opts(Handler, handle_sync_command, [Command, ext_state(State)]),
    State#{storage := Storage#{handler := NewHandlerSState}}.

%% исполняется на любой ноде, очерёдность не определена, не может менять стейт
-spec handle_async_command(raft_storage:command(), state()) ->
    ok.
handle_async_command(Command, State = #{handler := Handler}) ->
    _ = mg_utils:apply_mod_opts(Handler, handle_async_command, [Command, ext_state(State)]),
    ok.

-spec ext_state(state()) ->
    ext_state().
ext_state(#{rpc := RPC, options := Options, storage := Storage, role := Role}) ->
    #{
        rpc     => RPC,
        options => Options,
        storage => Storage,
        role    => ext_role(Role)
    }.

-spec
ext_role(role()             ) -> ext_role().
ext_role({leader   , _     }) -> leader;
ext_role({follower , Leader}) -> {follower, Leader};
ext_role({candidate, _     }) -> candidate.

%%
%% utils
%%
-spec lists_unzip4([{A, B, C, D}]) ->
    {[A], [B], [C], [D]}.
lists_unzip4(List) ->
    lists:foldr(
        fun({A, B, C, D}, {AccA, AccB, AccC, AccD}) ->
            {[A | AccA], [B | AccB], [C | AccC], [D | AccD]}
        end,
        {[], [], [], []},
        List
    ).

%%
%% log
%%
-spec log_timeout(state(), state()) ->
    ok.
log_timeout(StateBefore, StateAfter) ->
    ct:pal("~s~ntimeout~n~s~n~s",
        [format_id(StateBefore), format_state(StateBefore), format_state(StateAfter)]).

-spec log_income_message(raft_rpc:message(), state(), state()) ->
    ok.
log_income_message(Message, StateBefore, StateAfter) ->
    ct:pal("~s~nmessage ~s~n~s~n~s",
        [format_id(StateBefore), format_message(Message), format_state(StateBefore), format_state(StateAfter)]).

-spec log_outgoing_message(raft_rpc:message(), raft_rpc:endpoint(), state()) ->
    ok.
log_outgoing_message(Message, To, State) ->
    ct:pal("~s~n-> ~9999p ~s~n~s", [format_id(State), To, format_message(Message), format_state(State)]).

-spec format_state(state()) ->
    list().
format_state(State = #{current_term := Term, role := Role}) ->
    Commit      = commit_index(State),
    LastApplied = last_applied(State),
    LastLog     = last_log_index(State),
    Log         = log_entries(1, LastLog, State),
    io_lib:format("~9999p ~9999p ~9999p ~9999p ~9999p ~9999p", [ext_role(Role), Term, LastLog, Commit, LastApplied, Log]).

-spec format_id(state()) ->
    list().
format_id(#{options := #{self := Self}}) ->
    io_lib:format("~9999p ~9999p", [Self, self()]).

-spec format_message(raft_rpc:message()) ->
    list().
format_message(Message) ->
    io_lib:format("~9999p", [Message]).


% b
% <- {internal,{request,append_entries,{{0,0},[{1,{get_leader,<0.192.0>}}],0},c,1}}
% {follower,c} 1 0 0 0 []
% {follower,c} 1 1 0 0 [{1,{get_leader,<0.192.0>}}]

% b
% -> a {internal,{request,append_entries,{{0,0},[{1,{get_leader,<0.192.0>}}],0},c,1}}
% {follower,c} 1 0 0 0 []
