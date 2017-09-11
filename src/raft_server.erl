%%%
%%% Основная идея в том, чтобы сделать максимально лёгкую в стиле OTP реализацию Raft.
%%% В процессе написания была попытка усидеть на 3х стульях сразу:
%%%  - с одной стороны по максимуму придерживаться терминологии орининального документа (https://raft.github.io/raft.pdf);
%%%  - с другой делать в стили OTP.
%%%  - с третьей сделать наиболее обобщённо и гибко
%%%
%%% TODO:
%%%  - RPC на command и проксирование его (и откуда вернётся ответ туда и посылается следующий запрос)
%%%  - убрать gen_server
%%%  - идемпотентость команд
%%%  - handle info + timeout
%%%  -
%%%
%%% Вопросы:
%%%  - нужен ли тут дедлайн или его перенести на уровень дальше
%%%  -
-module(raft_server).

%% API
-export([start_link/5]).
-export([get_leader/1]).
% -export([send_command/3]).

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
    self              => raft_server_rpc:endpoint(),
    others            => ordsets:ordset(raft_server_rpc:endpoint()),
    election_timeout  => {From::timeout_ms(), To::timeout_ms()},
    broadcast_timeout => timeout_ms()
}.

-type raft_term() :: non_neg_integer().
-type index    () :: non_neg_integer().
-type command  () :: term().
-type log_entry() :: {raft_term(), command()}.



%% Версия без регистрации не имеет смысла (или я не прав? похоже, что прав, работа по пидам смысла не имеет).
%% А как же общение по RPC? Тут похоже обратное — версия с регистрацией не нужна
%% (но нужно всё-таки как-то зарегистрировать процесс при erl rpc).
-spec start_link(mg_utils:gen_reg_name(), raft_server_fsm:fsm(), raft_server_storage:storage(), raft_server_rpc:rpc(), options()) ->
    mg_utils:gen_start_ret().
start_link(RegName, FSM, Storage, RPC, Options) ->
    gen_server:start_link(RegName, ?MODULE, {FSM, Storage, RPC, Options}, []).

-spec get_leader(mg_utils:gen_ref()) ->
    {ok, raft_server_rpc:endpoint()} | undefined.
get_leader(Ref) ->
    gen_server:call(Ref, get_leader).

%%
%% gen_server callbacks
%%
-type follower_state () :: {Next::index(), Match::index(), Heartbeat::timestamp_ms()}.
-type followers_state() :: #{raft_server_rpc:endpoint() => follower_state()}.
-type role() ::
      {leader   , followers_state()}
    | {follower , MyLeader::(raft_server_rpc:endpoint() | undefined)}
    | {candidate, VotedFrom::ordsets:ordset(raft_server_rpc:endpoint())}
.

-type state() :: #{
    options       => options(),
    timer         => timestamp_ms(),

    % текущая роль и специфичные для неё данные
    role          => role(),

    % текущий терм (вообще, имхо, слово "эпоха" тут более подходящее)
    current_term  => raft_term(),

    rpc           => raft_server_rpc:rpc(),
    fsm           => raft_server_fsm:fsm(),

    % модуль и состояние хранилища
    storage       => raft_server_storage:storage(),
    storage_state => raft_server_storage:state()
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
handle_call({send_command, Command}, _From, State) ->
    NewState = handle_command(Command, State),
    {reply, ok, NewState, get_timer_timeout(NewState)};
handle_call(get_leader, _From, State) ->
    Resp =
        case State of
            #{role := {leader, _}, options := #{self := Leader}} ->
                {ok, Leader};
            #{role := {follower, undefined}} ->
                undefined;
            #{role := {follower, Leader}} ->
                {ok, Leader};
            #{role := {candidate, _}} ->
                undefined
        end,
    {reply, Resp, State, get_timer_timeout(State)}.

-spec handle_cast(_, state()) ->
    mg_utils:gen_server_handle_cast_ret(state()).
handle_cast(_, State) ->
    {noreply, State, get_timer_timeout(State)}.

-spec handle_info(_Info, state()) ->
    mg_utils:gen_server_handle_info_ret(state()).
handle_info(timeout, State) ->
    NewState = handle_timeout(State),
    {noreply, NewState, get_timer_timeout(NewState)};
handle_info({raft_server_rpc, Data}, State = #{rpc := RPC}) ->
    NewState = handle_rpc(raft_server_rpc:recv(RPC, Data), State),
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
-spec new_state({raft_server_fsm:fsm(), raft_server_storage:storage(), raft_server_rpc:rpc(), options()}) ->
    state().
new_state({FSM, Storage, RPC, Options}) ->
    State =
        #{
            options       => Options,
            timer         => undefined,
            role          => {follower, undefined},
            current_term  => 0,
            rpc           => RPC,
            fsm           => FSM,
            storage       => Storage,
            storage_state => raft_server_storage:init(Storage)
        },
    become_follower(State#{current_term := get_term_from_log(last_log_index(State), State)}).

-spec handle_timeout(state()) ->
    state().
handle_timeout(State = #{role := {leader, _}}) ->
    try_send_append_entries(State);
handle_timeout(State = #{role := _}) ->
    become_candidate(State).

-spec handle_command(command(), state()) ->
    {{reply, _} | noreply, state()}.
handle_command(Command, State = #{role := {leader, _}, current_term := CurrentTerm}) ->
    % добавить результат в лог (если запись уже есть, то ответить (как это узнать? чем?))
    % запустить процесс репликации (а если он уже идёт?)
    % в данный момент и при получении каждого ответа проверять, не прошла ли репликация на мажорити
    % когда пройдёт — применить к автомату (в процессе подвинуть указатель последнего апплая) и ответить
    NewState = append_log_entries(last_log_index(State), [{CurrentTerm, Command}], State),
    % TODO reply after commit
    {{reply, {ok, ok}}, try_send_append_entries(NewState)};
handle_command(_, State = #{role := {candidate, _}}) ->
    {{reply, {error, no_leader}}, State};
handle_command(_, State = #{role := {follower, undefined}}) ->
    {{reply, {error, no_leader}}, State};
handle_command(_, State = #{role := {follower, Leader}}) ->
    {{reply, {error, {leader_is, Leader}}}, State}.



-spec handle_rpc(raft_server_rpc:message(), state()) ->
    state().
% handle_rpc({client_request, Command, From}, State) ->
%     case handle_rpc_client_command(Command, From, State) of
%         {forward, To, NewState} ->
%             send_client_request(Command, From, To, NewState);
%         {nop, NewState} ->
%             NewState
%     end;
handle_rpc(Msg = {_, _, _, _, Term}, State = #{current_term := CurrentTerm}) when Term > CurrentTerm ->
    handle_rpc(Msg, become_follower(State#{current_term := Term}));
handle_rpc({_, _, _, _, Term}, State = #{current_term := CurrentTerm}) when Term < CurrentTerm ->
    State;
handle_rpc({request, Type, Body, From, _}, State) ->
    {Result, NewState} = handle_rpc_request(Type, Body, From, State),
    ok = send_response(From, Type, Result, NewState),
    NewState;
handle_rpc({{response, Succeed}, Type, From, _}, State) ->
    handle_rpc_response(Type, From, Succeed, State).

% do_client_reply_action({reply, To, Msg}, State) ->
%     send(To, Msg, State);
% do_client_reply_action({forward, To, Msg}, State) ->
%     send(To, Msg, State).
% do_client_reply_action(nop, State) ->
%     State.

% -spec handle_rpc_client_command(command(), raft_server_rpc:endpoint(), state()) ->
%     {nop, state()} | {forward, raft_server_rpc:endpoint(), state()}.
% handle_rpc_client_command(Command, From, State = #{role := {leader, _}, current_term := CurrentTerm}) ->
%     % добавить результат в лог (если запись уже есть, то ответить (как это узнать? чем?))
%     % запустить процесс репликации (а если он уже идёт?)
%     % в данный момент и при получении каждого ответа проверять, не прошла ли репликация на мажорити
%     % когда пройдёт — применить к автомату (в процессе подвинуть указатель последнего апплая) и ответить
%     NewState = append_log_entries(last_log_index(State), [{CurrentTerm, Command}], State),
%     {nop, try_send_append_entries(NewState)};
% handle_rpc_client_command(_, _, State = #{role := {follower, Leader}}) when Leader =/= undefined ->
%     {forward, Leader, State};
% handle_rpc_client_command(_, _, State = #{role := _}) ->
%     {nop, State}.

-spec handle_rpc_request(raft_server_rpc:message_type(), raft_server_rpc:message_body(), raft_server_rpc:endpoint(), state()) ->
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
    handle_rpc_request(append_entries, Body, Leader, update_follower(Leader, State));
handle_rpc_request(append_entries, {Prev, Entries, CommitIndex}, _, State0 = #{role := {follower, _}}) ->
    {Result, State1} = try_append_to_log(Prev, Entries, State0),
    State2 =
        case {Result, CommitIndex > last_log_index(State1)} of
            {true , true} -> commit(CommitIndex, State1);
            {_    , _   } -> State1
        end,
    {Result, schedule_election_timer(State2)}.

-spec handle_rpc_response(raft_server_rpc:message_type(), raft_server_rpc:endpoint(), boolean(), state()) ->
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

-spec add_vote(raft_server_rpc:endpoint(), state()) ->
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

-spec update_leader_follower(raft_server_rpc:endpoint(), follower_state(), state()) ->
    state().
update_leader_follower(Follower, NewFollowerState, State = #{role := {leader, FollowersState}}) ->
    update_leader(FollowersState#{Follower := NewFollowerState}, State).

-spec update_follower(raft_server_rpc:endpoint(), state()) ->
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
            ok = send_request(To, request_vote, {LastIndex, get_term_from_log(LastIndex, State)}, State)
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

-spec try_send_one_append_entries(raft_server_rpc:endpoint(), follower_state(), state()) ->
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

-spec send_append_entries(raft_server_rpc:endpoint(), index(), index(), state()) ->
    ok.
send_append_entries(To, NextIndex, MatchIndex, State) ->
    Prev = {get_term_from_log(MatchIndex, State), MatchIndex},
    Body = {Prev, log_entries(MatchIndex, NextIndex, State), commit_index(State)},
    send_request(To, append_entries, Body, State).

-spec send_request(raft_server_rpc:endpoint(), raft_server_rpc:message_type(), raft_server_rpc:message_body(), state()) ->
    ok.
send_request(To, Type, Body, State = #{options := #{self := Self}, current_term := Term}) ->
    send(To, {request, Type, Body, Self, Term}, State).

-spec send_response(raft_server_rpc:endpoint(), boolean(), raft_server_rpc:message_body(), state()) ->
    ok.
send_response(To, Type, Result, State = #{options := #{self := Self}, current_term := CurrentTerm}) ->
    send(To, {{response, Result}, Type, Self, CurrentTerm}, State).

-spec send(raft_server_rpc:endpoint(), raft_server_rpc:message(), state()) ->
    ok.
send(To, Message, #{rpc := RPC}) ->
    raft_server_rpc:send(RPC, To, Message).

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
%% interaction with storage
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
            ok
    end.

-spec apply_commited(index(), state()) ->
    state().
apply_commited(Index, State = #{fsm := FSM, storage_state := SState}) ->
    Command = element(2, log_entry(Index, State)),
    NewSState = raft_server_fsm:handle_command(FSM, Command, SState),
    set_last_applied(Index, State#{storage_state => NewSState}).

-spec last_applied(state()) ->
    index().
last_applied(#{storage := Mod, storage_state := SState}) ->
    set_default(raft_server_storage:get_one(Mod, main, last_applied, SState), 0).

-spec set_last_applied(index(), state()) ->
    index().
set_last_applied(Index, #{storage := Mod, storage_state := SState}) ->
    raft_server_storage:put(Mod, main, [{last_applied, Index}], SState).

-spec commit_index(state()) ->
    index().
commit_index(#{storage := Mod, storage_state := SState}) ->
    set_default(raft_server_storage:get_one(Mod, main, commit_index, SState), 0).

-spec set_commit_index(index(), state()) ->
    index().
set_commit_index(Index, #{storage := Mod, storage_state := SState}) ->
    raft_server_storage:put(Mod, main, [{commit_index, Index}], SState).

-spec last_log_index(state()) ->
    index().
last_log_index(#{storage := Mod, storage_state := SState}) ->
    set_default(raft_server_storage:get_one(Mod, main, last_log_index, SState), 0).

-spec set_last_log_index(index(), state()) ->
    state().
set_last_log_index(Index, State = #{storage := Mod, storage_state := SState}) ->
    State#{storage_state => raft_server_storage:put(Mod, main, [{last_log_index, Index}], SState)}.

-spec try_set_last_log_index(index(), state()) ->
    state().
try_set_last_log_index(Index, State) ->
    set_last_log_index(erlang:max(Index, last_log_index(State)), State).

-spec log_entry(index(), state()) ->
    log_entry().
log_entry(Index, #{storage := Mod, storage_state := SState}) ->
    set_default(raft_server_storage:get_one(Mod, log, Index, SState), {0, undefined}).

-spec log_entries(index(), index(), state()) ->
    [log_entry()].
log_entries(From, To, #{storage := Mod, storage_state := SState}) ->
    raft_server_storage:get(Mod, log, lists:seq(From, To), SState).

-spec append_log_entries(index(), [log_entry()], state()) ->
    state().
append_log_entries(PrevIndex, Entries, State = #{storage := Mod, storage_state := SState}) ->
    LastIndex = PrevIndex + erlang:length(Entries),
    Values    = lists:zip(lists:seq(PrevIndex + 1, LastIndex), Entries),
    NewSState = raft_server_storage:put(Mod, log, Values, SState),
    try_set_last_log_index(LastIndex, State#{storage_state := NewSState}).

-spec set_default(undefined | Value, Value) ->
    Value.
set_default(undefined, Default) ->
    Default;
set_default(Value, _) ->
    Value.

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
