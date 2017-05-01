%%%
%%% Основная идея в том, чтобы сделать максимально лёгкую в стиле OTP реализацию Raft.
%%% В процессе написания была попытка усидеть на 2х стульях сразу:
%%%  - с одной стороны по максимуму придерживаться терминологии орининального документа (https://raft.github.io/raft.pdf);
%%%  - с другой делать в стили OTP.
%%%
%%% Отличия от оригинала:
%%%  - send_command_rpc с проксированием на мастер
%%%  - отсутствие понятия fsm, apply и тд, это всё забота backend'а
%%%  -
%%%
%%% TODO:
%%%  - RPC на command и проксирование его (и откуда вернётся ответ туда и посылается следующий запрос)
%%%  - идемпотентость команд?
%%%  - handle info + timeout
%%%  - SCTP RPC
%%%  -
%%%
-module(raft_server).

%% API
-export([start_link  /4]).
-export([send_command/3]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% API
%%
-type timeout_ms  () :: non_neg_integer().
-type timestamp_ms() :: non_neg_integer().

% election_timeout << broadcast_timeout << mean_time_between_failures
% election_timeout ~ 2 * broadcast_timeout
-type options() ::#{
    self              => raft_server_rpc:endpoint(),
    others            => ordsets:ordset(raft_server_rpc:endpoint()),
    election_timeout  => {From::timeout_ms(), To::timeout_ms()},
    broadcast_timeout => timeout_ms()
}.

-type raft_term    () :: non_neg_integer().
-type index        () :: non_neg_integer().
-type command      () :: term().
-type log_entry    () :: {raft_term(), command()}.
-type backend_state() :: term().


%% behaviour
-callback init(_Args) ->
    backend_state().

%% это вместо apply
-callback commit(index(), backend_state()) ->
    backend_state().

-callback get_last_commit_index(backend_state()) ->
    index().

%% log
-callback get_last_log_index(backend_state()) ->
    index().
-callback get_log_entry(index(), backend_state()) ->
    log_entry().
% (From, To)
-callback get_log_entries(index(), index(), backend_state()) ->
    [log_entry()].
-callback append_log_entries(index(), [log_entry()], backend_state()) ->
    backend_state().



%% версия без регистрации не имеет смысла (или я не прав?)
-spec start_link(mg_utils:gen_reg_name(), module(), _Args, options()) ->
    mg_utils:gen_start_ret().
start_link(RegName, Module, Args, Options) ->
    gen_server:start_link(RegName, ?MODULE, {Module, Args, Options}, []).

-spec send_command(raft_server_rpc:endpoint(), command(), mg_utils:deadline()) ->
    ok.
send_command(To, Command, Deadline) ->
    case mg_utils:is_deadline_reached(Deadline) of
        false ->
            case gen_server:call(To, {send_command, Command}, mg_utils:deadline_to_timeout(Deadline)) of
                ok ->
                    ok;
                {error, {leader_is, LeaderRef}} ->
                    ok = timer:sleep(100), % TODO
                    send_command(LeaderRef, Command, Deadline);
                {error, no_leader} ->
                    % возможно он уже избрался
                    ok = timer:sleep(100), % TODO
                    send_command(To, Command, Deadline);
                {error, timeout} ->
                    % по аналогии с gen_server
                    exit({timeout, {?MODULE, send_command, [To, Command, Deadline]}})
            end;
        true ->
            % по аналогии с gen_server
            exit({timeout, {?MODULE, send_command, [To, Command, Deadline]}})
    end.

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

    rpc_mod       => module(),
    rpc_state     => raft_server_rpc:state(),

    % модуль и состояние бэкенда (со стейт машиной и логом)
    backend_mod   => module(),
    backend_state => backend_state()
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
    {reply, ok, NewState, get_timer_timeout(NewState)}.

-spec handle_cast(_, state()) ->
    mg_utils:gen_server_handle_cast_ret(state()).
handle_cast(_, State) ->
    {noreply, State, get_timer_timeout(State)}.

-spec handle_info(_Info, state()) ->
    mg_utils:gen_server_handle_info_ret(state()).
handle_info(timeout, State) ->
    NewState = handle_timeout(State),
    {noreply, NewState, get_timer_timeout(NewState)};
handle_info(Info, State0) ->
    State2 =
        case recv(Info, State0) of
            {Message, State1} ->
                handle_rpc(Message, State1);
            invalid_data ->
                exit(todo)
        end,
    {noreply, State2, get_timer_timeout(State2)}.

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
-spec new_state({module(), _Args, options()}) ->
    state().
new_state({BackendMod, BackendArgs, Options}) ->
    % FIXME
    RPCMod  = raft_server_rpc_erl,
    RPCArgs = undefined,
    State =
        #{
            options       => Options,
            timer         => undefined,
            role          => {follower, undefined},
            current_term  => 0,
            rpc_mod       => RPCMod,
            rpc_state     => raft_server_rpc:init(RPCMod, RPCArgs),
            backend_mod   => BackendMod,
            backend_state => BackendMod:init(BackendArgs)
        },
    become_follower(State#{current_term := get_term_from_log(backend_get_last_log_index(State), State)}).

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
    NewState = backend_append_log_entries(backend_get_last_log_index(State), [{CurrentTerm, Command}], State),
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
handle_rpc(Msg = {_, _, _, _, Term}, State = #{current_term := CurrentTerm}) when Term > CurrentTerm ->
    handle_rpc(Msg, become_follower(State#{current_term := Term}));
handle_rpc({_, _, _, _, Term}, State = #{current_term := CurrentTerm}) when Term < CurrentTerm ->
    State;
handle_rpc({request, Type, Body, From, _}, State) ->
    {Result, NewState} = handle_rpc_request(Type, Body, From, State),
    send_response(From, Type, Result, NewState);
handle_rpc({{response, Succeed}, Type, From, _}, State) ->
    handle_rpc_response(Type, From, Succeed, State).

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
        case {Result, CommitIndex > backend_get_last_log_index(State1)} of
            {true , true} -> backend_commit(CommitIndex, State1);
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
            backend_get_last_log_index(State),
            backend_get_commit_index  (State),
            update_leader_follower(From, NewFollowerState, State)
        )
    ).

%%

-spec try_append_to_log(undefined | log_entry(), [log_entry()], state()) ->
    {boolean(), state()}.
try_append_to_log({PrevTerm, PrevIndex}, Entries, State) ->
    case (backend_get_last_log_index(State) >= PrevIndex) andalso get_term_from_log(PrevIndex, State) of
        PrevTerm ->
            {true, backend_append_log_entries(PrevIndex, Entries, State)};
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
                true  -> backend_commit(IndexN, State);
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
    {backend_get_last_log_index(State) + 1, 0, 0, 0}.

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
    send_request_votes(NewState).

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
    state().
send_request_votes(State = #{options := #{others := Others}}) ->
    LastIndex = backend_get_last_log_index(State),
    lists:foldl(
        fun(To, StateAcc) ->
            send_request(To, request_vote, {LastIndex, get_term_from_log(LastIndex, StateAcc)}, StateAcc)
        end,
        State,
        Others
    ).

%% Послать в том случае если:
%%  - пришло время heartbeat
%%  - есть новые записи, но нет текущих запросов (NextIndex < LastLogIndex) & Match =:= NextIndex - 1
-spec try_send_append_entries(state()) ->
    state().
try_send_append_entries(State = #{role := {leader, FollowersState}}) ->
    {NewFollowersState, NewState} = maps_mapfold(fun try_send_append_entries/3, State, FollowersState),
    schedule_next_heartbeat_timer(update_leader(NewFollowersState, NewState)).

-spec try_send_append_entries(raft_server_rpc:endpoint(), follower_state(), state()) ->
    {follower_state(), state()}.
try_send_append_entries(To, FollowerState, State = #{options := #{broadcast_timeout := Timeout}}) ->
    LastLogIndex = backend_get_last_log_index(State),
    Now = now_ms(),
    case FollowerState of
        {NextIndex, MatchIndex, Heartbeat, _}
            when Heartbeat =< Now
            ->
                NewState = send_append_entries(To, NextIndex, MatchIndex, State),
                {{NextIndex, MatchIndex, Now + Timeout, Now + Timeout}, NewState};
        {NextIndex, MatchIndex, _, RPCTimeout}
            when    NextIndex   =<  LastLogIndex
            andalso RPCTimeout  =<  Now
            ->
                NewNextIndex =
                    case MatchIndex =:= (NextIndex - 1) of
                        true  -> LastLogIndex + 1;
                        false -> NextIndex
                    end,
                NewState = send_append_entries(To, NewNextIndex, MatchIndex, State),
                {{NewNextIndex, MatchIndex, Now + Timeout, Now + Timeout}, NewState};
        {_, _, _, _}   ->
            {FollowerState, State}
    end.

-spec send_append_entries(raft_server_rpc:endpoint(), index(), index(), state()) ->
    state().
send_append_entries(To, NextIndex, MatchIndex, State) ->
    Prev = {get_term_from_log(MatchIndex, State), MatchIndex},
    Body = {Prev, backend_get_log_entries(MatchIndex, NextIndex, State), backend_get_commit_index(State)},
    send_request(To, append_entries, Body, State).

-spec send_request(raft_server_rpc:endpoint(), raft_server_rpc:message_type(), raft_server_rpc:message_body(), state()) ->
    state().
send_request(To, Type, Body, State = #{options := #{self := Self}, current_term := Term}) ->
    send(To, {request, Type, Body, Self, Term}, State).

-spec send_response(raft_server_rpc:endpoint(), boolean(), raft_server_rpc:message_body(), state()) ->
    state().
send_response(To, Type, Result, State = #{options := #{self := Self}, current_term := CurrentTerm}) ->
    send(To, {{response, Result}, Type, Self, CurrentTerm}, State).

-spec send(raft_server_rpc:endpoint(), raft_server_rpc:message(), state()) ->
    state().
send(To, Message, State = #{rpc_mod := RPCMod, rpc_state := RPCState}) ->
    State#{rpc_state := raft_server_rpc:send(RPCMod, To, Message, RPCState)}.

-spec recv(_Info, state()) ->
    {raft_server_rpc:message(), state()} | invalid_data.
recv(Info, State = #{rpc_mod := RPCMod, rpc_state := RPCState}) ->
    case raft_server_rpc:recv(RPCMod, Info, RPCState) of
        {Message, NewRPCState} ->
            {Message, State#{rpc_state := NewRPCState}};
        invalid_data ->
            invalid_data
    end.

-spec get_term_from_log(index(), state()) ->
    term().
get_term_from_log(Index, State) ->
    element(1, backend_get_log_entry(Index, State)).

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
%% backend
%%
-spec backend_commit(index(), state()) ->
    state().
backend_commit(Index, State = #{backend_mod := BackendMod, backend_state := BackendState}) ->
    State#{backend_state := BackendMod:commit(Index, BackendState)}.

-spec backend_get_commit_index(state()) ->
    index().
backend_get_commit_index(#{backend_mod := BackendMod, backend_state := BackendState}) ->
    BackendMod:get_last_commit_index(BackendState).

-spec backend_get_last_log_index(state()) ->
    index().
backend_get_last_log_index(#{backend_mod := BackendMod, backend_state := BackendState}) ->
    BackendMod:get_last_log_index(BackendState).

-spec backend_get_log_entry(index(), state()) ->
    [log_entry()].
backend_get_log_entry(Index, #{backend_mod := BackendMod, backend_state := BackendState}) ->
    BackendMod:get_log_entry(Index, BackendState).

-spec backend_get_log_entries(index(), index(), state()) ->
    [log_entry()].
backend_get_log_entries(From, To, #{backend_mod := BackendMod, backend_state := BackendState}) ->
    BackendMod:get_log_entries(From, To, BackendState).

-spec backend_append_log_entries(index(), [log_entry()], state()) ->
    state().
backend_append_log_entries(PrevIndex, Entries, State = #{backend_mod := BackendMod, backend_state := BackendState}) ->
    State#{backend_state := BackendMod:append_log_entries(PrevIndex, Entries, BackendState)}.


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

-spec maps_mapfold(fun((K, V, Acc) -> {V1, Acc}), Acc, #{K => V}) ->
    {#{K => V1}, Acc}.
maps_mapfold(F, Acc, Map) ->
    {ListMap, NewAcc} =
        lists:mapfoldl(
            fun({K, V}, Acc_) ->
                {NewV, NewAcc_} = F(K, V, Acc_),
                {{K, NewV}, NewAcc_}
            end,
            Acc,
            maps:to_list(Map)
        ),
    {maps:from_list(ListMap), NewAcc}.
