%%%
%%% Основная идея в том, чтобы сделать максимально лёгкую в стиле OTP реализацию Raft.
%%% В процессе написания была попытка усидеть на 2х стульях сразу:
%%%  - с одной стороны по максимуму придерживаться терминологии орининального документа (https://raft.github.io/raft.pdf);
%%%  - с другой делать в стили OTP.
%%%
%%%
%%% TODO:
%%%  - сделать обработку AppendEntries
%%%  - запустить!
%%%  - сделать понятие "дельта"
%%%  - сделать поведение и нормальную работу с persistance state
%%%  -
%%%

% лидер:
%  - добавить в лог
%  - реплицировать на мажорити
%  - применить к автомату

% слейвы:
%  - добавить в лог
%  - применить к автомату

-module(raft_server).

%% API
-export([start_link   /2]).
-export([write_command/3]).
-export([read_command /3]).

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
    self              => mg_utils:gen_ref(),
    others            => ordsets:ordset(mg_utils:gen_ref()),
    election_timeout  => {From::timeout_ms(), To::timeout_ms()},
    broadcast_timeout => timeout_ms()
}.

%% версия без регистрации не имеет смысла (или я не прав?)
-spec start_link(mg_utils:gen_reg_name(), options()) ->
    mg_utils:gen_start_ret().
start_link(RegName, Options) ->
    gen_server:start_link(RegName, ?MODULE, Options, []).

-spec write_command(mg_utils:gen_ref(), fsm_command(), mg_utils:deadline()) ->
    _Reply.
write_command(Ref, Command, Deadline) ->
    case mg_utils:is_deadline_reached(Deadline) of
        false ->
            case gen_server:call(Ref, {write_command, Command, Deadline}, mg_utils:deadline_to_timeout(Deadline)) of
                {ok, Reply} ->
                    Reply;
                {error, {leader_is, LeaderRef}} ->
                    timer:sleep(100), % TODO
                    write_command(LeaderRef, Command, Deadline);
                {error, no_leader} ->
                    % возможно он уже избрался
                    timer:sleep(100), % TODO
                    write_command(Ref, Command, Deadline);
                {error, timeout} ->
                    % по аналогии с gen_server
                    exit({timeout, {?MODULE, write_command, [Ref, Command, Deadline]}})
            end;
        true ->
            % по аналогии с gen_server
            exit({timeout, {?MODULE, write_command, [Ref, Command, Deadline]}})
    end.

-spec read_command(mg_utils:gen_ref(), fsm_command(), mg_utils:deadline()) ->
    _Reply.
read_command(Ref, Command, Deadline) ->
    case mg_utils:is_deadline_reached(Deadline) of
        false ->
            case gen_server:call(Ref, {read_command, Command, Deadline}, mg_utils:deadline_to_timeout(Deadline)) of
                {ok, Reply} ->
                    Reply;
                {error, timeout} ->
                    % по аналогии с gen_server
                    exit({timeout, {?MODULE, read_command, [Ref, Command, Deadline]}})
            end;
        true ->
            % по аналогии с gen_server
            exit({timeout, {?MODULE, read_command, [Ref, Command, Deadline]}})
    end.

%%
%% gen_server callbacks
%%
-type raft_term  () :: non_neg_integer().
-type index      () :: non_neg_integer().
-type fsm_command() :: term().

-type follower_state () :: {Next::index(), Match::index(), Heartbeat::timestamp_ms()}.
-type followers_state() :: #{mg_utils:gen_ref() => follower_state()}.
-type role() ::
      {leader   , followers_state()}
    | {follower , MyLeader::(mg_utils:gen_ref() | undefined)}
    | {candidate, VotedFrom::ordsets:ordset(mg_utils:gen_ref())}
.

-type state() :: #{
    options      => options(),
    timer        => timestamp_ms(),

    % текущая роль и специфичные для неё данные
    role         => role(),

    % текущий терм (вообще, имхо, слово "эпоха" тут более подходящее)
    current_term => raft_term(),

    % индекс последней команды, закоммиченной в лог лидера
    commit_index => index(),

    % текущий лог
    log          => log(),

    % индекс последней команды, применённой к стейт машине
    last_applied => index(),

    % состояние стейт машины
    fsm          => fsm()
}.

-spec init(options()) ->
    mg_utils:gen_server_init_ret(state()).
init(Options) ->
    NewState = new_state(Options),
    {ok, NewState, get_timer_timeout(NewState)}.

%%
%% Calls
%%
-spec handle_call(_Call, mg_utils:gen_server_from(), state()) ->
    mg_utils:gen_server_handle_call_ret(state()).
handle_call({CommandType, Command, Deadline}, _From, State) ->
    {Reply, NewState} =
        case mg_utils:is_deadline_reached(Deadline) of
            false ->
                case CommandType of
                    write_command -> handle_write_command(Command, State);
                    read_command  -> {{reply, handle_read_command(Command, State)}, State}
                end;
            true ->
                {{reply, {error, timeout}}, State}
        end,
    case Reply of
        {reply, ReplyBody} ->
            {reply, ReplyBody, NewState, get_timer_timeout(NewState)};
        noreply ->
            {noreply, NewState, get_timer_timeout(NewState)}
    end.

-spec handle_cast(rpc_message(), state()) ->
    mg_utils:gen_server_handle_cast_ret(state()).
handle_cast(Msg, State) ->
    NewState = handle_rpc(Msg, State),
    {noreply, NewState, get_timer_timeout(NewState)}.

-spec handle_info(_Info, state()) ->
    mg_utils:gen_server_handle_info_ret(state()).
handle_info(timeout, State) ->
    NewState = handle_timeout(State),
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
-spec new_state(options()) ->
    state().
new_state(Options) ->
    become_follower(
        #{
            options      => Options,
            timer        => undefined,
            role         => {follower, undefined},
            current_term => 0,
            log          => log_new(),
            commit_index => 0,
            last_applied => 0,
            fsm          => fsm_new()
        }
    ).

-spec handle_timeout(state()) ->
    state().
handle_timeout(State = #{role := {leader, _}}) ->
    try_send_append_entries(State);
handle_timeout(State = #{role := _}) ->
    become_candidate(State).

-spec handle_write_command(fsm_command(), state()) ->
    {{reply, _} | noreply, state()}.
handle_write_command(Command, State = #{role := {leader, _}}) ->
    % добавить результат в лог (если запись уже есть, то ответить (как это узнать? чем?))
    % запустить процесс репликации (а если он уже идёт?)
    % в данный момент и при получении каждого ответа проверять, не прошла ли репликация на мажорити
    % когда пройдёт — применить к автомату (в процессе подвинуть указатель последнего апплая) и ответить
    NewState = append_to_log(Command, State),
    % TODO
    {{reply, {ok, ok}}, try_send_append_entries(NewState)};
handle_write_command(_, State = #{role := {candidate, _}}) ->
    {{reply, {error, no_leader}}, State};
handle_write_command(_, State = #{role := {follower, undefined}}) ->
    {{reply, {error, no_leader}}, State};
handle_write_command(_, State = #{role := {follower, Leader}}) ->
    {{reply, {error, {leader_is, Leader}}}, State}.

-spec handle_read_command(fsm_command(), state()) ->
    _Reply.
handle_read_command(Command, #{fsm := Fsm}) ->
    {ok, fsm_read(Command, Fsm)}.

-spec handle_rpc(rpc_message(), state()) ->
    state().
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

-spec handle_rpc_request(rpc_message_type(), rpc_message_body(), mg_utils:gen_ref(), state()) ->
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
handle_rpc_request(append_entries, {Prev, Entries, CommitIndex}, _, State0 = #{role := {follower, _}, log := Log}) ->
    {Result, State1} = try_append_to_log(Prev, Entries, State0),
    State2 =
        case Result of
            % 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            % когда возможно, что leaderCommit будет меньше чем commitIndex
            true  -> try_apply_commited(State1#{commit_index := erlang:min(log_get_last_index(Log), CommitIndex)});
            false -> State1
        end,
    {Result, schedule_election_timer(State2)}.

-spec handle_rpc_response(rpc_message_type(), mg_utils:gen_ref(), boolean(), state()) ->
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
        try_apply_commited(
            try_updated_commit_index(
                update_leader_follower(From, NewFollowerState, State)))).

%%

-spec try_append_to_log(undefined | log_entry(), [log_entry()], state()) ->
    {boolean(), state()}.
try_append_to_log({PrevTerm, PrevIndex}, Entries, State = #{log := Log}) ->
    case (log_get_last_index(Log) >= PrevIndex) andalso log_get_term(PrevIndex, Log) of
        PrevTerm ->
            {true, State#{log := log_append(PrevIndex, Entries, Log)}};
        _ ->
            {false, State}
    end.

-spec append_to_log(fsm_command(), state()) ->
    state().
append_to_log(Command, State=#{log := Log, current_term := CurrentTerm}) ->
    State#{log := log_append(log_get_last_index(Log), [{CurrentTerm, Command}], Log)}.

-spec add_vote(mg_utils:gen_ref(), state()) ->
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

-spec try_apply_commited(state()) ->
    state().
try_apply_commited(State = #{last_applied := LastApplied, commit_index := CommitIndex}) when LastApplied =:= CommitIndex ->
    State;
try_apply_commited(State = #{log := Log, last_applied := LastApplied, fsm := Fsm}) ->
    ApplyingIndex = LastApplied + 1,
    Command = log_get_command(ApplyingIndex, Log),
    NewFsm = fsm_apply(Command, Fsm),
    try_apply_commited(State#{last_applied := ApplyingIndex, fsm := NewFsm}).

-spec try_updated_commit_index(state()) ->
    state().
try_updated_commit_index(State=#{log := Log}) ->
    try_updated_commit_index(log_get_last_index(Log), State).

-spec try_updated_commit_index(index(), state()) ->
    state().
try_updated_commit_index(IndexN, State = #{current_term := CurrentTerm, commit_index := CommitIndex, log := Log}) ->
    % If there exists an N such that N > commitIndex, a majority
    % of matchIndex[i] ≥ N, and log[N].term == currentTerm:
    % set commitIndex = N (§5.3, §5.4)
    LogNTerm = log_get_term(IndexN, Log),
    case IndexN > CommitIndex andalso LogNTerm =:= CurrentTerm of
        true ->
            case is_replicated(IndexN, State) of
                true  -> State#{commit_index := IndexN};
                false -> try_updated_commit_index(IndexN - 1, State)
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
    maps:from_list([{Ref, new_follower_state(State)} || Ref <- Others]).

-spec new_follower_state(state()) ->
    follower_state().
new_follower_state(#{log := Log}) ->
    {log_get_last_index(Log) + 1, 0, 0, 0}.

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

-spec update_leader_follower(mg_utils:gen_ref(), follower_state(), state()) ->
    state().
update_leader_follower(Follower, NewFollowerState, State = #{role := {leader, FollowersState}}) ->
    update_leader(FollowersState#{Follower := NewFollowerState}, State).

-spec update_follower(mg_utils:gen_ref(), state()) ->
    state().
update_follower(Leader, State = #{role := {follower, _}}) ->
    set_role({follower, Leader}, State).

%%

-spec send_request_votes(state()) ->
    ok.
send_request_votes(State = #{options := #{others := Others}, log := Log}) ->
    LastIndex = log_get_last_index(Log),
    ok = lists:foreach(
            fun(Ref) ->
                ok = send_request(Ref, request_vote, {LastIndex, log_get_term(LastIndex, Log)}, State)
            end,
            Others
        ).

%% Послать в том случае если:
%%  - пришло время heartbeat
%%  - есть новые записи, но нет текущих запросов (NextIndex < LastLogIndex) & Match =:= NextIndex - 1
-spec try_send_append_entries(state()) ->
    state().
try_send_append_entries(State = #{role := {leader, FollowersState}}) ->
    schedule_next_heartbeat_timer(
        update_leader(
            maps:map(
                fun(Follower, FollowerState) ->
                    try_send_append_entries(Follower, FollowerState, State)
                end,
                FollowersState
            ),
            State
        )
    ).

-spec try_send_append_entries(mg_utils:gen_ref(), follower_state(), state()) ->
    follower_state().
try_send_append_entries(To, FollowerState, State = #{options := #{broadcast_timeout := Timeout}, log := Log}) ->
    LastLogIndex = log_get_last_index(Log),
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

-spec send_append_entries(mg_utils:gen_ref(), index(), index(), state()) ->
    ok.
send_append_entries(To, NextIndex, MatchIndex, State = #{commit_index := CommitIndex, log := Log}) ->
    Prev = {log_get_term(MatchIndex, Log), MatchIndex},
    Body = {Prev, log_get_range(MatchIndex, NextIndex, Log), CommitIndex},
    ok = send_request(To, append_entries, Body, State).

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
%% RPC
%%
-type rpc_message() :: {
    request | {response, _Success::boolean()},
    _Type       ::rpc_message_type(),
    _CurrentTerm::raft_term(),
    _From       ::mg_utils:gen_ref()
}.
-type rpc_message_type() ::
      request_vote
    | append_entries
.
-type rpc_message_body() :: term().

-spec send_request(mg_utils:gen_ref(), rpc_message_type(), rpc_message_body(), state()) ->
    ok.
send_request(To, Type, Body, #{options := #{self := Self}, current_term := Term}) ->
    ok = send(To, {request, Type, Body, Self, Term}).

-spec send_response(mg_utils:gen_ref(), boolean(), rpc_message_body(), state()) ->
    ok.
send_response(To, Type, Result, #{options := #{self := Self}, current_term := CurrentTerm}) ->
    ok = send(To, {{response, Result}, Type, Self, CurrentTerm}).

-spec send(mg_utils:gen_ref(), rpc_message()) ->
    ok.
send(Ref, Message) ->
    ok = gen_server:cast(Ref, Message).


%%
%% log
%%
-type log_entry() :: {raft_term(), fsm_command()}.
-type log() :: [log_entry()].

-spec log_new() ->
    log().
log_new() ->
    [].

-spec log_get_last_index(log()) ->
    index().
log_get_last_index(Log) ->
    erlang:length(Log).

-spec log_get(index(), log()) ->
    [log_entry()].
log_get(Index, Log) ->
    lists:nth(Index, Log).

-spec log_get_term(index(), log()) ->
    term().
log_get_term(0, _) ->
    0;
log_get_term(Index, Log) ->
    {Term, _} = log_get(Index, Log),
    Term.

-spec log_get_command(index(), log()) ->
    fsm_command().
log_get_command(0, Log) ->
    erlang:error(badarg, [0, Log]);
log_get_command(Index, Log) ->
    {_, Command} = log_get(Index, Log),
    Command.

%% (From, To)
-spec log_get_range(index(), index(), log()) ->
    [log_entry()].
log_get_range(From, To, Log) ->
    lists:sublist(Log, From + 1, To - From - 1).

-spec log_append(index(), [log_entry()], log()) ->
    log().
log_append(PrevN, LogEntries, Log) ->
    lists:sublist(Log, 1, PrevN) ++ LogEntries.

%%
%% fsm
%%
-type fsm() :: [fsm_command()].

-spec fsm_new() ->
    fsm().
fsm_new() ->
    undefined.

-spec fsm_apply(fsm_command(), fsm()) ->
    fsm().
fsm_apply({set, V}, undefined) ->
    V;
fsm_apply({reset, V}, _Fsm) ->
    V.

-spec fsm_read(fsm_command(), fsm()) ->
    fsm().
fsm_read(get, Fsm) ->
    Fsm.

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
