-module(raft_server_fsm).

%% API
-export([start_link  /2]).
-export([send_command/3]).

%% raft_server
-behaviour(raft_server).
-export([init/1, commit/2, get_last_commit_index/1, get_last_log_index/1, get_log_entry/2, get_log_entries/3, append_log_entries/3]).

%%
%% API
%%
-spec start_link(mg_utils:gen_reg_name(), raft_server:options()) ->
    mg_utils:gen_start_ret().
start_link(RegName, Options) ->
    raft_server:start_link(RegName, ?MODULE, undefined, Options).

-spec send_command(mg_utils:gen_ref(), command(), mg_utils:deadline()) ->
    ok.
send_command(Ref, Command, Deadline) ->
    raft_server:send_command(Ref, Command, Deadline).

%%
%% raft_server
%%
-type log  () :: [raft_server:log_entry()].
-type fsm  () :: term().
-type command() :: raft_server:command().
-type index() :: raft_server:index().
-type state() :: #{
    % индекс последней команды, закоммиченной в лог лидера
    commit_index => index(),

    % текущий лог
    log          => log(),

    % индекс последней команды, применённой к стейт машине
    last_applied => index(),

    % состояние стейт машины
    fsm          => fsm()
}.

-spec init(_) ->
    state().
init(_) ->
    #{
        commit_index => 0,
        log          => [],
        last_applied => 0,
        fsm          => undefined
    }.

-spec commit(raft_server:index(), state()) ->
    state().
commit(Index, State = #{commit_index := CommitIndex}) when Index > CommitIndex -> % assert
    try_apply_commited(State#{commit_index := Index}).

-spec try_apply_commited(state()) ->
    state().
try_apply_commited(State = #{last_applied := LastApplied, commit_index := CommitIndex}) when LastApplied =:= CommitIndex ->
    State;
try_apply_commited(State = #{log := Log, last_applied := LastApplied, fsm := Fsm, commit_index := CommitIndex})
    when LastApplied < CommitIndex ->
        ApplyingIndex = LastApplied + 1,
        Command = get_command_from_log(ApplyingIndex, Log),
        {_, NewFsm} = apply_command_to_fsm(Command, Fsm),
        try_apply_commited(State#{last_applied := ApplyingIndex, fsm := NewFsm}).

-spec get_command_from_log(index(), log()) ->
    command().
get_command_from_log(Index, State) ->
    {_, Command} = get_log_entry(Index, State),
    Command.

-spec get_last_commit_index(state()) ->
    index().
get_last_commit_index(#{commit_index := CommitIndex}) ->
    CommitIndex.

%% log
-spec get_last_log_index(state()) ->
    index().
get_last_log_index(#{log := Log}) ->
    erlang:length(Log).

-spec get_log_entry(index(), state()) ->
    raft_server:log_entry().
get_log_entry(0, #{}) ->
    {0, undefined};
get_log_entry(Index, #{log := Log}) ->
    lists:nth(Index, Log).

-spec get_log_entries(index(), index(), state()) ->
    [raft_server:log_entry()].
get_log_entries(From, To, #{log := Log}) ->
    lists:sublist(Log, From + 1, To - From - 1).

-spec append_log_entries(index(), [raft_server:log_entry()], state()) ->
    state().
append_log_entries(PrevIndex, LogEntries, State = #{log := Log}) ->
    State#{log := lists:sublist(Log, 1, PrevIndex) ++ LogEntries}.

%%
%% fsm
%%
-spec apply_command_to_fsm(command(), fsm()) ->
    {_Reply, fsm()}.
apply_command_to_fsm({set, V}, undefined) ->
    {ok, V};
apply_command_to_fsm({reset, V}, _Fsm) ->
    {ok, V};
apply_command_to_fsm(get, Fsm) ->
    {Fsm, Fsm}.
