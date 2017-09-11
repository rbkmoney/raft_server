-module(raft_server_fsm).

-export_type([fsm/0]).
-export([handle_command/3]).

-type fsm() :: mg_utils:mod_opts().

-callback handle_command(_, raft_server_storage:command(), raft_server_storage:state()) ->
    raft_server_storage:state().

% -callback handle_dirty_command(_, raft_server_storage:command(), raft_server_storage:state()) ->
%     ok.

%% исполняется на лидере
%% в определённой последовательности
%% может менять стейт
-spec handle_command(fsm(), raft_server_storage:command(), raft_server_storage:state()) ->
    raft_server_storage:state().
handle_command(FSM, Command, State) ->
    mg_utils:apply_mod_opts(FSM, handle_command, [Command, State]).

% %% может исполнять не на лидере
% %% очерёдность не определена
% %% не может менять стейт
% -spec handle_dirty_command(fsm(), raft_server_storage:command(), raft_server_storage:state()) ->
%     ok.
% handle_dirty_command(FSM, Command, State) ->
%     mg_utils:apply_mod_opts(FSM, handle_dirty_command, [Command, State]).
