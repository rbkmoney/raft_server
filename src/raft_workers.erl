-module(raft_workers).

% -type options() :: {worker(), name()}.
% -type worker() :: module().
% -type name() :: term().



% -spec child_spec(options(), atom()) ->
%     supervisor:child_spec().
% child_spec(Options, ChildID) ->
%     #{
%         id       => ChildID,
%         start    => {?MODULE, start_link, [Options]},
%         restart  => permanent,
%         type     => supervisor
%     }.

% -spec start_link(options()) ->
%     mg_utils:gen_start_ret().
% start_link({_, Name}) ->
%     mg_utils_supervisor_wrapper:start_link(
%         #{strategy => one_for_all},
%         [
%             workers_sup_child_spec(Name, worker_sup),
%             raft_supervisor:child_spec(workers_sup_ref(Name), raft_sup)
%         ]
%     ).

% -spec workers_sup_child_spec(name(), atom()) ->
%     supervisor:child_spec().
% workers_sup_child_spec(Name, ChildID) ->
%     #{
%         id       => ChildID,
%         start    => {?MODULE, start_workers_sup, [Name]},
%         restart  => permanent,
%         type     => supervisor
%     }.

% -spec start_workers_sup(name()) ->
%     mg_utils:gen_start_ret().
% start_workers_sup(Name) ->
%     mg_utils_supervisor_wrapper:start_link(
%         workers_sup_reg_name(Name),
%         #{strategy => one_for_one},
%         [
%             % добавляются динамически
%         ]
%     ).

% -spec do(options(), _ID, fun(() -> R), _ReqCtx, mg_utils:deadline()) ->
%     R | {error, _}.
% do(Options, ID, Fun, ReqCtx, Deadline) ->
%     try
%         Fun()
%     catch exit:Reason ->
%         handle_worker_exit(Options, ID, Fun, ReqCtx, Deadline, Reason)
%     end.

% -spec handle_worker_exit(options(), _ID, fun(() -> R), _ReqCtx, mg_utils:deadline(), _Reason) ->
%     R | {error, _}.
% handle_worker_exit(Options, ID, Fun, ReqCtx, Deadline, Reason) ->
%     case Reason of
%         % TODO тут должна быть ошибка, что нет группы этого воркера
%         no_raft ->
%             start_worker_and_retry(Options, ID, Fun, ReqCtx, Deadline);
%         {timeout, _} ->
%             {error, Reason};
%     end.

% -spec start_worker_and_retry(options(), _ID, fun(() -> R), _ReqCtx, mg_utils:deadline()) ->
%     R | {error, _}.
% start_worker_and_retry(Options, ID, Fun, ReqCtx, Deadline) ->
%     ok = start_worker(Options, ID),
%     do(Options, ID, Fun, ReqCtx, Deadline).

% -spec start_worker(options(), id()) ->
%     ok.
% start_worker({Worker, Name}, ID) ->
%     ok = raft_supervisor:start_child(raft_sup_ref(Name), Worker:child_spec(ID)).


% raft_sup_ref(Name) ->
%     ok.

% raft_sup_reg_name(Name) ->
%     ok.

% workers_sup_ref(Name) ->
%     ok.

% workers_sup_reg_name(Name) ->
%     ok.
