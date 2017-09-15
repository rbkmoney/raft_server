-module(raft_supervisor).

% %% API

% %%
% %% API
% %%
% -spec start_link(mg_utils:gen_reg_name(), mg_utils:gen_ref()) ->
%     mg_utils:gen_start_ret().
% start_link(RegName, RaftOptions, SupRef) ->
%     raft:start_link(RegName, {?MODULE, SupRef}, RaftOptions).

% -spec start_child(raft_rpc:endpoint(), supervisor:child_spec()) ->
%     ok | already_started.
% start_child(RaftOptions, ChildSpec) ->
%     raft_client:sync_call(
%         create_or_get_session(...),
%         {start_child, ChildSpec},
%         mg_utils:timeout_to_deadline(30000)
%     ).

% -spec terminate_child(raft_rpc:endpoint(), ID) ->
%     ok.
% terminate_child(RaftOptions, ID) ->
%     raft:sync_call(
%         create_or_get_session(...),
%         {terminate_child, ID},
%         mg_utils:timeout_to_deadline(30000)
%     ).

% -spec create_or_get_session(RPC, Cluster, Self, AttemptTimeout) ->
%     raft_client:session().
% create_or_get_session(RPC, Cluster, Self, AttemptTimeout) ->
%     ClusterID = {RPC, Cluster},
%     case raft_sessions_cache:get(ClusterID) of
%         undefined ->
%             Session = raft_client:new_client_session(ClusterID),
%             ok = raft_sessions_cache:put(ClusterID, Session),
%             Session;
%         Session ->
%             Session
%     end.

% %%

% -type state() :: #{
%     sup_ref := mg_utils:gen_ref()
% }.

% init(SupRef) ->
%     #{sup_ref := SupRef}.

% handle_sync_command({start_child, ChildSpec}, State = #{sup_ref := SupRef}) ->
%     case supervisor:start_child(SupRef, ChildSpec) of
%         {ok, _} ->
%             {ok, State};
%         {error, already_present} ->
%             {already_started, State};
%         {error, {already_started, _}} ->
%             {already_started, State}
%     end;
% handle_sync_command({terminate_child, ID}, State = #{sup_ref := SupRef}) ->
%     case supervisor:terminate_child(SupRef, ID) of
%         ok ->
%             _ = supervisor:delete_child(SupRef, ID),
%             {ok, State};
%         {error, not_found} ->
%             {ok, State}
%     end.

% %% TODO другие функции для рафт бэкенда
