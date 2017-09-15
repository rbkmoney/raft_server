%%
%% Тут попытка получить семантику аналогичную gen_server.
%%
-module(raft_server).

% call() ->
%     ok.

% cast() ->
%     ok.

% async_call() ->
%     ok.




% -type request_id() :: term().

% -opaque client_session() :: #{
%     rpc     => raft_rpc:rpc(),
%     self    => raft_rpc:endpoint(),
%     cluster => [raft_rpc:endpoint()],
%     timeout => timeout_ms(),
%     leader  => raft_rpc:endpoint() | undefined
% }.

% -spec new_client_session(raft_rpc:rpc(), raft_rpc:endpoint(), [raft_rpc:endpoint()], timeout_ms()) ->
%     client_session().
% new_client_session(RPC, Self, Cluster, AttemptTimeout) ->
%     #{
%         rpc     => RPC,
%         self    => Self,
%         cluster => Cluster,
%         timeout => AttemptTimeout,
%         leader  => undefined
%     }.

% -spec sync_call(client_session(), request_id(), Request, Deadline) ->
%     {Response, Session}.
% sync_call(Session = #{self := From}, CallID, Call, Deadline) ->
%     ok = send_command(Session, {client_interaction, sync_call, CallID, Call, From}),
%     case recv_command(Session) of
%         timeout ->
%             sync_call(Session, CallID, Call, Deadline);
%         {client_interaction, sync_call_reply, CallID, Reply, From} ->
%             {set_session_leader(From), Reply}
%     end.

% async_call(Session, Call, Deadline) ->
%     ok = send_command(Session, {client_interaction, async_call, Call, From}),
%     case recv_command(Session) of
%         timeout ->
%             async_call(Session, Call, Deadline);
%         {client_interaction, async_call_reply, Reply, From} ->
%             {set_session_leader(From), Reply}
%     end.

% -spec sync_cast(client_session(), request_id(), _Cast, mg_utils:deadline()) ->
% sync_cast(Session, Cast, Deadline) ->
%     ok = send_command(Session, {client_interaction, sync_cast, Cast, From}),
%     case recv_command(Session) of
%         timeout ->
%             sync_cast(Session, Cast, Deadline);
%         {client_interaction, sync_cast, Reply, From} ->
%             {set_session_leader(From), Reply}
%     end.

% % {client_request, RequestID, Response, From}

% -spec send_command(Session, request_id(), Command) ->
%     ok.
% send_command(Session = #{self := From}, Command) ->
%     % выбрать локальный (если можно) или рандомный эндпоинт
%     % послать ему команду и ждать
%     % когда придёт ответ, из поля From можно взять текущего лидера
%     ok = raft_rpc:send(RPC, select_endpoint(Session), Command),

% -spec recv_command(client_session()) ->
%     command() | timeout.
% recv_command(Session#{timeout := Timeout}) ->
%     receive
%         {raft_rpc, Data} ->
%             raft_rpc:recv(RPC, Data),
%     after Timeout ->
%         % TODO check timeout
%         timeout
%     end.

% select_endpoint(#{cluster := Cluster}) ->
%     % TODO поискать локальный
%     lists_random(Cluster).

% -spec lists_random(list(T)) ->
%     T.
% lists_random(List) ->
%     lists:nth(rand:uniform(length(List)), List).
