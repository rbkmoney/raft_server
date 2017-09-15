-module(raft_worker).

% -type options() #{
%     rpc  => raft:rpc(),
%     raft => raft:options()
% }

% -type unload() :: {timeout, 5000}.

% -spec child_spec(atom(), options()) ->
%     supervisor:child_spec().
% child_spec(ChildID, Options) ->
%     #{
%         id       => ChildID,
%         start    => {?MODULE, start_link, [Options]},
%         restart  => permanent,
%         shutdown => 5000
%     }.

% -type options() :: #{
%     raft   => raft:options(),
%     rpc    => raft:rpc(),
%     unload => unload()
% }.

% -spec start_link(options(), _ID, _ReqCtx) ->
%     mg_utils:gen_start_ret().
% start_link(Options = #{raft := RaftOptions, rpc := RPC}, ID, ReqCtx) ->
%     raft:start_link(self_reg_name(ID), {?MODULE, {ID, Options, ReqCtx}}, RPC, RaftOptions).


% call(Options, ) ->
%     raft:send_sync_command().


% init() ->
%     ok.

