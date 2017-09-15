%%%
%%% Как работает фрагментация?
%%% Адоптация?
%%% SAC?
%%%
%%%
-module(raft_rpc_sctp).

%% API
-export_type([endpoint/0]).
-export_type([peer    /0]).
-export([start_link/2]).

%% raft_rpc
-behaviour(raft_rpc).
-export([send/3, recv/2, get_nearest/2, self/1]).

%%
%% API
%%
-spec start_link(mg_utils:gen_reg_name(), raft_rpc_sctp_dispatcher:options()) ->
    mg_utils:gen_start_ret().
start_link(RegName, Options) ->
    raft_rpc_sctp_dispatcher:start_link(RegName, Options).

%%
%% raft_rpc
%%
-type endpoint() :: raft_rpc_sctp_dispatcher:endpoint().
-type peer    () :: raft_rpc_sctp_dispatcher:peer().

-spec send({peer(), mg_utils:gen_ref()}, endpoint(), raft_rpc:message()) ->
    ok.
send({_, DispatcherRef}, To, Message) ->
    ok = raft_rpc_sctp_dispatcher:send(DispatcherRef, To, Message).

-spec recv({peer(), mg_utils:gen_ref()}, term()) ->
    raft_rpc:message().
recv(_, Message) ->
    Message.


-spec get_nearest({peer(), mg_utils:gen_ref()}, [endpoint()]) ->
    endpoint().
get_nearest({SelfNodePeer, _}, Endpoints) ->
    case find_local(SelfNodePeer, Endpoints) of
        {ok, Local} ->
            Local;
        false ->
            mg_utils:lists_random(Endpoints)
    end.

-spec self({peer(), mg_utils:gen_ref()}) ->
    endpoint().
self({SelfNodePeer, _}) ->
    {SelfNodePeer, erlang:self()}.

%%

-spec find_local(peer(), [endpoint()]) ->
    {ok, endpoint()} | false.
find_local(_, []) ->
    false;
find_local(SelfNodePeer, [H|T]) ->
    case is_local(SelfNodePeer, H) of
        true  -> {ok, H};
        false -> find_local(SelfNodePeer, T)
    end.

-spec is_local(peer(), endpoint()) ->
    boolean().
is_local(SelfNodePeer, {Peer, _}) ->
    SelfNodePeer =:= Peer.

