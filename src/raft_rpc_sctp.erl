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
-export([send/4, recv/2, get_nearest/2, get_reply_endpoint/1]).

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

-spec send({peer(), mg_utils:gen_ref()}, endpoint(), endpoint(), raft_rpc:message()) ->
    ok.
send({SelfPeer, _}, From, {ToPeer, Ref}, Message) when SelfPeer =:= ToPeer ->
    % чтобы не слать локальный трафик через сеть
    % при этом происходит очень странный затуп на 3 секунды O_O
    _ = (catch mg_utils:gen_send(Ref, {raft_rpc, From, Message})),
    ok;
send({_, DispatcherRef}, From, To, Message) ->
    ok = raft_rpc_sctp_dispatcher:send(DispatcherRef, From, To, Message).

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

-spec get_reply_endpoint({peer(), mg_utils:gen_ref()}) ->
    endpoint().
get_reply_endpoint({SelfNodePeer, _}) ->
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

