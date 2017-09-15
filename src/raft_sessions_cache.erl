%%%
%%% Кеш для сессий общения с рафт группами.
%%% Нужен в первую очередь чтобы локально сохранять лидера, чтобы оптимальнее пускать трафик.
%%%
-module(raft_sessions_cache).

-export([put/2]).
-export([get/1]).

-type cluster_id() :: term().
-type session() :: term().

-spec put(cluster_id(), session()) ->
    ok.
put(ClusterID, Session) ->
    _ = erlang:put({?MODULE, ClusterID}, Session),
    ok.

-spec get(cluster_id()) ->
    session().
get(ClusterID) ->
    erlang:get({?MODULE, ClusterID}).
