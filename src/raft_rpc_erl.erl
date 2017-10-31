%%%
%%% Copyright 2017 RBKmoney
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

-module(raft_rpc_erl).

%% raft_rpc
-behaviour(raft_rpc).
-export([send/4, get_nearest/2, get_reply_endpoint/1]).

-type endpoint() ::
      raft_utils:gen_ref()
    | {node(), term()} % for testing
.

-spec send(_, endpoint(), endpoint(), raft_rpc:message()) ->
    ok.
send(Options, From, To, Message) ->
    FullMessage = {raft_rpc, From, Message},
    _ = case To of
            {global, GlobalName} ->
                catch global:send(GlobalName, FullMessage);
            {via, Mod, Name} ->
                catch Mod:send(Name, FullMessage);
            {Name, Node} when is_atom(Node) andalso not is_atom(Name) ->
                erlang:spawn(Node, ?MODULE, send, [Options, From, Name, Message]);
            LocalNameOrPid ->
                catch erlang:send(LocalNameOrPid, FullMessage)
        end,
    ok.

-spec get_nearest(_, [endpoint()]) ->
    endpoint().
get_nearest(_, Endpoints) ->
    case find_local(Endpoints) of
        {ok, Local} ->
            Local;
        false ->
            raft_utils:lists_random(Endpoints)
    end.

-spec get_reply_endpoint(_) ->
    endpoint().
get_reply_endpoint(_) ->
    erlang:self().

%%

-spec find_local([endpoint()]) ->
    {ok, endpoint()} | false.
find_local([]) ->
    false;
find_local([H|T]) ->
    case is_local(H) of
        true  -> {ok, H};
        false -> find_local(T)
    end.

-spec is_local(endpoint()) ->
    boolean().
is_local({Name, Node}) when is_atom(Node) andalso not is_atom(Name) ->
    Node =:= erlang:node();
is_local(Ref) ->
    try
        % а нет ли более простого варианта?
        erlang:node(raft_utils:gen_where_ref(Ref)) =:= erlang:node()
    catch error:badarg ->
        false
    end.
