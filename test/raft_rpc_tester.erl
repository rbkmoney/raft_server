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

-module(raft_rpc_tester).

%% API
-export([start_link   /1]).
% -export([remove_route /3]).
% -export([add_delay    /4]).
-export([split        /3]).
-export([restore      /1]).

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%% raft_rpc
-behaviour(raft_rpc).
-export([send/4, recv/2, get_nearest/2, get_reply_endpoint/1]).

-type endpoint() ::
      raft_utils:gen_ref()
    | {node(), term()} % for testing
.

-type options() :: {raft_utils:gen_ref(), endpoint()}.

-spec start_link(raft_utils:gen_reg_name()) ->
    raft_utils:gen_start_ret().
start_link(RegName) ->
    gen_server:start_link(RegName, ?MODULE, undefined, []).

-spec split(raft_utils:gen_ref(), [endpoint()], [endpoint()]) ->
    ok.
split(Ref, PartA, PartB) ->
    gen_server:call(Ref, {split, PartA, PartB}).

-spec restore(raft_utils:gen_ref()) ->
    ok.
restore(Ref) ->
    gen_server:call(Ref, restore).


%%
%% gen_server callbacks
%%
-type link_problem() ::
       broken
    % | {delay, N}
.
-type state() :: #{
    bad_links := #{{From::endpoint(), To::endpoint()} => link_problem()}
}.

-spec init(_) ->
    raft_utils:gen_server_init_ret(state()).
init(_) ->
    State = #{
        bad_links => #{}
    },
    {ok, State}.

-spec handle_call(_Call, raft_utils:gen_server_from(), state()) ->
    raft_utils:gen_server_handle_call_ret(state()).
handle_call({split, PartA, PartB}, _From, State) ->
    {reply, ok, do_split(PartA, PartB, State)};
handle_call(restore, _From, State) ->
    {reply, ok, do_restore(State)};
handle_call(_, _From, State) ->
    {noreply, State}.

-spec handle_cast(_, state()) ->
    raft_utils:gen_server_handle_cast_ret(state()).
handle_cast({send, From, To, Message}, State) ->
    ok = do_send(From, To, Message, State),
    {noreply, State};
handle_cast(_, State) ->
    {noreply, State}.

-spec handle_info(_Info, state()) ->
    raft_utils:gen_server_handle_info_ret(state()).
handle_info(_Info, State) ->
    {noreply, State}.

-spec code_change(_, state(), _) ->
    raft_utils:gen_server_code_change_ret(state()).
code_change(_, State, _) ->
    {ok, State}.

-spec terminate(_Reason, state()) ->
    ok.
terminate(_, _) ->
    ok.

%%
%% raft_rpc
%%
-spec send(options(), endpoint(), endpoint(), raft_rpc:message()) ->
    ok.
send({TesterRef, _}, From, To, Message) ->
    gen_server:cast(TesterRef, {send, From, To, Message}).

-spec recv(options(), term()) ->
    raft_rpc:message().
recv(_, Message) ->
    Message.

-spec get_nearest(options(), [endpoint()]) ->
    endpoint().
get_nearest({_, Self}, Endpoints) ->
    case lists:member(Self, Endpoints) of
        true  -> Self;
        false -> raft_utils:lists_random(Endpoints)
    end.

-spec get_reply_endpoint(options()) ->
    endpoint().
get_reply_endpoint(_) ->
    erlang:self().

%%

-spec do_split([endpoint()], [endpoint()], state()) ->
    state().
do_split(PartA, PartB, State) ->
    lists:foldl(
        fun add_bad_link/2,
        State,
        [{From, To} || From <- PartA, To <- PartB] ++
        [{From, To} || From <- PartB, To <- PartA]
    ).

-spec add_bad_link({endpoint(), endpoint()}, state()) ->
    state().
add_bad_link({From, To}, State = #{bad_links := BadLinks}) ->
    State#{bad_links := maps:put({From, To}, broken, BadLinks)}.

-spec do_restore(state()) ->
    state().
do_restore(State) ->
    State#{bad_links := #{}}.


-spec do_send(endpoint(), endpoint(), raft_rpc:message(), state()) ->
    ok.
do_send(From, To, Message, State) ->
    case get_connectivity(From, To, State) of
        normal ->
            raft_rpc_erl:send(undefined, From, To, Message);
        broken ->
            ok
    end.

-spec get_connectivity(endpoint(), endpoint(), state()) ->
    link_problem() | normal.
get_connectivity(From, To, #{bad_links := BadLinks}) ->
    maps:get({From, To}, BadLinks, normal).

