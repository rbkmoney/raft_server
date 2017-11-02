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
-export([start_link   /0]).
% -export([remove_route /3]).
% -export([add_delay    /4]).
-export([split        /2]).
-export([restore      /0]).

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

-spec start_link() ->
    raft_utils:gen_start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, undefined, []).

-spec split([endpoint()], [endpoint()]) ->
    ok.
split(PartA, PartB) ->
    gen_server:call(?MODULE, {split, PartA, PartB}).

-spec restore() ->
    ok.
restore() ->
    gen_server:call(?MODULE, restore).


%%
%% gen_server callbacks
%%
-type link_problem() ::
       broken
    % | {delay, N}
.
-type state() :: #{
    bad_links := atom() | ets:tid()
}.

-spec init(_) ->
    raft_utils:gen_server_init_ret(state()).
init(_) ->
    State = #{
        bad_links => ets:new(?MODULE, [set, protected, named_table, {read_concurrency, true}])
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
send(_, From, To, Message) ->
    case get_connectivity(From, To) of
        normal ->
            raft_rpc_erl:send(undefined, From, To, Message);
        broken ->
            ok
    end.

-spec recv(options(), term()) ->
    raft_rpc:message().
recv(_, Message) ->
    Message.

-spec get_nearest(options(), [endpoint()]) ->
    endpoint().
get_nearest(Self, Endpoints) ->
    case lists:member(Self, Endpoints) of
        true  -> Self;
        false -> raft_utils:lists_random(Endpoints)
    end.

-spec get_reply_endpoint(options()) ->
    endpoint().
get_reply_endpoint(_) ->
    erlang:self().

%%

-spec get_connectivity(endpoint(), endpoint()) ->
    link_problem() | normal.
get_connectivity(From, To) ->
    case ets:lookup(?MODULE, {From, To}) of
        [            ] -> normal;
        [{_, Problem}] -> Problem
    end.

-spec do_split([endpoint()], [endpoint()], state()) ->
    state().
do_split(PartA, PartB, State) ->
    BadLinks =
        [{{From, To}, broken} || From <- PartA, To <- PartB] ++
        [{{From, To}, broken} || From <- PartB, To <- PartA],
    add_bad_links(BadLinks, State).

-spec add_bad_links([{{endpoint(), endpoint()}, link_problem()}], state()) ->
    state().
add_bad_links(BadLinks, State) ->
    true = ets:insert(?MODULE, BadLinks),
    State.

-spec do_restore(state()) ->
    state().
do_restore(State) ->
    true = ets:delete_all_objects(?MODULE),
    State.
