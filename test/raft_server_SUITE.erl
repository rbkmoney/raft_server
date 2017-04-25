-module(raft_server_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all/0]).

%% tests
-export([simple_test/1]).

%% raft_server
% -behaviour(raft_server).
% -export([process_machine/5]).

%%
%% tests descriptions
%%
-type test_name () :: atom().
-type config    () :: [{atom(), _}].

-spec all() ->
    [test_name()].
all() ->
    [
       simple_test
    ].

%%
%% tests
%%
-type name() :: atom().
-type cluster() :: [name()].

-spec simple_test(config()) ->
    _.
simple_test(_) ->
    dbg:tracer(), dbg:p(all, c),
    dbg:tpl({raft_server, 'set_role', '_'}, x),
    % Cluster = [e, a, b],
    Cluster = [e, a, b, c, d],
    ClusterPids = start_cluster(Cluster),

    ok = test_write(e, {set, hello1}),
    ok = timer:sleep(100),
    hello1 = test_read(a, get),
    ok = timer:sleep(100),
    hello1 = test_read(e, get),

    ok = test_write(a, {reset, hello2}),
    ok = timer:sleep(100),
    hello2 = test_read(a, get),
    ok = timer:sleep(100),
    hello2 = test_read(e, get),

    mg_utils:stop_wait_all(ClusterPids, shutdown, 1000).

-spec start_cluster(cluster()) ->
    [pid()].
start_cluster(Cluster) ->
    [start_one(Name, Cluster) || Name <- Cluster].

-spec start_one(name(), cluster()) ->
    pid().
start_one(Name, Cluster) ->
    {ok, P} = raft_server:start_link(reg_name(Name), options(Name, Cluster)),
    P.

-spec test_write(name(), _Cmd) ->
    _Reply.
test_write(Name, Cmd) ->
    raft_server:write_command(ref(Name), Cmd, mg_utils:default_deadline()).

-spec test_read(name(), _Cmd) ->
    _Reply.
test_read(Name, Cmd) ->
    raft_server:read_command(ref(Name), Cmd, mg_utils:default_deadline()).

-spec options(name(), cluster()) ->
    raft_server:options().
options(Self, Cluster) ->
    #{
        self              => ref(Self),
        others            => ordsets:from_list([ref(Name) || Name <- Cluster -- [Self]]),
        election_timeout  => {150, 300},
        broadcast_timeout => 50
    }.

-spec ref(name()) ->
    mg_utils:gen_ref().
ref(Name) ->
    Name.

-spec reg_name(name()) ->
    mg_utils:gen_reg_name().
reg_name(Name) ->
    {local, Name}.
