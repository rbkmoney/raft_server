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

-module(raft_rpc_sctp_dispatcher).

%% API
-export_type([options /0]).
-export_type([peer    /0]).
-export_type([endpoint/0]).
-export([start_link/2]).
-export([send      /4]).

-include_lib("kernel/include/inet_sctp.hrl").

%% gen_server callbacks
-behaviour(gen_server).
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%%
%% API
%%
-type options() :: #{
    port := inet:port_number(),                          % порт интерконнекта на котором работает данный элемент кластера
    ip   := inet:ip_address() | any | loopback,   % исходящий интерфейс
    socket_options := [gen_sctp:option()]
}.
-type peer() :: {inet:ip_address(), inet:port_number()}.
-type endpoint() :: {peer(), raft_utils:gen_ref()}. % TODO node() вместо peer() (можно попробовать сделать резолвинг)

-spec start_link(raft_utils:gen_reg_name(), options()) ->
    raft_utils:gen_start_ret().
start_link(RegName, Options) ->
    gen_server:start_link(RegName, ?MODULE, Options, []).

-spec send(raft_utils:gen_ref(), endpoint(), endpoint(), raft_rpc:message()) ->
    ok.
send(Ref, From, To, Message) ->
    % TODO возможена перегрузка!!!
    ok = gen_server:cast(Ref, {send, From, To, Message}).

%%
%% gen_server callbacks
%%
-type association() ::
      {connected, association_connected()}
    | {connecting, Buffer::#{raft_utils:gen_ref() => {endpoint(), raft_rpc:message()}}}
    |  disconnected
.
-type association_connected() :: gen_sctp:assoc_id().

-type state() :: #{
    options      := options(),
    socket       := gen_sctp:sctp_socket(),
    associations := #{peer() => association()}
}.


-spec init(options()) ->
    raft_utils:gen_server_init_ret(state()).
init(Options = #{port := Port, ip := IP, socket_options := SocketOptions}) ->
    % при передаче несколько раз одной опции с разными значениями используется первая в списке
    % поэтому тут те опции, от которых зависит логика работы находятся в самом начале
    {ok, Socket} =
        gen_sctp:open(
            [
                {ip, IP},
                {port, Port},
                {active, false},
                {type, seqpacket},
                {sctp_nodelay, true}
            ] ++ SocketOptions
        ),
    ok = gen_sctp:listen(Socket, true),
    State =
        #{
            options      => Options,
            socket       => Socket,
            associations => #{}
        },
    {ok, ready_for_recv(State)}.

-spec handle_call(_Call, raft_utils:gen_server_from(), state()) ->
    raft_utils:gen_server_handle_call_ret(state()).
handle_call(Call, From, State) ->
    ok = error_logger:error_msg("unexpected gen_server call received: ~p from ~p", [Call, From]),
    {noreply, State}.

-spec handle_cast(_, state()) ->
    raft_utils:gen_server_handle_cast_ret(state()).
handle_cast({send, From, {SCTPEndpoint, Ref}, Message}, State) ->
    ToSend = {Ref, From, Message},
    NewAssociation =
        case get_accociation(SCTPEndpoint, State) of
            Association = {connected, _} ->
                ok = do_send(ToSend, Association, State),
                Association;
            Association = {connecting, _} ->
                append_to_connecting_buffer(ToSend, Association);
            disconnected ->
                append_to_connecting_buffer(ToSend, association_connect(SCTPEndpoint, State))
        end,
    {noreply, update_association(SCTPEndpoint, NewAssociation, State)}.

-spec handle_info(_Info, state()) ->
    raft_utils:gen_server_handle_info_ret(state()).
handle_info({sctp, S, IP, Port, Data}, State = #{socket := S}) ->
    SCTPEndpoint = {IP, Port},
    NewAssociation = handle_association_event(get_accociation(SCTPEndpoint, State), Data, State),
    {noreply, ready_for_recv(update_association(SCTPEndpoint, NewAssociation, State))}.

-spec code_change(_, state(), _) ->
    raft_utils:gen_server_code_change_ret(state()).
code_change(_, State, _) ->
    {ok, State}.

-spec terminate(_Reason, state()) ->
    ok.
terminate(_, _) ->
    ok.

-spec ready_for_recv(state()) ->
    state().
ready_for_recv(State = #{socket := Socket}) ->
    ok = inet:setopts(Socket, [{active, once}]),
    State.


%%
%% associations
%%
-spec get_accociation(peer(), state()) ->
    association().
get_accociation(SCTPEndpoint, #{associations := Associations}) ->
    maps:get(SCTPEndpoint, Associations, disconnected).

-spec update_association(peer(), association(), state()) ->
    state().
update_association(SCTPEndpoint, Association, State = #{associations := Associations}) ->
    State#{associations := maps:put(SCTPEndpoint, Association, Associations)}.

-spec handle_association_event(association(), _TODO, state()) ->
    association().
handle_association_event(Association, {_, #sctp_assoc_change{assoc_id = AssocID, state = AccocState}}, State) ->
    case {AccocState, Association} of
        % принятое соединение
        {comm_up, disconnected} ->
            association_connected(AssocID);
        % соединение успешно установленно
        {comm_up, {connecting, Buffer}} ->
            NewAssociation = association_connected(AssocID),
            ok = do_send_all(maps:to_list(Buffer), NewAssociation, State),
            NewAssociation;
        % коннект не прошел
        {cant_assoc, {connecting, _}} ->
            % дропнуть сообщения мы можем, что и делаем (хотя не очень хочется)
            association_disconnected();
        % связь потеряна
        {comm_lost, {connected, _}} ->
            association_disconnected();
        % соединение завершено
        {shutdown_comp, _} ->
            association_disconnected();
        {_, _} ->
            % не понятно в каком стейте будет соединения после этого сообщения
            % поэтому лучше явно упасть и перезапусться
            erlang:exit({'unknown sctp_assoc_change', Association, AccocState})
    end;
handle_association_event({connected, _}, {_, {sctp_shutdown_event, _}}, _) ->
    association_disconnected();
handle_association_event(Association = {connected, _}, {_, Data}, _) ->
    {Ref, From, Message} = erlang:binary_to_term(Data),
    _ = (catch raft_utils:gen_send(Ref, {raft_rpc, From, Message})),
    Association.

%%

-spec association_connected(gen_sctp:assoc_id()) ->
    association().
association_connected(AssocID) ->
    {connected, AssocID}.

-spec association_disconnected() ->
    association().
association_disconnected() ->
    disconnected.

-spec association_connect(peer(), state()) ->
    association().
association_connect({IP, Port}, #{socket := Socket}) ->
    case gen_sctp:connect_init(Socket, IP, Port, [], 1000) of
        ok              -> ok;
        {error,eisconn} -> ok
    end,
    {connecting, #{}}.

-spec append_to_connecting_buffer({raft_utils:gen_ref(), endpoint(), raft_rpc:message()}, association()) ->
    association().
append_to_connecting_buffer({To, From, Message}, {connecting, Buffer}) ->
    {connecting, Buffer#{To => {From, Message}}}.

-spec do_send_all([{raft_utils:gen_ref(), endpoint(), raft_rpc:message()}], association(), state()) ->
    ok.
do_send_all(Messages, Association, State) ->
    ok = lists:foreach(
            fun({Ref, {From, Message}}) ->
                ok = do_send({Ref, From, Message}, Association, State)
            end,
            lists:reverse(Messages)
        ).

-spec do_send({raft_utils:gen_ref(), endpoint(), raft_rpc:message()}, association(), state()) ->
    ok.
do_send({Ref, From, Message}, {connected, Association}, #{socket := Socket}) ->
    case gen_sctp:send(Socket, Association, 0, erlang:term_to_binary({Ref, From, Message})) of
        ok ->
            ok;
        {error, Reason} ->
            ok = error_logger:error_msg("error while sending: ~p", [Reason])
    end.

% если есть соединение — послать
% если нет — начать соединяться и забуферизировать до конца процесса соединения
% при успешном завершении соединения послать весь буффер
% при неуспешном дропнуть буффер?
