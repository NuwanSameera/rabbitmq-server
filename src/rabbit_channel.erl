%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_channel).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-behaviour(gen_server2).

-export([start_link/10, do/2, do/3, flush/1, shutdown/1]).
-export([send_command/2, deliver/4, flushed/2, confirm/2]).
-export([list/0, info_keys/0, info/1, info/2, info_all/0, info_all/1]).
-export([refresh_config_local/0, ready_for_close/1]).
-export([force_event_refresh/0]).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2, handle_pre_hibernate/1, prioritise_call/3,
         prioritise_cast/2, prioritise_info/2, format_message_queue/2]).
%% Internal
-export([list_local/0]).

-record(ch, {state, protocol, channel, reader_pid, writer_pid, conn_pid,
             limiter, tx_status, next_tag,
             unacked_message_q, uncommitted_message_q, uncommitted_acks,
             user, virtual_host, most_recently_declared_queue, queue_monitors,
             consumer_mapping, blocking, queue_consumers, queue_collector_pid,
             stats_timer, confirm_enabled, publish_seqno, unconfirmed_mq,
             unconfirmed_qm, confirmed, capabilities, trace_state}).

-define(MAX_PERMISSION_CACHE_SIZE, 12).

-define(STATISTICS_KEYS,
        [pid,
         transactional,
         confirm,
         consumer_count,
         messages_unacknowledged,
         messages_unconfirmed,
         messages_uncommitted,
         acks_uncommitted,
         prefetch_count,
         client_flow_blocked]).

-define(CREATION_EVENT_KEYS,
        [pid,
         connection,
         number,
         user,
         vhost]).

-define(INFO_KEYS, ?CREATION_EVENT_KEYS ++ ?STATISTICS_KEYS -- [pid]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([channel_number/0]).

-type(channel_number() :: non_neg_integer()).

-spec(start_link/10 ::
        (channel_number(), pid(), pid(), pid(), rabbit_types:protocol(),
         rabbit_types:user(), rabbit_types:vhost(), rabbit_framing:amqp_table(),
         pid(), rabbit_limiter:token()) -> rabbit_types:ok_pid_or_error()).
-spec(do/2 :: (pid(), rabbit_framing:amqp_method_record()) -> 'ok').
-spec(do/3 :: (pid(), rabbit_framing:amqp_method_record(),
               rabbit_types:maybe(rabbit_types:content())) -> 'ok').
-spec(flush/1 :: (pid()) -> 'ok').
-spec(shutdown/1 :: (pid()) -> 'ok').
-spec(send_command/2 :: (pid(), rabbit_framing:amqp_method_record()) -> 'ok').
-spec(deliver/4 ::
        (pid(), rabbit_types:ctag(), boolean(), rabbit_amqqueue:qmsg())
        -> 'ok').
-spec(flushed/2 :: (pid(), pid()) -> 'ok').
-spec(confirm/2 ::(pid(), [non_neg_integer()]) -> 'ok').
-spec(list/0 :: () -> [pid()]).
-spec(list_local/0 :: () -> [pid()]).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(info/1 :: (pid()) -> rabbit_types:infos()).
-spec(info/2 :: (pid(), rabbit_types:info_keys()) -> rabbit_types:infos()).
-spec(info_all/0 :: () -> [rabbit_types:infos()]).
-spec(info_all/1 :: (rabbit_types:info_keys()) -> [rabbit_types:infos()]).
-spec(refresh_config_local/0 :: () -> 'ok').
-spec(ready_for_close/1 :: (pid()) -> 'ok').
-spec(force_event_refresh/0 :: () -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start_link(Channel, ReaderPid, WriterPid, ConnPid, Protocol, User, VHost,
           Capabilities, CollectorPid, Limiter) ->
    gen_server2:start_link(
      ?MODULE, [Channel, ReaderPid, WriterPid, ConnPid, Protocol, User,
                VHost, Capabilities, CollectorPid, Limiter], []).

do(Pid, Method) ->
    do(Pid, Method, none).

do(Pid, Method, Content) ->
    gen_server2:cast(Pid, {method, Method, Content}).

flush(Pid) ->
    gen_server2:call(Pid, flush, infinity).

shutdown(Pid) ->
    gen_server2:cast(Pid, terminate).

send_command(Pid, Msg) ->
    gen_server2:cast(Pid,  {command, Msg}).

deliver(Pid, ConsumerTag, AckRequired, Msg) ->
    gen_server2:cast(Pid, {deliver, ConsumerTag, AckRequired, Msg}).

flushed(Pid, QPid) ->
    gen_server2:cast(Pid, {flushed, QPid}).

confirm(Pid, MsgSeqNos) ->
    gen_server2:cast(Pid, {confirm, MsgSeqNos, self()}).

list() ->
    rabbit_misc:append_rpc_all_nodes(rabbit_mnesia:running_clustered_nodes(),
                                     rabbit_channel, list_local, []).

list_local() ->
    pg_local:get_members(rabbit_channels).

info_keys() -> ?INFO_KEYS.

info(Pid) ->
    gen_server2:call(Pid, info, infinity).

info(Pid, Items) ->
    case gen_server2:call(Pid, {info, Items}, infinity) of
        {ok, Res}      -> Res;
        {error, Error} -> throw(Error)
    end.

info_all() ->
    rabbit_misc:filter_exit_map(fun (C) -> info(C) end, list()).

info_all(Items) ->
    rabbit_misc:filter_exit_map(fun (C) -> info(C, Items) end, list()).

refresh_config_local() ->
    rabbit_misc:upmap(
      fun (C) -> gen_server2:call(C, refresh_config) end, list_local()),
    ok.

ready_for_close(Pid) ->
    gen_server2:cast(Pid, ready_for_close).

force_event_refresh() ->
    [gen_server2:cast(C, force_event_refresh) || C <- list()],
    ok.

%%---------------------------------------------------------------------------

init([Channel, ReaderPid, WriterPid, ConnPid, Protocol, User, VHost,
      Capabilities, CollectorPid, Limiter]) ->
    process_flag(trap_exit, true),
    ok = pg_local:join(rabbit_channels, self()),
    StatsTimer = rabbit_event:init_stats_timer(),
    State = #ch{state                   = starting,
                protocol                = Protocol,
                channel                 = Channel,
                reader_pid              = ReaderPid,
                writer_pid              = WriterPid,
                conn_pid                = ConnPid,
                limiter                 = Limiter,
                tx_status               = none,
                next_tag                = 1,
                unacked_message_q       = queue:new(),
                uncommitted_message_q   = queue:new(),
                uncommitted_acks        = [],
                user                    = User,
                virtual_host            = VHost,
                most_recently_declared_queue = <<>>,
                queue_monitors          = dict:new(),
                consumer_mapping        = dict:new(),
                blocking                = sets:new(),
                queue_consumers         = dict:new(),
                queue_collector_pid     = CollectorPid,
                stats_timer             = StatsTimer,
                confirm_enabled         = false,
                publish_seqno           = 1,
                unconfirmed_mq          = gb_trees:empty(),
                unconfirmed_qm          = gb_trees:empty(),
                confirmed               = [],
                capabilities            = Capabilities,
                trace_state             = rabbit_trace:init(VHost)},
    rabbit_event:notify(channel_created, infos(?CREATION_EVENT_KEYS, State)),
    rabbit_event:if_enabled(StatsTimer,
                            fun() -> emit_stats(State) end),
    {ok, State, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

prioritise_call(Msg, _From, _State) ->
    case Msg of
        info           -> 9;
        {info, _Items} -> 9;
        _              -> 0
    end.

prioritise_cast(Msg, _State) ->
    case Msg of
        {confirm, _MsgSeqNos, _QPid} -> 5;
        _                            -> 0
    end.

prioritise_info(Msg, _State) ->
    case Msg of
        emit_stats                   -> 7;
        _                            -> 0
    end.

handle_call(flush, _From, State) ->
    reply(ok, State);

handle_call(info, _From, State) ->
    reply(infos(?INFO_KEYS, State), State);

handle_call({info, Items}, _From, State) ->
    try
        reply({ok, infos(Items, State)}, State)
    catch Error -> reply({error, Error}, State)
    end;

handle_call(refresh_config, _From, State = #ch{virtual_host = VHost}) ->
    reply(ok, State#ch{trace_state = rabbit_trace:init(VHost)});

handle_call(_Request, _From, State) ->
    noreply(State).

handle_cast({method, Method, Content}, State) ->
    try handle_method(Method, Content, State) of
        {reply, Reply, NewState} ->
            ok = rabbit_writer:send_command(NewState#ch.writer_pid, Reply),
            noreply(NewState);
        {noreply, NewState} ->
            noreply(NewState);
        stop ->
            {stop, normal, State}
    catch
        exit:Reason = #amqp_error{} ->
            MethodName = rabbit_misc:method_record_type(Method),
            send_exception(Reason#amqp_error{method = MethodName}, State);
        _:Reason ->
            {stop, {Reason, erlang:get_stacktrace()}, State}
    end;

handle_cast({flushed, QPid}, State) ->
    {noreply, queue_blocked(QPid, State), hibernate};

handle_cast(ready_for_close, State = #ch{state      = closing,
                                         writer_pid = WriterPid}) ->
    ok = rabbit_writer:send_command_sync(WriterPid, #'channel.close_ok'{}),
    {stop, normal, State};

handle_cast(terminate, State) ->
    {stop, normal, State};

handle_cast({command, #'basic.consume_ok'{consumer_tag = ConsumerTag} = Msg},
            State = #ch{writer_pid = WriterPid}) ->
    ok = rabbit_writer:send_command(WriterPid, Msg),
    noreply(consumer_monitor(ConsumerTag, State));

handle_cast({command, Msg}, State = #ch{writer_pid = WriterPid}) ->
    ok = rabbit_writer:send_command(WriterPid, Msg),
    noreply(State);

handle_cast({deliver, ConsumerTag, AckRequired,
             Msg = {_QName, QPid, _MsgId, Redelivered,
                    #basic_message{exchange_name = ExchangeName,
                                   routing_keys = [RoutingKey | _CcRoutes],
                                   content = Content}}},
            State = #ch{writer_pid  = WriterPid,
                        next_tag    = DeliveryTag,
                        trace_state = TraceState}) ->
    State1 = lock_message(AckRequired,
                          ack_record(DeliveryTag, ConsumerTag, Msg),
                          State),

    M = #'basic.deliver'{consumer_tag = ConsumerTag,
                         delivery_tag = DeliveryTag,
                         redelivered = Redelivered,
                         exchange = ExchangeName#resource.name,
                         routing_key = RoutingKey},
    rabbit_writer:send_command_and_notify(WriterPid, QPid, self(), M, Content),
    State2 = maybe_incr_stats([{QPid, 1}], case AckRequired of
                                               true  -> deliver;
                                               false -> deliver_no_ack
                                           end, State1),
    State3 = maybe_incr_redeliver_stats(Redelivered, QPid, State2),
    rabbit_trace:tap_trace_out(Msg, TraceState),
    noreply(State3#ch{next_tag = DeliveryTag + 1});


handle_cast(force_event_refresh, State) ->
    rabbit_event:notify(channel_created, infos(?CREATION_EVENT_KEYS, State)),
    noreply(State);
handle_cast({confirm, MsgSeqNos, From}, State) ->
    State1 = #ch{confirmed = C} = confirm(MsgSeqNos, From, State),
    noreply([send_confirms], State1, case C of [] -> hibernate; _ -> 0 end).

handle_info(timeout, State) ->
    noreply(State);

handle_info(emit_stats, State = #ch{stats_timer = StatsTimer}) ->
    emit_stats(State),
    noreply([ensure_stats_timer],
            State#ch{stats_timer = rabbit_event:reset_stats_timer(StatsTimer)});

handle_info({'DOWN', _MRef, process, QPid, Reason}, State) ->
    State1 = handle_publishing_queue_down(QPid, Reason, State),
    State2 = queue_blocked(QPid, State1),
    State3 = handle_consuming_queue_down(QPid, State2),
    erase_queue_stats(QPid),
    noreply(State3#ch{queue_monitors =
                          dict:erase(QPid, State3#ch.queue_monitors)});

handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State}.

handle_pre_hibernate(State = #ch{stats_timer = StatsTimer}) ->
    ok = clear_permission_cache(),
    rabbit_event:if_enabled(
      StatsTimer, fun () -> emit_stats(State, [{idle_since, now()}]) end),
    StatsTimer1 = rabbit_event:stop_stats_timer(StatsTimer),
    {hibernate, State#ch{stats_timer = StatsTimer1}}.

terminate(Reason, State) ->
    {Res, _State1} = notify_queues(State),
    case Reason of
        normal            -> ok = Res;
        shutdown          -> ok = Res;
        {shutdown, _Term} -> ok = Res;
        _                 -> ok
    end,
    pg_local:leave(rabbit_channels, self()),
    rabbit_event:notify(channel_closed, [{pid, self()}]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).

%%---------------------------------------------------------------------------

reply(Reply, NewState) -> reply(Reply, [], NewState).

reply(Reply, Mask, NewState) -> reply(Reply, Mask, NewState, hibernate).

reply(Reply, Mask, NewState, Timeout) ->
    {reply, Reply, next_state(Mask, NewState), Timeout}.

noreply(NewState) -> noreply([], NewState).

noreply(Mask, NewState) -> noreply(Mask, NewState, hibernate).

noreply(Mask, NewState, Timeout) ->
    {noreply, next_state(Mask, NewState), Timeout}.

next_state(Mask, State) ->
    lists:foldl(fun (ensure_stats_timer, State1) -> ensure_stats_timer(State1);
                    (send_confirms,      State1) -> send_confirms(State1)
                end, State, [ensure_stats_timer, send_confirms] -- Mask).

ensure_stats_timer(State = #ch{stats_timer = StatsTimer}) ->
    State#ch{stats_timer = rabbit_event:ensure_stats_timer(
                             StatsTimer, self(), emit_stats)}.

return_ok(State, true, _Msg)  -> {noreply, State};
return_ok(State, false, Msg)  -> {reply, Msg, State}.

ok_msg(true, _Msg) -> undefined;
ok_msg(false, Msg) -> Msg.

send_exception(Reason, State = #ch{protocol   = Protocol,
                                   channel    = Channel,
                                   writer_pid = WriterPid,
                                   reader_pid = ReaderPid,
                                   conn_pid   = ConnPid}) ->
    {CloseChannel, CloseMethod} =
        rabbit_binary_generator:map_exception(Channel, Reason, Protocol),
    rabbit_log:error("connection ~p, channel ~p - error:~n~p~n",
                     [ConnPid, Channel, Reason]),
    %% something bad's happened: notify_queues may not be 'ok'
    {_Result, State1} = notify_queues(State),
    case CloseChannel of
        Channel -> ok = rabbit_writer:send_command(WriterPid, CloseMethod),
                   {noreply, State1};
        _       -> ReaderPid ! {channel_exit, Channel, Reason},
                   {stop, normal, State1}
    end.

return_queue_declare_ok(#resource{name = ActualName},
                        NoWait, MessageCount, ConsumerCount, State) ->
    return_ok(State#ch{most_recently_declared_queue = ActualName}, NoWait,
              #'queue.declare_ok'{queue          = ActualName,
                                  message_count  = MessageCount,
                                  consumer_count = ConsumerCount}).

check_resource_access(User, Resource, Perm) ->
    V = {Resource, Perm},
    Cache = case get(permission_cache) of
                undefined -> [];
                Other     -> Other
            end,
    CacheTail =
        case lists:member(V, Cache) of
            true  -> lists:delete(V, Cache);
            false -> ok = rabbit_access_control:check_resource_access(
                            User, Resource, Perm),
                     lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE - 1)
        end,
    put(permission_cache, [V | CacheTail]),
    ok.

clear_permission_cache() ->
    erase(permission_cache),
    ok.

check_configure_permitted(Resource, #ch{user = User}) ->
    check_resource_access(User, Resource, configure).

check_write_permitted(Resource, #ch{user = User}) ->
    check_resource_access(User, Resource, write).

check_read_permitted(Resource, #ch{user = User}) ->
    check_resource_access(User, Resource, read).

check_user_id_header(#'P_basic'{user_id = undefined}, _) ->
    ok;
check_user_id_header(#'P_basic'{user_id = Username},
                     #ch{user = #user{username = Username}}) ->
    ok;
check_user_id_header(#'P_basic'{user_id = Claimed},
                     #ch{user = #user{username = Actual}}) ->
    rabbit_misc:protocol_error(
      precondition_failed, "user_id property set to '~s' but "
      "authenticated user was '~s'", [Claimed, Actual]).

check_internal_exchange(#exchange{name = Name, internal = true}) ->
    rabbit_misc:protocol_error(access_refused,
                               "cannot publish to internal ~s",
                               [rabbit_misc:rs(Name)]);
check_internal_exchange(_) ->
    ok.

expand_queue_name_shortcut(<<>>, #ch{most_recently_declared_queue = <<>>}) ->
    rabbit_misc:protocol_error(
      not_found, "no previously declared queue", []);
expand_queue_name_shortcut(<<>>, #ch{virtual_host = VHostPath,
                                     most_recently_declared_queue = MRDQ}) ->
    rabbit_misc:r(VHostPath, queue, MRDQ);
expand_queue_name_shortcut(QueueNameBin, #ch{virtual_host = VHostPath}) ->
    rabbit_misc:r(VHostPath, queue, QueueNameBin).

expand_routing_key_shortcut(<<>>, <<>>,
                            #ch{most_recently_declared_queue = <<>>}) ->
    rabbit_misc:protocol_error(
      not_found, "no previously declared queue", []);
expand_routing_key_shortcut(<<>>, <<>>,
                            #ch{most_recently_declared_queue = MRDQ}) ->
    MRDQ;
expand_routing_key_shortcut(_QueueNameBin, RoutingKey, _State) ->
    RoutingKey.

expand_binding(queue, DestinationNameBin, RoutingKey, State) ->
    {expand_queue_name_shortcut(DestinationNameBin, State),
     expand_routing_key_shortcut(DestinationNameBin, RoutingKey, State)};
expand_binding(exchange, DestinationNameBin, RoutingKey, State) ->
    {rabbit_misc:r(State#ch.virtual_host, exchange, DestinationNameBin),
     RoutingKey}.

check_not_default_exchange(#resource{kind = exchange, name = <<"">>}) ->
    rabbit_misc:protocol_error(
      access_refused, "operation not permitted on the default exchange", []);
check_not_default_exchange(_) ->
    ok.

%% check that an exchange/queue name does not contain the reserved
%% "amq."  prefix.
%%
%% One, quite reasonable, interpretation of the spec, taken by the
%% QPid M1 Java client, is that the exclusion of "amq." prefixed names
%% only applies on actual creation, and not in the cases where the
%% entity already exists. This is how we use this function in the code
%% below. However, AMQP JIRA 123 changes that in 0-10, and possibly
%% 0-9SP1, making it illegal to attempt to declare an exchange/queue
%% with an amq.* name when passive=false. So this will need
%% revisiting.
%%
%% TODO: enforce other constraints on name. See AMQP JIRA 69.
check_name(Kind, NameBin = <<"amq.", _/binary>>) ->
    rabbit_misc:protocol_error(
      access_refused,
      "~s name '~s' contains reserved prefix 'amq.*'",[Kind, NameBin]);
check_name(_Kind, NameBin) ->
    NameBin.

queue_blocked(QPid, State = #ch{blocking = Blocking}) ->
    case sets:is_element(QPid, Blocking) of
        false -> State;
        true  -> Blocking1 = sets:del_element(QPid, Blocking),
                 ok = case sets:size(Blocking1) of
                          0 -> rabbit_writer:send_command(
                                 State#ch.writer_pid,
                                 #'channel.flow_ok'{active = false});
                          _ -> ok
                      end,
                 demonitor_queue(QPid, State#ch{blocking = Blocking1})
    end.

record_confirm(undefined, _, State) ->
    State;
record_confirm(MsgSeqNo, XName, State) ->
    record_confirms([{MsgSeqNo, XName}], State).

record_confirms([], State) ->
    State;
record_confirms(MXs, State = #ch{confirmed = C}) ->
    State#ch{confirmed = [MXs | C]}.

confirm([], _QPid, State) ->
    State;
confirm(MsgSeqNos, QPid, State) ->
    {MXs, State1} = process_confirms(MsgSeqNos, QPid, false, State),
    record_confirms(MXs, State1).

process_confirms(MsgSeqNos, QPid, Nack, State) ->
    lists:foldl(
      fun(MsgSeqNo, {_MXs, _State = #ch{unconfirmed_mq = UMQ0}} = Acc) ->
              case gb_trees:lookup(MsgSeqNo, UMQ0) of
                  {value, XQ} -> remove_unconfirmed(MsgSeqNo, QPid, XQ,
                                                    Acc, Nack);
                  none        -> Acc
              end
      end, {[], State}, MsgSeqNos).

remove_unconfirmed(MsgSeqNo, QPid, {XName, Qs},
                   {MXs, State = #ch{unconfirmed_mq = UMQ,
                                     unconfirmed_qm = UQM}},
                   Nack) ->
    State1 = case gb_trees:lookup(QPid, UQM) of
                 {value, MsgSeqNos} ->
                     MsgSeqNos1 = gb_sets:delete(MsgSeqNo, MsgSeqNos),
                     case gb_sets:is_empty(MsgSeqNos1) of
                         true  -> UQM1 = gb_trees:delete(QPid, UQM),
                                  demonitor_queue(
                                    QPid, State#ch{unconfirmed_qm = UQM1});
                         false -> UQM1 = gb_trees:update(QPid, MsgSeqNos1, UQM),
                                  State#ch{unconfirmed_qm = UQM1}
                     end;
                 none ->
                     State
             end,
    Qs1 = gb_sets:del_element(QPid, Qs),
    %% If QPid somehow died initiating a nack, clear the message from
    %% internal data-structures.  Also, cleanup empty entries.
    case (Nack orelse gb_sets:is_empty(Qs1)) of
        true  -> UMQ1 = gb_trees:delete(MsgSeqNo, UMQ),
                 {[{MsgSeqNo, XName} | MXs], State1#ch{unconfirmed_mq = UMQ1}};
        false -> UMQ1 = gb_trees:update(MsgSeqNo, {XName, Qs1}, UMQ),
                 {MXs, State1#ch{unconfirmed_mq = UMQ1}}
    end.

handle_method(#'channel.open'{}, _, State = #ch{state = starting}) ->
    {reply, #'channel.open_ok'{}, State#ch{state = running}};

handle_method(#'channel.open'{}, _, _State) ->
    rabbit_misc:protocol_error(
      command_invalid, "second 'channel.open' seen", []);

handle_method(_Method, _, #ch{state = starting}) ->
    rabbit_misc:protocol_error(channel_error, "expected 'channel.open'", []);

handle_method(#'channel.close_ok'{}, _, #ch{state = closing}) ->
    stop;

handle_method(#'channel.close'{}, _, State = #ch{state = closing}) ->
    {reply, #'channel.close_ok'{}, State};

handle_method(_Method, _, State = #ch{state = closing}) ->
    {noreply, State};

handle_method(#'channel.close'{}, _, State = #ch{reader_pid = ReaderPid}) ->
    {ok, State1} = notify_queues(State),
    ReaderPid ! {channel_closing, self()},
    {noreply, State1};

%% Even though the spec prohibits the client from sending commands
%% while waiting for the reply to a synchronous command, we generally
%% do allow this...except in the case of a pending tx.commit, where
%% it could wreak havoc.
handle_method(_Method, _, #ch{tx_status = TxStatus})
  when TxStatus =/= none andalso TxStatus =/= in_progress ->
    rabbit_misc:protocol_error(
      channel_error, "unexpected command while processing 'tx.commit'", []);

handle_method(#'access.request'{},_, State) ->
    {reply, #'access.request_ok'{ticket = 1}, State};

handle_method(#'basic.publish'{exchange    = ExchangeNameBin,
                               routing_key = RoutingKey,
                               mandatory   = Mandatory,
                               immediate   = Immediate},
              Content, State = #ch{virtual_host    = VHostPath,
                                   tx_status       = TxStatus,
                                   confirm_enabled = ConfirmEnabled,
                                   trace_state     = TraceState}) ->
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    check_write_permitted(ExchangeName, State),
    Exchange = rabbit_exchange:lookup_or_die(ExchangeName),
    check_internal_exchange(Exchange),
    %% We decode the content's properties here because we're almost
    %% certain to want to look at delivery-mode and priority.
    DecodedContent = rabbit_binary_parser:ensure_content_decoded(Content),
    check_user_id_header(DecodedContent#content.properties, State),
    {MsgSeqNo, State1} =
        case {TxStatus, ConfirmEnabled} of
            {none, false} -> {undefined, State};
            {_, _}        -> SeqNo = State#ch.publish_seqno,
                             {SeqNo, State#ch{publish_seqno = SeqNo + 1}}
        end,
    case rabbit_basic:message(ExchangeName, RoutingKey, DecodedContent) of
        {ok, Message} ->
            rabbit_trace:tap_trace_in(Message, TraceState),
            Delivery = rabbit_basic:delivery(Mandatory, Immediate, Message,
                                             MsgSeqNo),
            QNames = rabbit_exchange:route(Exchange, Delivery),
            {noreply,
             case TxStatus of
                 none        -> deliver_to_queues({Delivery, QNames}, State1);
                 in_progress -> TMQ = State1#ch.uncommitted_message_q,
                                NewTMQ = queue:in({Delivery, QNames}, TMQ),
                                State1#ch{uncommitted_message_q = NewTMQ}
             end};
        {error, Reason} ->
            rabbit_misc:protocol_error(precondition_failed,
                                       "invalid message: ~p", [Reason])
    end;

handle_method(#'basic.nack'{delivery_tag = DeliveryTag,
                            multiple     = Multiple,
                            requeue      = Requeue},
              _, State) ->
    reject(DeliveryTag, Requeue, Multiple, State);

handle_method(#'basic.ack'{delivery_tag = DeliveryTag,
                           multiple = Multiple},
              _, State = #ch{unacked_message_q = UAMQ, tx_status = TxStatus}) ->
    {Acked, Remaining} = collect_acks(UAMQ, DeliveryTag, Multiple),
    State1 = State#ch{unacked_message_q = Remaining},
    {noreply,
     case TxStatus of
         none        -> ack(Acked, State1);
         in_progress -> State1#ch{uncommitted_acks =
                                      Acked ++ State1#ch.uncommitted_acks}
     end};

handle_method(#'basic.get'{queue = QueueNameBin,
                           no_ack = NoAck},
              _, State = #ch{writer_pid  = WriterPid,
                             conn_pid    = ConnPid,
                             next_tag    = DeliveryTag,
                             trace_state = TraceState}) ->
    QueueName = expand_queue_name_shortcut(QueueNameBin, State),
    check_read_permitted(QueueName, State),
    case rabbit_amqqueue:with_exclusive_access_or_die(
           QueueName, ConnPid,
           fun (Q) -> rabbit_amqqueue:basic_get(Q, self(), NoAck) end) of
        {ok, MessageCount,
         Msg = {_QName, QPid, _MsgId, Redelivered,
                #basic_message{exchange_name = ExchangeName,
                               routing_keys = [RoutingKey | _CcRoutes],
                               content = Content}}} ->
            State1 = lock_message(not(NoAck),
                                  ack_record(DeliveryTag, none, Msg),
                                  State),
            State2 = maybe_incr_stats([{QPid, 1}], case NoAck of
                                                       true  -> get_no_ack;
                                                       false -> get
                                                   end, State1),
            State3 = maybe_incr_redeliver_stats(Redelivered, QPid, State2),
            rabbit_trace:tap_trace_out(Msg, TraceState),
            ok = rabbit_writer:send_command(
                   WriterPid,
                   #'basic.get_ok'{delivery_tag = DeliveryTag,
                                   redelivered = Redelivered,
                                   exchange = ExchangeName#resource.name,
                                   routing_key = RoutingKey,
                                   message_count = MessageCount},
                   Content),
            {noreply, State3#ch{next_tag = DeliveryTag + 1}};
        empty ->
            {reply, #'basic.get_empty'{}, State}
    end;

handle_method(#'basic.consume'{queue        = QueueNameBin,
                               consumer_tag = ConsumerTag,
                               no_local     = _, % FIXME: implement
                               no_ack       = NoAck,
                               exclusive    = ExclusiveConsume,
                               nowait       = NoWait},
              _, State = #ch{conn_pid          = ConnPid,
                             limiter           = Limiter,
                             consumer_mapping  = ConsumerMapping}) ->
    case dict:find(ConsumerTag, ConsumerMapping) of
        error ->
            QueueName = expand_queue_name_shortcut(QueueNameBin, State),
            check_read_permitted(QueueName, State),
            ActualConsumerTag =
                case ConsumerTag of
                    <<>>  -> rabbit_guid:binstring_guid("amq.ctag");
                    Other -> Other
                end,

            %% We get the queue process to send the consume_ok on our
            %% behalf. This is for symmetry with basic.cancel - see
            %% the comment in that method for why.
            case rabbit_amqqueue:with_exclusive_access_or_die(
                   QueueName, ConnPid,
                   fun (Q) ->
                           {rabbit_amqqueue:basic_consume(
                              Q, NoAck, self(), Limiter,
                              ActualConsumerTag, ExclusiveConsume,
                              ok_msg(NoWait, #'basic.consume_ok'{
                                       consumer_tag = ActualConsumerTag})),
                            Q}
                   end) of
                {ok, Q} ->
                    State1 = State#ch{consumer_mapping =
                                          dict:store(ActualConsumerTag, Q,
                                                     ConsumerMapping)},
                    {noreply,
                     case NoWait of
                         true  -> consumer_monitor(ActualConsumerTag, State1);
                         false -> State1
                     end};
                {{error, exclusive_consume_unavailable}, _Q} ->
                    rabbit_misc:protocol_error(
                      access_refused, "~s in exclusive use",
                      [rabbit_misc:rs(QueueName)])
            end;
        {ok, _} ->
            %% Attempted reuse of consumer tag.
            rabbit_misc:protocol_error(
              not_allowed, "attempt to reuse consumer tag '~s'", [ConsumerTag])
    end;

handle_method(#'basic.cancel'{consumer_tag = ConsumerTag,
                              nowait = NoWait},
              _, State = #ch{consumer_mapping = ConsumerMapping,
                             queue_consumers  = QCons}) ->
    OkMsg = #'basic.cancel_ok'{consumer_tag = ConsumerTag},
    case dict:find(ConsumerTag, ConsumerMapping) of
        error ->
            %% Spec requires we ignore this situation.
            return_ok(State, NoWait, OkMsg);
        {ok, Q = #amqqueue{pid = QPid}} ->
            ConsumerMapping1 = dict:erase(ConsumerTag, ConsumerMapping),
            QCons1 =
                case dict:find(QPid, QCons) of
                    error       -> QCons;
                    {ok, CTags} -> CTags1 = gb_sets:delete(ConsumerTag, CTags),
                                   case gb_sets:is_empty(CTags1) of
                                       true  -> dict:erase(QPid, QCons);
                                       false -> dict:store(QPid, CTags1, QCons)
                                   end
                end,
            NewState = demonitor_queue(
                         Q, State#ch{consumer_mapping = ConsumerMapping1,
                                     queue_consumers  = QCons1}),
            %% In order to ensure that no more messages are sent to
            %% the consumer after the cancel_ok has been sent, we get
            %% the queue process to send the cancel_ok on our
            %% behalf. If we were sending the cancel_ok ourselves it
            %% might overtake a message sent previously by the queue.
            case rabbit_misc:with_exit_handler(
                   fun () -> {error, not_found} end,
                   fun () ->
                           rabbit_amqqueue:basic_cancel(
                             Q, self(), ConsumerTag,
                             ok_msg(NoWait, #'basic.cancel_ok'{
                                      consumer_tag = ConsumerTag}))
                   end) of
                ok ->
                    {noreply, NewState};
                {error, not_found} ->
                    %% Spec requires we ignore this situation.
                    return_ok(NewState, NoWait, OkMsg)
            end
    end;

handle_method(#'basic.qos'{global = true}, _, _State) ->
    rabbit_misc:protocol_error(not_implemented, "global=true", []);

handle_method(#'basic.qos'{prefetch_size = Size}, _, _State) when Size /= 0 ->
    rabbit_misc:protocol_error(not_implemented,
                               "prefetch_size!=0 (~w)", [Size]);

handle_method(#'basic.qos'{prefetch_count = PrefetchCount}, _,
              State = #ch{limiter = Limiter}) ->
    Limiter1 = case {rabbit_limiter:is_enabled(Limiter), PrefetchCount} of
                   {false, 0} -> Limiter;
                   {false, _} -> enable_limiter(State);
                   {_, _}     -> Limiter
               end,
    Limiter3 = case rabbit_limiter:limit(Limiter1, PrefetchCount) of
                   ok                   -> Limiter1;
                   {disabled, Limiter2} -> ok = limit_queues(Limiter2, State),
                                           Limiter2
               end,
    {reply, #'basic.qos_ok'{}, State#ch{limiter = Limiter3}};

handle_method(#'basic.recover_async'{requeue = true},
              _, State = #ch{unacked_message_q = UAMQ,
                             limiter = Limiter}) ->
    OkFun = fun () -> ok end,
    UAMQL = queue:to_list(UAMQ),
    ok = fold_per_queue(
           fun (QPid, MsgIds, ok) ->
                   rabbit_misc:with_exit_handler(
                     OkFun, fun () ->
                                    rabbit_amqqueue:requeue(
                                      QPid, MsgIds, self())
                            end)
           end, ok, UAMQL),
    ok = notify_limiter(Limiter, UAMQL),
    %% No answer required - basic.recover is the newer, synchronous
    %% variant of this method
    {noreply, State#ch{unacked_message_q = queue:new()}};

handle_method(#'basic.recover_async'{requeue = false}, _, _State) ->
    rabbit_misc:protocol_error(not_implemented, "requeue=false", []);

handle_method(#'basic.recover'{requeue = Requeue}, Content, State) ->
    {noreply, State2 = #ch{writer_pid = WriterPid}} =
        handle_method(#'basic.recover_async'{requeue = Requeue},
                      Content,
                      State),
    ok = rabbit_writer:send_command(WriterPid, #'basic.recover_ok'{}),
    {noreply, State2};

handle_method(#'basic.reject'{delivery_tag = DeliveryTag,
                              requeue = Requeue},
              _, State) ->
    reject(DeliveryTag, Requeue, false, State);

handle_method(#'exchange.declare'{exchange = ExchangeNameBin,
                                  type = TypeNameBin,
                                  passive = false,
                                  durable = Durable,
                                  auto_delete = AutoDelete,
                                  internal = Internal,
                                  nowait = NoWait,
                                  arguments = Args},
              _, State = #ch{virtual_host = VHostPath}) ->
    CheckedType = rabbit_exchange:check_type(TypeNameBin),
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    check_not_default_exchange(ExchangeName),
    check_configure_permitted(ExchangeName, State),
    X = case rabbit_exchange:lookup(ExchangeName) of
            {ok, FoundX} -> FoundX;
            {error, not_found} ->
                check_name('exchange', ExchangeNameBin),
                case rabbit_misc:r_arg(VHostPath, exchange, Args,
                                       <<"alternate-exchange">>) of
                    undefined -> ok;
                    AName     -> check_read_permitted(ExchangeName, State),
                                 check_write_permitted(AName, State),
                                 ok
                end,
                rabbit_exchange:declare(ExchangeName,
                                        CheckedType,
                                        Durable,
                                        AutoDelete,
                                        Internal,
                                        Args)
        end,
    ok = rabbit_exchange:assert_equivalence(X, CheckedType, Durable,
                                            AutoDelete, Internal, Args),
    return_ok(State, NoWait, #'exchange.declare_ok'{});

handle_method(#'exchange.declare'{exchange = ExchangeNameBin,
                                  passive = true,
                                  nowait = NoWait},
              _, State = #ch{virtual_host = VHostPath}) ->
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    check_not_default_exchange(ExchangeName),
    _ = rabbit_exchange:lookup_or_die(ExchangeName),
    return_ok(State, NoWait, #'exchange.declare_ok'{});

handle_method(#'exchange.delete'{exchange = ExchangeNameBin,
                                 if_unused = IfUnused,
                                 nowait = NoWait},
              _, State = #ch{virtual_host = VHostPath}) ->
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    check_not_default_exchange(ExchangeName),
    check_configure_permitted(ExchangeName, State),
    case rabbit_exchange:delete(ExchangeName, IfUnused) of
        {error, not_found} ->
            rabbit_misc:not_found(ExchangeName);
        {error, in_use} ->
            rabbit_misc:protocol_error(
              precondition_failed, "~s in use", [rabbit_misc:rs(ExchangeName)]);
        ok ->
            return_ok(State, NoWait,  #'exchange.delete_ok'{})
    end;

handle_method(#'exchange.bind'{destination = DestinationNameBin,
                               source = SourceNameBin,
                               routing_key = RoutingKey,
                               nowait = NoWait,
                               arguments = Arguments}, _, State) ->
    binding_action(fun rabbit_binding:add/2,
                   SourceNameBin, exchange, DestinationNameBin, RoutingKey,
                   Arguments, #'exchange.bind_ok'{}, NoWait, State);

handle_method(#'exchange.unbind'{destination = DestinationNameBin,
                                 source = SourceNameBin,
                                 routing_key = RoutingKey,
                                 nowait = NoWait,
                                 arguments = Arguments}, _, State) ->
    binding_action(fun rabbit_binding:remove/2,
                   SourceNameBin, exchange, DestinationNameBin, RoutingKey,
                   Arguments, #'exchange.unbind_ok'{}, NoWait, State);

handle_method(#'queue.declare'{queue       = QueueNameBin,
                               passive     = false,
                               durable     = Durable,
                               exclusive   = ExclusiveDeclare,
                               auto_delete = AutoDelete,
                               nowait      = NoWait,
                               arguments   = Args} = Declare,
              _, State = #ch{virtual_host        = VHostPath,
                             conn_pid            = ConnPid,
                             queue_collector_pid = CollectorPid}) ->
    Owner = case ExclusiveDeclare of
                true  -> ConnPid;
                false -> none
            end,
    ActualNameBin = case QueueNameBin of
                        <<>>  -> rabbit_guid:binstring_guid("amq.gen");
                        Other -> check_name('queue', Other)
                    end,
    QueueName = rabbit_misc:r(VHostPath, queue, ActualNameBin),
    check_configure_permitted(QueueName, State),
    case rabbit_amqqueue:with(
           QueueName,
           fun (Q) -> ok = rabbit_amqqueue:assert_equivalence(
                             Q, Durable, AutoDelete, Args, Owner),
                      rabbit_amqqueue:stat(Q)
           end) of
        {ok, MessageCount, ConsumerCount} ->
            return_queue_declare_ok(QueueName, NoWait, MessageCount,
                                    ConsumerCount, State);
        {error, not_found} ->
            case rabbit_amqqueue:declare(QueueName, Durable, AutoDelete,
                                         Args, Owner) of
                {new, Q = #amqqueue{}} ->
                    %% We need to notify the reader within the channel
                    %% process so that we can be sure there are no
                    %% outstanding exclusive queues being declared as
                    %% the connection shuts down.
                    ok = case Owner of
                             none -> ok;
                             _    -> rabbit_queue_collector:register(
                                       CollectorPid, Q)
                         end,
                    return_queue_declare_ok(QueueName, NoWait, 0, 0, State);
                {existing, _Q} ->
                    %% must have been created between the stat and the
                    %% declare. Loop around again.
                    handle_method(Declare, none, State)
            end
    end;

handle_method(#'queue.declare'{queue   = QueueNameBin,
                               passive = true,
                               nowait  = NoWait},
              _, State = #ch{virtual_host = VHostPath,
                             conn_pid     = ConnPid}) ->
    QueueName = rabbit_misc:r(VHostPath, queue, QueueNameBin),
    {{ok, MessageCount, ConsumerCount}, #amqqueue{} = Q} =
        rabbit_amqqueue:with_or_die(
          QueueName, fun (Q) -> {rabbit_amqqueue:stat(Q), Q} end),
    ok = rabbit_amqqueue:check_exclusive_access(Q, ConnPid),
    return_queue_declare_ok(QueueName, NoWait, MessageCount, ConsumerCount,
                            State);

handle_method(#'queue.delete'{queue = QueueNameBin,
                              if_unused = IfUnused,
                              if_empty = IfEmpty,
                              nowait = NoWait},
              _, State = #ch{conn_pid = ConnPid}) ->
    QueueName = expand_queue_name_shortcut(QueueNameBin, State),
    check_configure_permitted(QueueName, State),
    case rabbit_amqqueue:with_exclusive_access_or_die(
           QueueName, ConnPid,
           fun (Q) -> rabbit_amqqueue:delete(Q, IfUnused, IfEmpty) end) of
        {error, in_use} ->
            rabbit_misc:protocol_error(
              precondition_failed, "~s in use", [rabbit_misc:rs(QueueName)]);
        {error, not_empty} ->
            rabbit_misc:protocol_error(
              precondition_failed, "~s not empty", [rabbit_misc:rs(QueueName)]);
        {ok, PurgedMessageCount} ->
            return_ok(State, NoWait,
                      #'queue.delete_ok'{message_count = PurgedMessageCount})
    end;

handle_method(#'queue.bind'{queue = QueueNameBin,
                            exchange = ExchangeNameBin,
                            routing_key = RoutingKey,
                            nowait = NoWait,
                            arguments = Arguments}, _, State) ->
    binding_action(fun rabbit_binding:add/2,
                   ExchangeNameBin, queue, QueueNameBin, RoutingKey, Arguments,
                   #'queue.bind_ok'{}, NoWait, State);

handle_method(#'queue.unbind'{queue = QueueNameBin,
                              exchange = ExchangeNameBin,
                              routing_key = RoutingKey,
                              arguments = Arguments}, _, State) ->
    binding_action(fun rabbit_binding:remove/2,
                   ExchangeNameBin, queue, QueueNameBin, RoutingKey, Arguments,
                   #'queue.unbind_ok'{}, false, State);

handle_method(#'queue.purge'{queue = QueueNameBin,
                             nowait = NoWait},
              _, State = #ch{conn_pid = ConnPid}) ->
    QueueName = expand_queue_name_shortcut(QueueNameBin, State),
    check_read_permitted(QueueName, State),
    {ok, PurgedMessageCount} = rabbit_amqqueue:with_exclusive_access_or_die(
                                 QueueName, ConnPid,
                                 fun (Q) -> rabbit_amqqueue:purge(Q) end),
    return_ok(State, NoWait,
              #'queue.purge_ok'{message_count = PurgedMessageCount});

handle_method(#'tx.select'{}, _, #ch{confirm_enabled = true}) ->
    rabbit_misc:protocol_error(
      precondition_failed, "cannot switch from confirm to tx mode", []);

handle_method(#'tx.select'{}, _, State) ->
    {reply, #'tx.select_ok'{}, State#ch{tx_status = in_progress}};

handle_method(#'tx.commit'{}, _, #ch{tx_status = none}) ->
    rabbit_misc:protocol_error(
      precondition_failed, "channel is not transactional", []);

handle_method(#'tx.commit'{}, _, State = #ch{uncommitted_message_q = TMQ,
                                             uncommitted_acks      = TAL}) ->
    State1 = new_tx(ack(TAL, rabbit_misc:queue_fold(fun deliver_to_queues/2,
                                                    State, TMQ))),
    {noreply, maybe_complete_tx(State1#ch{tx_status = committing})};

handle_method(#'tx.rollback'{}, _, #ch{tx_status = none}) ->
    rabbit_misc:protocol_error(
      precondition_failed, "channel is not transactional", []);

handle_method(#'tx.rollback'{}, _, State = #ch{unacked_message_q = UAMQ,
                                               uncommitted_acks  = TAL}) ->
    TAQ = queue:from_list(lists:reverse(TAL)),
    {reply, #'tx.rollback_ok'{},
     new_tx(State#ch{unacked_message_q = queue:join(TAQ, UAMQ)})};

handle_method(#'confirm.select'{}, _, #ch{tx_status = in_progress}) ->
    rabbit_misc:protocol_error(
      precondition_failed, "cannot switch from tx to confirm mode", []);

handle_method(#'confirm.select'{nowait = NoWait}, _, State) ->
    return_ok(State#ch{confirm_enabled = true},
              NoWait, #'confirm.select_ok'{});

handle_method(#'channel.flow'{active = true}, _,
              State = #ch{limiter = Limiter}) ->
    Limiter2 = case rabbit_limiter:unblock(Limiter) of
                   ok                   -> Limiter;
                   {disabled, Limiter1} -> ok = limit_queues(Limiter1, State),
                                           Limiter1
               end,
    {reply, #'channel.flow_ok'{active = true}, State#ch{limiter = Limiter2}};

handle_method(#'channel.flow'{active = false}, _,
              State = #ch{consumer_mapping = Consumers,
                          limiter          = Limiter}) ->
    Limiter1 = case rabbit_limiter:is_enabled(Limiter) of
                   true  -> Limiter;
                   false -> enable_limiter(State)
               end,
    State1 = State#ch{limiter = Limiter1},
    ok = rabbit_limiter:block(Limiter1),
    case consumer_queues(Consumers) of
        []    -> {reply, #'channel.flow_ok'{active = false}, State1};
        QPids -> State2 = lists:foldl(fun monitor_queue/2,
                                      State1#ch{blocking =
                                                    sets:from_list(QPids)},
                                      QPids),
                 ok = rabbit_amqqueue:flush_all(QPids, self()),
                 {noreply, State2}
    end;

handle_method(_MethodRecord, _Content, _State) ->
    rabbit_misc:protocol_error(
      command_invalid, "unimplemented method", []).

%%----------------------------------------------------------------------------

consumer_monitor(ConsumerTag,
                 State = #ch{consumer_mapping = ConsumerMapping,
                             queue_consumers  = QCons,
                             capabilities     = Capabilities}) ->
    case rabbit_misc:table_lookup(
           Capabilities, <<"consumer_cancel_notify">>) of
        {bool, true} ->
            #amqqueue{pid = QPid} = dict:fetch(ConsumerTag, ConsumerMapping),
            QCons1 = dict:update(QPid,
                                 fun (CTags) ->
                                         gb_sets:insert(ConsumerTag, CTags)
                                 end,
                                 gb_sets:singleton(ConsumerTag),
                                 QCons),
            monitor_queue(QPid, State#ch{queue_consumers = QCons1});
        _ ->
            State
    end.

monitor_queue(QPid, State = #ch{queue_monitors = QMons}) ->
    case (not dict:is_key(QPid, QMons) andalso
          queue_monitor_needed(QPid, State)) of
        true  -> MRef = erlang:monitor(process, QPid),
                 State#ch{queue_monitors = dict:store(QPid, MRef, QMons)};
        false -> State
    end.

demonitor_queue(QPid, State = #ch{queue_monitors = QMons}) ->
    case (dict:is_key(QPid, QMons) andalso
          not queue_monitor_needed(QPid, State)) of
        true  -> true = erlang:demonitor(dict:fetch(QPid, QMons)),
                 State#ch{queue_monitors = dict:erase(QPid, QMons)};
        false -> State
    end.

queue_monitor_needed(QPid, #ch{stats_timer     = StatsTimer,
                               queue_consumers = QCons,
                               blocking        = Blocking,
                               unconfirmed_qm  = UQM}) ->
    StatsEnabled      = rabbit_event:stats_level(StatsTimer) =:= fine,
    ConsumerMonitored = dict:is_key(QPid, QCons),
    QueueBlocked      = sets:is_element(QPid, Blocking),
    ConfirmMonitored  = gb_trees:is_defined(QPid, UQM),
    StatsEnabled or ConsumerMonitored or QueueBlocked or ConfirmMonitored.

handle_publishing_queue_down(QPid, Reason, State = #ch{unconfirmed_qm = UQM}) ->
    MsgSeqNos = case gb_trees:lookup(QPid, UQM) of
                    {value, MsgSet} -> gb_sets:to_list(MsgSet);
                    none            -> []
                end,
    %% We remove the MsgSeqNos from UQM before calling
    %% process_confirms to prevent each MsgSeqNo being removed from
    %% the set one by one which which would be inefficient
    State1 = State#ch{unconfirmed_qm = gb_trees:delete_any(QPid, UQM)},
    {Nack, SendFun} =
        case Reason of
            Reason when Reason =:= noproc; Reason =:= noconnection;
                        Reason =:= normal; Reason =:= shutdown ->
                {false, fun record_confirms/2};
            {shutdown, _} ->
                {false, fun record_confirms/2};
            _ ->
                {true,  fun send_nacks/2}
        end,
    {MXs, State2} = process_confirms(MsgSeqNos, QPid, Nack, State1),
    SendFun(MXs, State2).

handle_consuming_queue_down(QPid,
                            State = #ch{consumer_mapping = ConsumerMapping,
                                        queue_consumers  = QCons,
                                        writer_pid       = WriterPid}) ->
    ConsumerTags = case dict:find(QPid, QCons) of
                       error       -> gb_sets:new();
                       {ok, CTags} -> CTags
                   end,
    ConsumerMapping1 =
        gb_sets:fold(fun (CTag, CMap) ->
                             Cancel = #'basic.cancel'{consumer_tag = CTag,
                                                      nowait       = true},
                             ok = rabbit_writer:send_command(WriterPid, Cancel),
                             dict:erase(CTag, CMap)
                     end, ConsumerMapping, ConsumerTags),
    State#ch{consumer_mapping = ConsumerMapping1,
             queue_consumers  = dict:erase(QPid, QCons)}.

binding_action(Fun, ExchangeNameBin, DestinationType, DestinationNameBin,
               RoutingKey, Arguments, ReturnMethod, NoWait,
               State = #ch{virtual_host = VHostPath,
                           conn_pid     = ConnPid }) ->
    %% FIXME: connection exception (!) on failure??
    %% (see rule named "failure" in spec-XML)
    %% FIXME: don't allow binding to internal exchanges -
    %% including the one named "" !
    {DestinationName, ActualRoutingKey} =
        expand_binding(DestinationType, DestinationNameBin, RoutingKey, State),
    check_write_permitted(DestinationName, State),
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    [check_not_default_exchange(N) || N <- [DestinationName, ExchangeName]],
    check_read_permitted(ExchangeName, State),
    case Fun(#binding{source      = ExchangeName,
                      destination = DestinationName,
                      key         = ActualRoutingKey,
                      args        = Arguments},
             fun (_X, Q = #amqqueue{}) ->
                     try rabbit_amqqueue:check_exclusive_access(Q, ConnPid)
                     catch exit:Reason -> {error, Reason}
                     end;
                 (_X, #exchange{}) ->
                     ok
             end) of
        {error, source_not_found} ->
            rabbit_misc:not_found(ExchangeName);
        {error, destination_not_found} ->
            rabbit_misc:not_found(DestinationName);
        {error, source_and_destination_not_found} ->
            rabbit_misc:protocol_error(
              not_found, "no ~s and no ~s", [rabbit_misc:rs(ExchangeName),
                                             rabbit_misc:rs(DestinationName)]);
        {error, binding_not_found} ->
            rabbit_misc:protocol_error(
              not_found, "no binding ~s between ~s and ~s",
              [RoutingKey, rabbit_misc:rs(ExchangeName),
               rabbit_misc:rs(DestinationName)]);
        {error, #amqp_error{} = Error} ->
            rabbit_misc:protocol_error(Error);
        ok -> return_ok(State, NoWait, ReturnMethod)
    end.

basic_return(#basic_message{exchange_name = ExchangeName,
                            routing_keys  = [RoutingKey | _CcRoutes],
                            content       = Content},
             #ch{protocol = Protocol, writer_pid = WriterPid}, Reason) ->
    {_Close, ReplyCode, ReplyText} = Protocol:lookup_amqp_exception(Reason),
    ok = rabbit_writer:send_command(
           WriterPid,
           #'basic.return'{reply_code  = ReplyCode,
                           reply_text  = ReplyText,
                           exchange    = ExchangeName#resource.name,
                           routing_key = RoutingKey},
           Content).

reject(DeliveryTag, Requeue, Multiple, State = #ch{unacked_message_q = UAMQ}) ->
    {Acked, Remaining} = collect_acks(UAMQ, DeliveryTag, Multiple),
    ok = fold_per_queue(
           fun (QPid, MsgIds, ok) ->
                   rabbit_amqqueue:reject(QPid, MsgIds, Requeue, self())
           end, ok, Acked),
    ok = notify_limiter(State#ch.limiter, Acked),
    {noreply, State#ch{unacked_message_q = Remaining}}.

ack_record(DeliveryTag, ConsumerTag,
           _MsgStruct = {_QName, QPid, MsgId, _Redelivered, _Msg}) ->
    {DeliveryTag, ConsumerTag, {QPid, MsgId}}.

collect_acks(Q, 0, true) ->
    {queue:to_list(Q), queue:new()};
collect_acks(Q, DeliveryTag, Multiple) ->
    collect_acks([], queue:new(), Q, DeliveryTag, Multiple).

collect_acks(ToAcc, PrefixAcc, Q, DeliveryTag, Multiple) ->
    case queue:out(Q) of
        {{value, UnackedMsg = {CurrentDeliveryTag, _ConsumerTag, _Msg}},
         QTail} ->
            if CurrentDeliveryTag == DeliveryTag ->
                    {[UnackedMsg | ToAcc], queue:join(PrefixAcc, QTail)};
               Multiple ->
                    collect_acks([UnackedMsg | ToAcc], PrefixAcc,
                                 QTail, DeliveryTag, Multiple);
               true ->
                    collect_acks(ToAcc, queue:in(UnackedMsg, PrefixAcc),
                                 QTail, DeliveryTag, Multiple)
            end;
        {empty, _} ->
            rabbit_misc:protocol_error(
              precondition_failed, "unknown delivery tag ~w", [DeliveryTag])
    end.

ack(Acked, State) ->
    QIncs = fold_per_queue(
              fun (QPid, MsgIds, L) ->
                      ok = rabbit_amqqueue:ack(QPid, MsgIds, self()),
                      [{QPid, length(MsgIds)} | L]
              end, [], Acked),
    ok = notify_limiter(State#ch.limiter, Acked),
    maybe_incr_stats(QIncs, ack, State).

new_tx(State) -> State#ch{uncommitted_message_q = queue:new(),
                          uncommitted_acks      = []}.

notify_queues(State = #ch{state = closing}) ->
    {ok, State};
notify_queues(State = #ch{consumer_mapping = Consumers}) ->
    {rabbit_amqqueue:notify_down_all(consumer_queues(Consumers), self()),
     State#ch{state = closing}}.

fold_per_queue(_F, Acc, []) ->
    Acc;
fold_per_queue(F, Acc, [{_DTag, _CTag, {QPid, MsgId}}]) -> %% common case
    F(QPid, [MsgId], Acc);
fold_per_queue(F, Acc, UAL) ->
    T = lists:foldl(fun ({_DTag, _CTag, {QPid, MsgId}}, T) ->
                            rabbit_misc:gb_trees_cons(QPid, MsgId, T)
                    end, gb_trees:empty(), UAL),
    rabbit_misc:gb_trees_fold(F, Acc, T).

enable_limiter(State = #ch{unacked_message_q = UAMQ,
                           limiter           = Limiter}) ->
    Limiter1 = rabbit_limiter:enable(Limiter, queue:len(UAMQ)),
    ok = limit_queues(Limiter1, State),
    Limiter1.

limit_queues(Limiter, #ch{consumer_mapping = Consumers}) ->
    rabbit_amqqueue:limit_all(consumer_queues(Consumers), self(), Limiter).

consumer_queues(Consumers) ->
    lists:usort([QPid ||
                    {_Key, #amqqueue{pid = QPid}} <- dict:to_list(Consumers)]).

%% tell the limiter about the number of acks that have been received
%% for messages delivered to subscribed consumers, but not acks for
%% messages sent in a response to a basic.get (identified by their
%% 'none' consumer tag)
notify_limiter(Limiter, Acked) ->
    case rabbit_limiter:is_enabled(Limiter) of
        false -> ok;
        true  -> case lists:foldl(fun ({_, none, _}, Acc) -> Acc;
                                      ({_, _, _}, Acc)    -> Acc + 1
                                  end, 0, Acked) of
                     0     -> ok;
                     Count -> rabbit_limiter:ack(Limiter, Count)
                 end
    end.

deliver_to_queues({Delivery = #delivery{message    = Message = #basic_message{
                                                       exchange_name = XName},
                                        msg_seq_no = MsgSeqNo},
                   QNames}, State) ->
    {RoutingRes, DeliveredQPids} = rabbit_router:deliver(QNames, Delivery),
    State1 = process_routing_result(RoutingRes, DeliveredQPids,
                                    XName, MsgSeqNo, Message, State),
    maybe_incr_stats([{XName, 1} |
                      [{{QPid, XName}, 1} ||
                          QPid <- DeliveredQPids]], publish, State1).

process_routing_result(unroutable,    _, XName,  MsgSeqNo, Msg, State) ->
    ok = basic_return(Msg, State, no_route),
    record_confirm(MsgSeqNo, XName,
                   maybe_incr_stats([{Msg#basic_message.exchange_name, 1}],
                                    return_unroutable, State));
process_routing_result(not_delivered, _, XName,  MsgSeqNo, Msg, State) ->
    ok = basic_return(Msg, State, no_consumers),
    record_confirm(MsgSeqNo, XName,
                   maybe_incr_stats([{XName, 1}], return_not_delivered, State));
process_routing_result(routed,       [], XName,  MsgSeqNo,   _, State) ->
    record_confirm(MsgSeqNo, XName, State);
process_routing_result(routed,        _,     _, undefined,   _, State) ->
    State;
process_routing_result(routed,    QPids, XName,  MsgSeqNo,   _, State) ->
    #ch{unconfirmed_mq = UMQ} = State,
    UMQ1 = gb_trees:insert(MsgSeqNo, {XName, gb_sets:from_list(QPids)}, UMQ),
    SingletonSet = gb_sets:singleton(MsgSeqNo),
    lists:foldl(
      fun (QPid, State0 = #ch{unconfirmed_qm = UQM}) ->
              case gb_trees:lookup(QPid, UQM) of
                  {value, MsgSeqNos} ->
                      MsgSeqNos1 = gb_sets:insert(MsgSeqNo, MsgSeqNos),
                      UQM1 = gb_trees:update(QPid, MsgSeqNos1, UQM),
                      State0#ch{unconfirmed_qm = UQM1};
                  none ->
                      UQM1 = gb_trees:insert(QPid, SingletonSet, UQM),
                      monitor_queue(QPid, State0#ch{unconfirmed_qm = UQM1})
              end
      end, State#ch{unconfirmed_mq = UMQ1}, QPids).

lock_message(true, MsgStruct, State = #ch{unacked_message_q = UAMQ}) ->
    State#ch{unacked_message_q = queue:in(MsgStruct, UAMQ)};
lock_message(false, _MsgStruct, State) ->
    State.

send_nacks([], State) ->
    State;
send_nacks(MXs, State = #ch{tx_status = none}) ->
    MsgSeqNos = [ MsgSeqNo || {MsgSeqNo, _} <- MXs ],
    coalesce_and_send(MsgSeqNos,
                      fun(MsgSeqNo, Multiple) ->
                              #'basic.nack'{delivery_tag = MsgSeqNo,
                                            multiple = Multiple}
                      end, State);
send_nacks(_, State) ->
    maybe_complete_tx(State#ch{tx_status = failed}).

send_confirms(State = #ch{tx_status = none, confirmed = []}) ->
    State;
send_confirms(State = #ch{tx_status = none, confirmed = C}) ->
    {MsgSeqNos, State1} =
        lists:foldl(fun ({MsgSeqNo, ExchangeName}, {MSNs, State0}) ->
                            {[MsgSeqNo | MSNs],
                             maybe_incr_stats([{ExchangeName, 1}], confirm,
                                              State0)}
                    end, {[], State}, lists:append(C)),
    send_confirms(MsgSeqNos, State1 #ch{confirmed = []});
send_confirms(State) ->
    maybe_complete_tx(State).

send_confirms([], State) ->
    State;
send_confirms([MsgSeqNo], State = #ch{writer_pid = WriterPid}) ->
    ok = rabbit_writer:send_command(WriterPid,
                                    #'basic.ack'{delivery_tag = MsgSeqNo}),
    State;
send_confirms(Cs, State) ->
    coalesce_and_send(Cs, fun(MsgSeqNo, Multiple) ->
                                  #'basic.ack'{delivery_tag = MsgSeqNo,
                                               multiple     = Multiple}
                          end, State).

coalesce_and_send(MsgSeqNos, MkMsgFun,
                  State = #ch{writer_pid = WriterPid, unconfirmed_mq = UMQ}) ->
    SMsgSeqNos = lists:usort(MsgSeqNos),
    CutOff = case gb_trees:is_empty(UMQ) of
                 true  -> lists:last(SMsgSeqNos) + 1;
                 false -> {SeqNo, _XQ} = gb_trees:smallest(UMQ), SeqNo
             end,
    {Ms, Ss} = lists:splitwith(fun(X) -> X < CutOff end, SMsgSeqNos),
    case Ms of
        [] -> ok;
        _  -> ok = rabbit_writer:send_command(
                     WriterPid, MkMsgFun(lists:last(Ms), true))
    end,
    [ok = rabbit_writer:send_command(
            WriterPid, MkMsgFun(SeqNo, false)) || SeqNo <- Ss],
    State.

maybe_complete_tx(State = #ch{tx_status = in_progress}) ->
    State;
maybe_complete_tx(State = #ch{unconfirmed_mq = UMQ}) ->
    case gb_trees:is_empty(UMQ) of
        false -> State;
        true  -> complete_tx(State#ch{confirmed = []})
    end.

complete_tx(State = #ch{tx_status = committing}) ->
    ok = rabbit_writer:send_command(State#ch.writer_pid, #'tx.commit_ok'{}),
    State#ch{tx_status = in_progress};
complete_tx(State = #ch{tx_status = failed}) ->
    {noreply, State1} = send_exception(
                          rabbit_misc:amqp_error(
                            precondition_failed, "partial tx completion", [],
                            'tx.commit'),
                          State),
    State1#ch{tx_status = in_progress}.

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

i(pid,            _)                               -> self();
i(connection,     #ch{conn_pid         = ConnPid}) -> ConnPid;
i(number,         #ch{channel          = Channel}) -> Channel;
i(user,           #ch{user             = User})    -> User#user.username;
i(vhost,          #ch{virtual_host     = VHost})   -> VHost;
i(transactional,  #ch{tx_status        = TE})      -> TE =/= none;
i(confirm,        #ch{confirm_enabled  = CE})      -> CE;
i(consumer_count, #ch{consumer_mapping = ConsumerMapping}) ->
    dict:size(ConsumerMapping);
i(messages_unconfirmed, #ch{unconfirmed_mq = UMQ}) ->
    gb_trees:size(UMQ);
i(messages_unacknowledged, #ch{unacked_message_q = UAMQ}) ->
    queue:len(UAMQ);
i(messages_uncommitted, #ch{uncommitted_message_q = TMQ}) ->
    queue:len(TMQ);
i(acks_uncommitted, #ch{uncommitted_acks = TAL}) ->
    length(TAL);
i(prefetch_count, #ch{limiter = Limiter}) ->
    rabbit_limiter:get_limit(Limiter);
i(client_flow_blocked, #ch{limiter = Limiter}) ->
    rabbit_limiter:is_blocked(Limiter);
i(Item, _) ->
    throw({bad_argument, Item}).

maybe_incr_redeliver_stats(true, QPid, State) ->
    maybe_incr_stats([{QPid, 1}], redeliver, State);
maybe_incr_redeliver_stats(_, _, State) ->
    State.

maybe_incr_stats(QXIncs, Measure, State = #ch{stats_timer = StatsTimer}) ->
    case rabbit_event:stats_level(StatsTimer) of
        fine -> lists:foldl(fun ({QX, Inc}, State0) ->
                                    incr_stats(QX, Inc, Measure, State0)
                            end, State, QXIncs);
        _    -> State
    end.

incr_stats({QPid, _} = QX, Inc, Measure, State) ->
    update_measures(queue_exchange_stats, QX, Inc, Measure),
    monitor_queue(QPid, State);
incr_stats(QPid, Inc, Measure, State) when is_pid(QPid) ->
    update_measures(queue_stats, QPid, Inc, Measure),
    monitor_queue(QPid, State);
incr_stats(X, Inc, Measure, State) ->
    update_measures(exchange_stats, X, Inc, Measure),
    State.

update_measures(Type, QX, Inc, Measure) ->
    Measures = case get({Type, QX}) of
                   undefined -> [];
                   D         -> D
               end,
    Cur = case orddict:find(Measure, Measures) of
              error   -> 0;
              {ok, C} -> C
          end,
    put({Type, QX},
        orddict:store(Measure, Cur + Inc, Measures)).

emit_stats(State) ->
    emit_stats(State, []).

emit_stats(State = #ch{stats_timer = StatsTimer}, Extra) ->
    CoarseStats = infos(?STATISTICS_KEYS, State),
    case rabbit_event:stats_level(StatsTimer) of
        coarse ->
            rabbit_event:notify(channel_stats, Extra ++ CoarseStats);
        fine ->
            FineStats =
                [{channel_queue_stats,
                  [{QPid, Stats} || {{queue_stats, QPid}, Stats} <- get()]},
                 {channel_exchange_stats,
                  [{X, Stats} || {{exchange_stats, X}, Stats} <- get()]},
                 {channel_queue_exchange_stats,
                  [{QX, Stats} ||
                      {{queue_exchange_stats, QX}, Stats} <- get()]}],
            rabbit_event:notify(channel_stats,
                                Extra ++ CoarseStats ++ FineStats)
    end.

erase_queue_stats(QPid) ->
    erase({queue_stats, QPid}),
    [erase({queue_exchange_stats, QX}) ||
        {{queue_exchange_stats, QX = {QPid0, _}}, _} <- get(), QPid =:= QPid0].
