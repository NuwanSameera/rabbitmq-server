%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_connections).

-export([init/2, to_json/2, content_types_provided/2,
         is_authorized/2, allowed_methods/2, delete_resource/2,
         augmented/2]).
-export([variances/2]).

-import(rabbit_misc, [pget/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

to_json(ReqData, Context) ->
    to_json(username_qs(ReqData), ReqData, Context).

to_json(undefined, ReqData, Context) ->
    % Note: no "username" query parameter
    try
        Connections = do_connections_query(ReqData, Context),
        rabbit_mgmt_util:reply_list_or_paginate(Connections, ReqData, Context)
    catch
        {error, invalid_range_parameters, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), ReqData, Context)
    end;
to_json(Username, ReqData, Context) ->
    UserConnections = list_user_connections(Username),
    Connections = rabbit_mgmt_util:filter_tracked_conn_list(UserConnections, ReqData, Context),
    rabbit_mgmt_util:reply_list_or_paginate(Connections, ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

delete_resource(ReqData, Context) ->
    delete_resource(username_qs(ReqData), ReqData, Context).

delete_resource(undefined, ReqData, Context) ->
    % Note: no "username" query parameter given, do not allow
    % closing all connections
    Reason = "DELETE requires a ?username=NAME query parameter",
    rabbit_mgmt_util:bad_request(Reason, ReqData, Context);
delete_resource(Username, ReqData, Context) ->
    ok = close_user_connections(Username, ReqData),
    {true, ReqData, Context}.

%%--------------------------------------------------------------------

username_qs(ReqData) ->
    rabbit_mgmt_util:qs_val(<<"username">>, ReqData).

augmented(ReqData, Context) ->
    rabbit_mgmt_util:filter_conn_ch_list(
      rabbit_mgmt_db:get_all_connections(
        rabbit_mgmt_util:range_ceil(ReqData)), ReqData, Context).

do_connections_query(ReqData, Context) ->
    case rabbit_mgmt_util:disable_stats(ReqData) of
        false ->
            augmented(ReqData, Context);
        true ->
            rabbit_mgmt_util:filter_tracked_conn_list(rabbit_connection_tracking:list(),
                                                      ReqData, Context)
    end.

close_user_connections(Username, ReqData) when is_binary(Username) ->
    close_connections(list_user_connections(Username), Username, ReqData).

close_connections([], _Username, _ReqData) ->
    ok;
close_connections([Conn | Rest], Username, ReqData) ->
    ok = close_connection(Conn, Username, ReqData),
    close_connections(Rest, Username, ReqData).

close_connection(#tracked_connection{pid = undefined}, _Username, _ReqData) ->
    ok;
close_connection(#tracked_connection{name = Name, pid = Pid, username = Username, type = Type}, Username, ReqData) when is_pid(Pid) ->
    Conn = [{name, Name}, {pid, Pid}, {user, Username}, {type, Type}],
    rabbit_mgmt_util:force_close_connection(ReqData, Conn, Pid);
close_connection(_, _Username, _ReqData) ->
    ok.

list_user_connections(Username) ->
    rabbit_connection_tracking:list_of_user(Username).
