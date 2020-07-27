%%%-------------------------------------------------------------------
%% @doc dss public API
%% @end
%%%-------------------------------------------------------------------

-module(dss_app).

-behaviour(application).

-export([start/2, stop/1]).
-include("records.hrl").

start(StartType, ProxyNode) ->
    case StartType of
        proxy ->
            %Create schema and start mnesia
            mnesia:create_schema([node()]),
            mnesia:start(),
            %Init databases
            database_logic:initDB(),
            %spawn proxy node with supervisor
            dss_proxy_sup:start_link();
        storage ->
            %Starts mnesia and connect to proxy node
            mnesia:start(),
            net_adm:ping(ProxyNode),
            global:sync(),
            %spawn storage node with supervisor
            dss_storage_sup:start_link(),
            %calculate number of vnodes according to resources (to do)
            VNodes = calculate_VNodes(),
            %adds storage node to hash ring
            proxy_genserver_calls:add_node(node(), whereis(?StorageNode), VNodes)
    end.


stop(_State) ->
    ok.

%% internal functions

%Used to calculate vNodes according to the resources
calculate_VNodes() -> 5.
