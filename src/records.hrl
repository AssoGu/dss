%%%-------------------------------------------------------------------
%%% @author asorg
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 11. Jul 2020 13:51
%%%-------------------------------------------------------------------
-author("asorg").

-define(SERVER, ?MODULE).
-define(LocalDB_folder, "localDB/").
-define(LocalDB, localDB).
-define(GlobalDB, globalDB).
-define(TopologyDB, topologyDB).
-define(HashRing,ring).
-define(CHUNK_SIZE, 65536). %64KB chunks
-define(LoadBalancer, loadbalancer).
%% databases records
-record(?GlobalDB, {filename, creation_date, location, valid}).
-record(?TopologyDB, {ip, capacity, vNodes_count, vNodes_keys}).