%%%-------------------------------------------------------------------
%%% @author adircohen
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. Jul 2020 0:09
%%%-------------------------------------------------------------------
-module(load_balancer_logic).

-author("adircohen").

-export([new_ring/2, ring_lookup/2, get_positions/2, get_positions/3, add_node/2, test_ring/0]).
-include("records.hrl").
-define(HASH, md5).


%%%===================================================================
%%% Consistent Hashing API
%%%===================================================================
%@doc - create new consistent hashing ring
%% Input - Nodes: List of Nodes
%% VNodes: List, number of VNodes created for each Node in "Nodes" respectively
%% Output
new_ring(Nodes, VNodes) ->
  %lists:zip create lists of  [{NodeName, NumOfVNodes}...]
  NodesHashes = [node_position(X) || X <- lists:zip(Nodes, VNodes)],
  %Build hash ring
  Ring = gb_trees:from_orddict(lists:keysort(1,lists:flatten(NodesHashes))),
  %save ring in local database
  put(?HashRing,Ring),
  ok.

%@doc - add new node to CH ring
%% Input - Nodes: List of Nodes
%% VNodes: List, number of VNodes created for each Node in "Nodes" respectively
%% Output
add_node(Node, VNodes) ->
  Positions = node_position({Node,VNodes}),
  NewRing = lists:foldl(fun({Pos, Node}, Ring) -> gb_trees:insert(Pos, Node, Ring) end, get(?HashRing), Positions),
  put(?HashRing,NewRing),
  ok.

%@doc - perform lookup on CH ring
%% Input - Key - hash of object
%% Input - N Closest Nodes to Key
%% Output - [Node ID1, Node ID2 ..., Node IDN]
ring_lookup(Key, N) ->
  Ring = get(?HashRing),
  Iter = gb_trees:iterator_from(Key, Ring),
  ring_lookup(Iter, Ring, N, []).

ring_lookup(_,_,0,Nodes) -> Nodes;
ring_lookup(Iter, Ring, N, Nodes) ->
  %Iterate over hash ring nodes, starting from "Iter"
  case gb_trees:next(Iter) of
    none ->
      %End of the ring, start iterate from the beginning of the ring
      {SmallestKey, _} = gb_trees:smallest(Ring),
      ring_lookup(gb_trees:iterator_from(SmallestKey, Ring), Ring, N, Nodes);
    {_,Node, IterRest} ->
      %Check if Node already exists in "Nodes" list.
      IsExists = lists:member(Node, Nodes),
      if
        IsExists == false ->
          %Node does not exists, append to "Nodes"
          ring_lookup(IterRest, Ring, N-1, [Node|Nodes]);
        true ->
          %Node exists, skip to next node
          ring_lookup(IterRest, Ring, N, Nodes)
      end
  end.


%@doc - returns positions of given file on the ring
%% Input - FileName
%% Input - Ring - gb_tree
%% Input - N - Number of replicas
%% Output - [{PartName0,[Pos1,Pos2..],{PartName1, [Pos1,Pos2..]}...],
get_positions(FileName, PartsNum, N) ->
  PartNames =  gen_part_names(FileName, PartsNum),
  [{PartName, ring_lookup(hash(PartName), N)} || PartName <- PartNames].

%@doc - same function but return position for one part.
get_positions(FileName, N) ->
  {FileName, ring_lookup(hash(FileName), N)}.

%%====================================================================
%% Internal functions
%%====================================================================

%@doc - returns nodes positions
node_position({Node, VNodes}) ->
  [{hash(Node, Indx), Node} ||  Indx <- lists:seq(1, VNodes)].

%@doc - hash functions
hash(X) ->
  XBin = term_to_binary(X),
  crypto:hash(?HASH, <<XBin/binary>>).
hash(X, Y) ->
  XBin = term_to_binary(X),
  YBin = term_to_binary(Y),
  crypto:hash(?HASH, <<XBin/binary, YBin/binary>>).

%@doc - generate part names
gen_part_names(FileName, PartsNum) ->
  gen_part_names(FileName, PartsNum, []).
gen_part_names(_,0,PartNames) ->
  PartNames;
gen_part_names(FileName, PartsNum, PartNames) ->
  PartName = FileName ++ "." ++ "part" ++ integer_to_list(PartsNum - 1),
  gen_part_names(FileName, PartsNum-1, [PartName|PartNames]).

test_ring() ->
  new_ring(["10.0.0.2","10.0.0.3","10.0.0.7","10.0.0.5", "10.10.10.10"],[2,3,4,3,4]).