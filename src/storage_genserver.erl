%%%-------------------------------------------------------------------
%%% @author asorg
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(storage_genserver).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-include("records.hrl").
-record(state, {requests}).




%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?StorageNode}, ?MODULE, [], []).

init([]) ->
  {ok, #state{requests = 0}}.

%@doc
%% Input - FileName , String
%% Output - {FileName,Binary}
%% Output error - {FileName, notFound}
handle_call({download_file, FileName}, _From, #state{requests = X}) ->
  %Perform storage lookup
  RetVal = files_logic:read_file(FileName, ?LocalDB_folder),
  {reply, {FileName, RetVal}, #state{requests = X+1}};

%@doc
%% Input - {FileName, Binary}
%% Output - ok
%% Output error - {error,Reason}
handle_call({upload_file, {FileName, Bin}}, _From, #state{requests = X}) ->
  %Write file to disk
  RetVal = files_logic:save_to_disk(FileName, [Bin], ?LocalDB_folder),
  %Replay to caller
  case RetVal of
    ok ->
      {reply, ok, #state{requests = X+1}};
    {error,Reason} ->
      {reply, {error,Reason}, #state{requests = X+1}}
  end;


%@doc
%% Input - FileName , String
%% Output - ok
%% Output error - {error, Reason}
handle_call({delete_file, FileName}, _From, #state{requests = X}) ->
  %Delete file from disk
  RetVal = files_logic:delete_file(FileName, ?LocalDB_folder),
  %Replay to caller
  case RetVal of
    ok ->
      {reply, ok, #state{requests = X+1}};
    {error,Reason} ->
      {reply, {error,Reason}, #state{requests = X+1}}
  end.

handle_cast(_Request, State ) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
