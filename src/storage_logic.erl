%%%-------------------------------------------------------------------
%%% @author adircohen
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Description:
%%%     Storage interface for storage nodes.
%%% Created : 09. Jul 2020 21:28
%%%-------------------------------------------------------------------
-module(storage_logic).
-author("adircohen").

-export([upload_file/1, download_file/1, delete_file/1, upload_chunk/2, upload_chunks/2]).

-include("records.hrl").


% 1. check if the file already exists
% 3. read the file
% 4. split file to chunks
% 5. hash each chunk and create list of 3 positions per chunk
% 6. upload the chunks to the storage nodes
% 7. update mnesia DB with valid 1

upload_file(FileName) ->
  % 1. check if the file already exists
  Result = database_logic:global_is_exists(FileName),
  if
    Result == exists ->
      io:format("file= ~p already exists, please use update_file instead ~n",[FileName]),
      file_already_exists;
    true ->
      io:format("uploading file= ~p .... ~n",[FileName]),
      % 3. read the file
      File = files_logic:read_file(FileName,?LocalDB_folder),
      % 4. split file to chunks
      Chunks = files_logic:split_to_chunks(File, ?CHUNK_SIZE, []),
      % 5. hash each chunk and create list of 3 positions per chunk
      ChunksNum = length(Chunks),
      % positions looks like: [ {PartName, [Pos1, Pos2]}, {PartName, [Pos1, Pos2]}... ]
      Positions = proxy_genserver_calls:get_positions(FileName,ChunksNum),
      % 6. upload the chunks to the storage nodes
      upload_chunks_serial(Positions, Chunks),
      % 7. update mnesia DB with valid 1
      database_logic:global_insert_file(FileName,Positions),
      io:format("Finish upload file= ~p ~n",[FileName])
  end.

% 1. check if the file exists in mnesia DB
% 2. collect a list of parts locations
% 3. download the chunks from the storage nodes
% 4. combine_chunks and save to disk
% 5. delete all temporary chunks saved to memory in step #3.


download_file(FileName) ->
  % 1. check if the file exists in mnesia DB
  Result = database_logic:global_is_exists(FileName),
  if
    Result == exists ->
      io:format("downloading file= ~p .... ~n",[FileName]),
      % 2. collect a list of parts locations
      File = database_logic:global_find_file(FileName),
      Positions = File#?GlobalDB.location,
      % 3. download the chunks from the storage nodes
      download_chunks(Positions),
      % 4. combine_chunks and save to disk
      ChunksNum = length(Positions),
      files_logic:combine_chunks(FileName, ChunksNum),
      % 5. delete all temporary chunks saved to memory in step #3.
      delete_local_chunks(Positions);
    true ->
      io:format("file= ~p does not exists in global DB ~n",[FileName])
  end.

%%% -----------------
%%% delete file logic
%%% -----------------
% 1. check if the file already exists
% 2. update mnesia DB with valid 0
% 3. get from mnesia DB the locations of each chunk
% 4. create tuple of {storage_node, [chunks to upload]}
% 5. delete the chunks from the storage nodes
% 6. update mnesia DB

% 1. check if the file exists in mnesia DB
% 2. change valid to 0
% 3. collect a list of parts locations
% 4. delete the chunks from the storage nodes
delete_file(FileName) ->
  io:format("delete file= ~p .... ~n",[FileName]),
  % 1. check if the file exists in mnesia DB
  Result = database_logic:global_is_exists(FileName),
  if
    Result == exists ->
      % 2. change valid to 0
      database_logic:global_update_valid(FileName, 0),
      % 3. collect a list of parts locations
      {_,[Entry]} = database_logic:global_find_file(FileName),
      Positions = Entry#?GlobalDB.location,
      % 4. delete the chunks from the storage nodes
      delete_chunks_serial(Positions),
      io:format("deleting file= ~p from DB has been completed.... ~n",[FileName]),
      database_logic:global_delete_file(FileName);
    true ->
      io:format("file= ~p does not exists in global DB ~n",[FileName])
  end.

%%% --------------------------- %%%
%%%  Upload Internal Functions  %%%
%%% --------------------------- %%%

%Serial implementation
upload_chunks_serial([],[]) ->
  io:format("finish upload chunks ~n");

upload_chunks_serial([PartNameAndLocations|T],[Chunk|Rest]) ->
  upload_chunk(PartNameAndLocations,Chunk),
  upload_chunks_serial(T,Rest).


%Parallel implementation
% Handle each the full list of chunks and positions
upload_chunks([],[]) ->
  io:format("finish upload chunks ~n");

upload_chunks([PartNameAndLocations|T],[Chunk|Rest]) ->
  spawn(?MODULE, upload_chunk,[PartNameAndLocations,Chunk]),
  upload_chunks(T,Rest).

% Handle one part of a file

upload_chunk({FileName, []}, _) ->
  io:format("finish upload chunk= ~p ~n",[FileName]),
  ok;

upload_chunk({FileName, [Pos|T]}, Chunk) ->
  RetVal = storage_genserver_calls:upload_file({FileName, Chunk}, Pos),
  case RetVal of
    ok    ->
      upload_chunk({FileName, T}, Chunk);
    _Else ->
      io:format("Upload ~p failed~n",[FileName]),
      upload_chunk({FileName, T}, Chunk)
  end.


%%% --------------------------- %%%
%%% Download Internal Functions %%%
%%% --------------------------- %%%

% Handle each part from the full list of chunks and positions
download_chunks([]) ->
  io:format("finish download chunks ~n");

download_chunks([PartNameAndLocations|T]) ->
  spawn(?MODULE, download_chunk,[PartNameAndLocations]),
  download_chunks(T).

% Handle one part of a file
download_chunk({PartName, []}) ->
  io:format("ERROR - There is no more servers to download from the part ~p ~n",[PartName]);

% Steps:
% 1. Downaload the Chunk
% 2. If ok, store in memory, Else - try next storage for this part
download_chunk({PartName, [Pos|T]}) ->
  RetVal = storage_genserver_calls:download_file(PartName, Pos),
  case RetVal of
    ok    ->
      {_, Chunk} = RetVal,
      files_logic:save_to_disk(PartName, [Chunk], ?LocalDB_folder),
      io:format("Finish saving part ~p into memory ~n",[PartName]);
    _Else ->
      io:format("Download ~p failed, trying to download from next server ~n",[PartName]),
      download_chunk({PartName, T})
  end.

%%% --------------------------- %%%
%%%  Delete Internal Functions  %%%
%%% --------------------------- %%%

% Delete all parts of a file from memory
delete_local_chunks([]) ->
  io:format("Finish deleting all parts ~n");

delete_local_chunks([{PartName, _}|T]) ->
  spawn(files_logic, delete_file,[PartName, ?LocalDB_folder]),
  delete_local_chunks(T).


delete_chunks_serial([]) ->
  io:format("Finish deleting all parts ~n");

delete_chunks_serial([PartNameAndLocations|T]) ->
  delete_chunk(PartNameAndLocations),
  delete_chunks_serial(T).

%parallel
% Delete all parts of a file from memory
delete_chunks([]) ->
  io:format("Finish deleting all parts ~n");

delete_chunks([PartNameAndLocations|T]) ->
  spawn(?MODULE, delete_chunk,[PartNameAndLocations]),
  delete_chunks(T).

% Handle one part of a file
delete_chunk({PartName, []}) ->
  io:format("Finish deleting part ~p from global DB ~n",[PartName]);

delete_chunk({PartName, [Pos|T]}) ->
  io:format("Trying to delete part= ~p from server= ~p ~n",[PartName, Pos]),
  RetVal = storage_genserver_calls:delete_file(PartName, Pos),
  case RetVal of
    ok    ->
      delete_chunk({PartName, T});
    _Else ->
      io:format("Deleting part= ~p has failed, trying to download from next server ~n",[PartName]),
      delete_chunk({PartName, T})
  end.
