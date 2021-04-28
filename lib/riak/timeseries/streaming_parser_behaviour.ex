defmodule Riak.Timeseries.StreamingParserBehaviour do
  @callback begin_request(handle :: pid | atom | tuple | any, stream :: {table :: String.t, req_id :: any}) :: pid | atom | tuple
  @callback process_request(handle :: pid | atom | tuple, stream :: {table :: String.t, req_id :: any}, data :: list) :: :contine | {:halt, any}
  @callback end_request(handle :: pid | atom | tuple, stream :: {table :: String.t, req_id :: any}) :: any
  @callback halt_request(handle :: pid | atom | tuple, stream :: {table :: String.t, req_id :: any}, error :: tuple) :: any
end