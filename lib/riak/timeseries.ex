defmodule Riak.Timeseries do
  import Riak.Pool

  defpool query(pid, query_text) when is_pid(pid) and is_binary(query_text) do
    query_list = :erlang.binary_to_list(query_text)
    case :riakc_ts.query(pid, query_list) do
      {:ok, results} -> results;
      err -> err
    end
  end

  defpool put(pid, table, data) when is_pid(pid)  and is_list(data)do
    :riakc_ts.put(pid, table, data)
  end

  defpool get(pid, table, key) when is_pid(pid) and is_list(key) do
    get(pid, table, key, [])
  end

  defpool get(pid, table, key, options) when is_pid(pid) and is_list(key) do
    case :riakc_ts.get(pid, table, key, options) do
      {:ok, results} -> results;
      err -> err
    end
  end

  defpool delete(pid, table, key) when is_pid(pid) and is_list(key) do
    delete(pid, table, key, [])
  end

  defpool delete(pid, table, key, options) when is_pid(pid) and is_list(key) do
    :riakc_ts.delete(pid, table, key, options)
  end

  defpool list_process!(pid, table, lambda) when is_pid(pid) do
    list_process!(pid, table, lambda, [])
  end

  defpool list_process!(pid, table, lambda, options) when is_pid(pid) do
    case lambda do
      {m,f} ->
        {:ok, req_id} = :riakc_ts.stream_list_keys(pid, table, options)
        mfa_process_for_list(req_id, {m,f}, [])
      v when is_function(v, 2) ->
        {:ok, req_id} = :riakc_ts.stream_list_keys(pid, table, options)
        lambda_process_for_list(req_id, lambda, [])
    end
  end

  defpool stream_list_process!(pid, table, agent, ref) when is_pid(pid) and is_atom(agent) do
    stream_list_process!(pid, table, agent, ref, [])
  end

  defpool stream_list_process!(pid, table, agent, ref, options) when is_pid(pid) and is_atom(agent) do
    {:ok, req_id} = :riakc_ts.stream_list_keys(pid, table, options)
    # Allow new ref creation on the fly is caller wishes a process or handler to be generated per request or per table.
    ref = agent.begin_request(ref, {table, req_id})
    external_process_for_list({table, req_id}, agent, ref)
  end

  defpool list!(pid, table) when is_pid(pid) do
    list!(pid, table, [])
  end

  defpool list!(pid, table, options) when is_pid(pid) do
    {:ok, req_id} = :riakc_ts.stream_list_keys(pid, table, options)
    wait_for_list(req_id, [])
  end

  def key_stream(pid, table, options) when is_pid(pid) do
    # alias, return raw stream request identifier for external processing.
    # Do not wrap pid generation as caller will need to handle pid clean up once stream has completed processing.
    :riakc_ts.stream_list_keys(pid, table, options)
  end

  defp wait_for_list(req_id, acc) do
    receive do
      {^req_id, :done} -> {:ok, acc}
      {^req_id, {:error, reason}} -> {:error, reason}
      {^req_id, {_, res}} -> wait_for_list(req_id, [res|acc])
    end
  end

  defp lambda_process_for_list(req_id, lambda, acc) do
    receive do
      {^req_id, :done} -> lambda.(:done, acc)
      {^req_id, {:error, reason}} -> lambda.({:error, reason}, acc)
      {^req_id, {_, res}} -> lambda_process_for_list(req_id, lambda, lambda.(res, acc))
    end
  end

  defp mfa_process_for_list(req_id, {m,f}, acc) do
    receive do
      {^req_id, :done} -> apply(m, f, [:done, acc])
      {^req_id, {:error, reason}} -> apply(m, f, [{:error, reason}, acc])
      {^req_id, {_, res}} -> mfa_process_for_list(req_id, {m,f}, apply(m,f, [res, acc]))
    end
  end

  defp external_process_for_list(stream = {_table, req_id}, agent, ref) do
    receive do
      {^req_id, :done} -> agent.end_request(ref, stream)
      {^req_id, {:error, reason}} -> agent.halt_request(ref, stream, {:error, reason})
      {^req_id, {_, res}} ->
        case agent.process_request(ref, stream, res) do
          {:halt, v} -> v
          _ -> external_process_for_list(stream, agent, ref)
        end
    end
  end
end