defmodule KvStore.LogEntry do

  defstruct(
    type: nil,
    is_stale: false,
    kvnode: nil,
    client: nil,
    latency: 0,
    key: nil,
    value: nil,
    timestamp: 0,
    start_ts: 0
  )

  @spec new(%{}) :: %KvStore.LogEntry{}
  def new(log) do
    %KvStore.LogEntry{
      type: Map.get(log, :type, nil),
      is_stale: Map.get(log, :is_stale, false),
      kvnode: Map.get(log, :kvnode, nil),
      client: Map.get(log, :client, nil),
      latency: Map.get(log, :client_latency, 0),
      key: Map.get(log, :key, nil),
      value: Map.get(log, :value, nil),
      timestamp: :os.system_time(:millisecond),
      start_ts: Map.get(log, :start_ts, -1)
    }
  end

end

defmodule KvStore.Observer do

  require KvStore.Utils
  require KvStore.PutRequestLog
  require KvStore.GetRequestLog
  require KvStore.ClientRequestLog
  require KvStore.LogEntry
  import KvStore.Utils
  import Emulation
  import Kernel, except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Logger

  '''
  `data`
    map a key to a list that contain all put history. Each entries contain %{ts, value}.
    The entries with larger ts is in the front of the list. The end of the list should contain entry %{ts: 0, value: nil}.
  `log`
    map request id to %KvStore.LogEntry{}
  '''

  defstruct(
    data: %{},
    log: %{}
  )

  @spec init(atom()) :: %KvStore.Observer{}
  def init(node) do
    Logger.info("Initializing Observer with node: #{inspect(node)}")
    %KvStore.Observer{
      data: %{},
      log: %{}
    }
  end

  @spec run(%KvStore.Observer{}) :: %KvStore.Observer{}
  def run(state) do
    # Logger.info("Observer run with state #{inspect(state)}")
    receive do
      {_, %KvStore.GetRequestLog{} = request} ->
        Logger.info("Observer receive (get) #{inspect(request)}")
        state = log_get(state, request)
        # Logger.info("Observer data: #{inspect(state.data)}, log: #{inspect(state.log)}")
        run(state)
      {_, {:get_start_time, req_id, start_ts}} ->
        Logger.info("Observer receive start_timestamp of [#{req_id}] = #{start_ts}")
        state = log_get_start_time(state, req_id, start_ts)
        run(state)
      {_, %KvStore.PutRequestLog{} = request} ->
        Logger.info("Observer receive (put) #{inspect(request)}")
        state = log_put(state, request)
        # Logger.info("Observer data: #{inspect(state.data)}, log: #{inspect(state.log)}")
        run(state)
      {_, %KvStore.ClientRequestLog{} = request} ->
        Logger.info("Observer receive (client) #{inspect(request)}")
        state = log_client(state, request)
        # Logger.info("Observer data: #{inspect(state.data)}, log: #{inspect(state.log)}")
        run(state)
      {sender, :get_log} ->
        Logger.info("Observer send log to #{inspect(sender)}")
        send(sender, state.log)
        run(state)
      unknown ->
        Logger.error("Observer Unknown message received: #{inspect(unknown)}")
        run(state)
    end
  end

  @spec log_get_start_time(%KvStore.Observer{}, non_neg_integer(), non_neg_integer()) :: %KvStore.Observer{}
  def log_get_start_time(state, req_id, start_ts) do
    if Map.has_key?(state.log, req_id) do
      IO.puts("in log_get_start_time [#{req_id}], #{inspect(Map.get(state.data, state.log[req_id].key, [%{end_ts: 0, value: nil} | []]))}, log_entry = #{inspect(state.log[req_id].value)}")
      log_entry = %KvStore.LogEntry{state.log[req_id] |
        start_ts: start_ts,
        is_stale: if state.log[req_id].value != nil do
          check_staleness(Map.get(state.data, state.log[req_id].key, [%{end_ts: 0, value: nil} | []]), start_ts, state.log[req_id].value)
          else false end
      }
      %{state | log: Map.put(state.log, req_id, log_entry)}
    else
      log_entry = KvStore.LogEntry.new(%{
        type: :get,
        start_ts: start_ts
      })
      %{state | log: Map.put(state.log, req_id, log_entry)}
    end
  end

  @spec log_get(%KvStore.Observer{}, %KvStore.GetRequestLog{}) :: %KvStore.Observer{}
  def log_get(state, request) do
    if Map.has_key?(state.log, request.req_id) do
      IO.puts("in log_get [#{request.req_id}], #{inspect(Map.get(state.data, request.key, [%{end_ts: 0, value: nil} | []]))}, log_entry = #{inspect(state.log[request.req_id])}")
      log_entry = %KvStore.LogEntry{state.log[request.req_id] |
        type: :get,
        is_stale: if state.log[request.req_id].start_ts != -1 do
          check_staleness(Map.get(state.data, request.key, [%{end_ts: 0, start_ts: 0, value: nil} | []]), state.log[request.req_id].start_ts, request.object)
          else false end,
        kvnode: request.sender,
        key: request.key,
        value: request.object
      }
      %{state | log: Map.put(state.log, request.req_id, log_entry)}
    else
      log_entry = KvStore.LogEntry.new(%{
        type: :get,
        kvnode: request.sender,
        key: request.key,
        value: request.object
      })
      %{state | log: Map.put(state.log, request.req_id, log_entry)}
    end
  end

  defp find_first_before(data, timestamp, accum) do
    if data == [] do
      accum
    else
      [head | tail] = data
      if head.end_ts > 0 and head.end_ts <= timestamp do
        diff = timestamp - head.end_ts
        if (accum == nil || diff < (timestamp - accum.end_ts)) do
          find_first_before(tail, timestamp, head)
        else
          find_first_before(tail, timestamp, accum)
        end
      else
        find_first_before(tail, timestamp, accum)
      end
    end
  end

  defp find_open_requests(data, timestamp, accum, closed) do
    if data == [] do
      accum
    else
      [head | tail] = data
      if head.start_ts > 0 and !MapSet.member?(closed, head.req_id) do #Open
        find_open_requests(tail, timestamp, [head | accum], closed)
      else
        if head.end_ts > 0 and head.end_ts < timestamp do
          find_open_requests(tail, timestamp, accum, MapSet.put(closed, head.req_id))
        else
          find_open_requests(tail, timestamp, accum, closed)
        end
      end
    end
  end

  defp check_staleness(data, ts, objects) do
    #Fix - data not guaranteed to be in timestamp order. Need to find all. Use min by maybe
    IO.puts("Finding first before for #{ts} in #{inspect(data)}")
    first_put_end = find_first_before(data, ts, nil)
    IO.puts("Got first put end #{inspect(first_put_end)}")
    if first_put_end == nil do
      false
    else
      #other_viable = Enum.filter(data, fn entry -> entry.end_ts == 0 && entry.start_ts > first_put_end.end_ts && entry.start_ts < ts end)
      other_viable = find_open_requests(data, ts, [], MapSet.new())
      IO.puts("Viable responses #{inspect([first_put_end | other_viable])}")
      !(Enum.member?(Enum.map(objects, fn cache_entry -> cache_entry.object end), first_put_end.value) || Enum.any?(other_viable, fn viable -> Enum.member?(Enum.map(objects, fn cache_entry -> cache_entry.object end), viable.value) end))
    end
    #[head | tail] = data
    #IO.puts("check_staleness, data length = #{length(data)}, #{head.end_ts} <=> #{ts}")
    #if head.end_ts < ts do
    #  if head.value == nil do
    #    false
    #  else
    #    !Enum.member?(Enum.map(objects, fn cache_entry -> cache_entry.object end), head.value)
    #  end
    #else
    #  if head.start_ts < ts do
    #    if head.value == nil do
    #      false
    #    else
    #      if Enum.member?(Enum.map(objects, fn cache_entry -> cache_entry.object end), head.value) do
    #        false
    #      else
    #        check_staleness(tail, ts, objects)
    #      end
    #    end
    #  else
    #    check_staleness(tail, ts, objects)
    #  end
    #end
  end

  @spec log_put(%KvStore.Observer{}, %KvStore.PutRequestLog{}) :: %KvStore.Observer{}
  def log_put(state, request) do
    # state = %{state | data: Map.put(state.data, request.key, request.object)}
    state = %{state | data: Map.put(state.data, request.key, [%{req_id: request.req_id, start_ts: request.start_ts, end_ts: request.end_ts, value: request.object} | Map.get(state.data, request.key, [%{req_id: -1, end_ts: 0, start_ts: 0, value: nil} | []])])}
    if Map.has_key?(state.log, request.req_id) do
      log_entry = %KvStore.LogEntry{state.log[request.req_id] |
        type: :put,
        kvnode: request.sender,
        key: request.key,
        value: request.object,
        start_ts: request.start_ts
      }
      %{state | log: Map.put(state.log, request.req_id, log_entry)}
    else
      log_entry = KvStore.LogEntry.new(%{
        type: :put,
        kvnode: request.sender,
        key: request.key,
        value: request.object,
        start_ts: request.start_ts
      })
      %{state | log: Map.put(state.log, request.req_id, log_entry)}
    end
  end

  @spec log_client(%KvStore.Observer{}, %KvStore.ClientRequestLog{}) :: %KvStore.Observer{}
  def log_client(state, request) do
    if Map.has_key?(state.log, request.req_id) do
      log_entry = %KvStore.LogEntry{state.log[request.req_id] |
        client: request.sender,
        latency: request.recv_ts - request.send_ts
      }
      %{state | log: Map.put(state.log, request.req_id, log_entry)}
    else
      IO.puts("[#{request.req_id}] log_client comes before log_get/put")
      log_entry = KvStore.LogEntry.new(%{
        client: request.sender,
        latency: request.recv_ts - request.send_ts
      })
      %{state | log: Map.put(state.log, request.req_id, log_entry)}
    end
  end

end
