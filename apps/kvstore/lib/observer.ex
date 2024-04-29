defmodule KvStore.LogEntry do

  defstruct(
    type: nil,
    is_stale: false,
    kvnode: nil,
    client: nil,
    latency: 0,
    key: nil,
    value: nil,
    timestamp: 0
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
      timestamp: :os.system_time(:millisecond)
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
    map key to value
  `log`
    map request id to %KvStore.LogEntry{}
  '''

  defstruct(
    data: %{},
    log: %{},
    get_count: 0,
    stale_count: 0
  )

  @spec init(atom()) :: %KvStore.Observer{}
  def init(node) do
    Logger.info("Initializing Observer with node: #{inspect(node)}")
    %KvStore.Observer{
      data: %{},
      log: %{},
      get_count: 0,
      stale_count: 0
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
      {sender, :get_stale_stat} ->
        Logger.info("Observer report stale stat to #{inspect(sender)}")
        send(sender, %{get_count: state.get_count, stale_count: state.stale_count})
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

  @spec log_get(%KvStore.Observer{}, %KvStore.GetRequestLog{}) :: %KvStore.Observer{}
  def log_get(state, request) do
    if Map.has_key?(state.log, request.req_id) do
      Logger.debug("in log_get #{inspect(request.object)}")
      cache_entry = Enum.at(request.object, 0)
      is_stale = check_staleness(Map.get(state.data, request.key, nil), cache_entry)
      if is_stale do
        Logger.warning("Stale data 1: #{inspect(Map.get(state.data, request.key, nil))}, cache: #{inspect(cache_entry)}")
      end
      log_entry = %KvStore.LogEntry{state.log[request.req_id] |
        type: :get,
        is_stale: is_stale,
        kvnode: request.sender,
        key: request.key,
        value: request.object
      }
      %{state |
        log: Map.put(state.log, request.req_id, log_entry),
        get_count: state.get_count + 1,
        stale_count: if is_stale do state.stale_count + 1 else state.stale_count end
        }
    else
      cache_entry = Enum.at(request.object, 0)
      is_stale = check_staleness(Map.get(state.data, request.key, nil), cache_entry)
      if is_stale do
        Logger.warning("Stale data 2: #{inspect(Map.get(state.data, request.key, nil))}, cache: #{inspect(cache_entry)}")
      end
      log_entry = KvStore.LogEntry.new(%{
        type: :get,
        is_stale: is_stale,
        kvnode: request.sender,
        key: request.key,
        value: request.object
      })
      %{state |
        log: Map.put(state.log, request.req_id, log_entry),
        get_count: state.get_count + 1,
        stale_count: if is_stale do state.stale_count + 1 else state.stale_count end
        }
    end
  end

  defp check_staleness(data, value) do
    if value == nil do
      data != nil
    else
      data != value.object
    end
  end

  @spec log_put(%KvStore.Observer{}, %KvStore.PutRequestLog{}) :: %KvStore.Observer{}
  def log_put(state, request) do
    state = %{state | data: Map.put(state.data, request.key, request.object)}
    if Map.has_key?(state.log, request.req_id) do
      log_entry = %KvStore.LogEntry{state.log[request.req_id] |
        type: :put,
        kvnode: request.sender,
        key: request.key,
        value: request.object
      }
      %{state | log: Map.put(state.log, request.req_id, log_entry)}
    else
      log_entry = KvStore.LogEntry.new(%{
        type: :put,
        kvnode: request.sender,
        key: request.key,
        value: request.object
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
      log_entry = KvStore.LogEntry.new(%{
        client: request.sender,
        latency: request.recv_ts - request.send_ts
      })
      %{state | log: Map.put(state.log, request.req_id, log_entry)}
    end
  end

end
