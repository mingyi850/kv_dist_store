defmodule KvStore.LogEntry do 

  defstruct(
    type: nil, 
    is_stale: false,
    kvnode: nil,
    client: nil,
    kvnode_latency: 0,
    client_latency: 0
  )

  @spec new(%{}) :: %KvStore.LogEntry{}
  def new(log) do
    %KvStore.LogEntry{
      type: Map.get(log, :type, nil),
      is_stale: Map.get(log, :is_stale, false),
      kvnode: Map.get(log, :kvnode, nil),
      client: Map.get(log, :client, nil),
      kvnode_latency: Map.get(log, :kvnode_latency, 0),
      client_latency: Map.get(log, :client_latency, 0)
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
    map a key to a list that contain all put history. Each entries contain %{id, value}. 
    The entries with larger id is in the front of the list. The end of the list should contain entry %{id: 0, value: nil}.
  `log`
    map request id to %KvStore.LogEntry{}
  '''

  defstruct(
    data: %{},
    log: %{}
  )

  @spec init([atom()]) :: %KvStore.Observer{}
  def init(node) do
    Logger.info("Initializing Observer with node: #{inspect(node)}")
    %KvStore.Observer{
      data: %{},
      log: %{}
    }
  end

  @spec run(%KvStore.Observer{}) :: %KvStore.Observer{}
  def run(state) do
    Logger.info("Starting Observer with state #{inspect(state)}")
    receive do
      {_, %KvStore.GetRequestLog{} = request} -> 
        state = log_get(state, request)
        run(state)
      {_, %KvStore.PutRequestLog{} = request} -> 
        run(state)
      {_, %KvStore.ClientRequestLog{} = request} -> 
        run(state)
      unknown ->
        Logger.error("Observer Unknown message received: #{inspect(unknown)}")
        run(state)
    end
  end

  @spec log_get(%KvStore.Observer{}, %KvStore.GetRequestLog{}) :: %KvStore.Observer{}
  def log_get(state, request) do 
    if Map.has_key?(state.log, request.req_id) do 
      log_entry = %KvStore.LogEntry{state.log[request.req_id] | 
        is_stale: check_staleness(Map.get(state.data, request.key, [%{id: 0, value: nil}]), request.req_id, request.object),
        kvnode: request.sender,
        # kvnode_latency: request.resp_ts - request.recv_ts
      }
      %{state | log: Map.put(state.log, request.req_id, log_entry)}
    else 
      log_entry = KvStore.LogEntry.new(%{
        type: :get,
        is_stale: check_staleness(Map.get(state.data, request.key, [%{id: 0, value: nil}]), request.req_id, request.object),
        kvnode: request.sender,
        # kvnode_latency: request.resp_ts - request.recv_ts
      })
      %{state | log: Map.put(state.log, request.req_id, log_entry)}
    end
  end

  defp check_staleness(data, req_id, value) do
    [head | tail] = data
    if req_id < head.id do 
      check_staleness(tail, req_id, value)
    else 
      value == head.value
    end
  end

  @spec log_put(%KvStore.Observer{}, %KvStore.PutRequestLog{}) :: %KvStore.Observer{}
  def log_put(state, request) do 
    state = %{state | data: Map.put(state.data, request.key, [%{id: request.req_id, value: request.object} | Map.get(state.data, request.key, %{id: 0, value: nil})])}
    if Map.has_key?(state.log, request.req_id) do 
      log_entry = %KvStore.LogEntry{state.log[request.req_id] | 
        kvnode: request.sender,
        # kvnode_latency: request.resp_ts - request.recv_ts
      }
      %{state | log: Map.put(state.log, request.req_id, log_entry)}
    else 
      log_entry = KvStore.LogEntry.new(%{
        type: :put,
        kvnode: request.sender,
        # kvnode_latency: request.resp_ts - request.recv_ts
      })
      %{state | log: Map.put(state.log, request.req_id, log_entry)}
    end
  end

  @spec log_client(%KvStore.Observer{}, %KvStore.ClientRequestLog{}) :: %KvStore.Observer{}
  def log_client(state, request) do 
    if Map.has_key?(state.log, request.req_id) do 
      log_entry = %KvStore.LogEntry{state.log[request.req_id] | 
        client: request.sender,
        client_latency: request.recv_ts - request.send_ts
      }
      %{state | log: Map.put(state.log, request.req_id, log_entry)}
    else 
      log_entry = KvStore.LogEntry.new(%{
        client: request.sender,
        client_latency: request.recv_ts - request.send_ts
      })
      %{state | log: Map.put(state.log, request.req_id, log_entry)}
    end
  end

end