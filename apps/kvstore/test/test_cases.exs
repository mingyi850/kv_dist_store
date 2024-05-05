defmodule TestCase do
  use ExUnit.Case
  import Emulation, only: [spawn: 2, send: 2, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2, spawn: 2]

  require KvStore.TestClient
  require KvStore.LogEntry
  import KvStore.TestClient
  import KvStore.LoadBalancer
  import KvStore.Observer

  require Logger

  '''
  Get latency stats
  '''
  @spec latency_stat(%KvStore.LogEntry{}) :: nil
  def latency_stat(logs) do
    # Logger.info("LATENCY STAT")
    IO.puts("%% LATENCY STAT")
    # Logger.debug("logs: #{inspect(logs)}")
    latencies = Enum.map(logs, fn {req_id, log_entry} -> %{type: log_entry.type, latency: log_entry.latency} end)
    # Logger.info("latencies: #{inspect(latencies)}")
    #IO.puts("%% latencies: #{inspect(latencies)}")

    latencies_get = Enum.filter(latencies, fn %{type: t, latency: l} -> t == :get end) |> Enum.map(fn %{type: t, latency: l} -> l end)
    # Logger.info("latencies of get: #{inspect(latencies_get)}, mean: #{Enum.sum(latencies_get)/Enum.count(latencies_get)}")
    #IO.puts("%% latencies of get: #{inspect(latencies_get)}, mean: #{Enum.sum(latencies_get)/Enum.count(latencies_get)}")
    IO.puts("%% Average Latency of get: #{Enum.sum(latencies_get)/Enum.count(latencies_get)}")
    latencies_put = Enum.filter(latencies, fn %{type: t, latency: l} -> t == :put end) |> Enum.map(fn %{type: t, latency: l} -> l end)
    # Logger.info("latencies of put: #{inspect(latencies_put)}, mean: #{Enum.sum(latencies_put)/Enum.count(latencies_put)}")
    #IO.puts("%% latencies of put: #{inspect(latencies_put)}, mean: #{Enum.sum(latencies_put)/Enum.count(latencies_put)}")
    IO.puts("%% Average Latency of put: #{Enum.sum(latencies_put)/Enum.count(latencies_put)}")
  end

  '''
  Get staleness stats
  '''
  @spec staleness_stat(%KvStore.LogEntry{}) :: nil
  def staleness_stat(logs) do
    # Logger.info("STALENESS STAT")
    IO.puts("%% STALENESS STAT")
    logs_get = Enum.filter(logs, fn {req_id, log_entry} -> log_entry.type == :get end)
    stale_gets = Enum.filter(logs_get, fn {req_id, log_entry} -> log_entry.is_stale end)
    if length(stale_gets) > 0 do
      #IO.puts("Stale entries found")
      {stale_id, stale_entry} = hd(stale_gets)
      stale_key = stale_entry.key
      stale_debug = Enum.sort_by(Enum.filter(logs, fn {req_id, log_entry} -> stale_key == log_entry.key && abs(req_id - stale_id) < 10 end), fn {req_id, log_entry} -> log_entry.timestamp end)
      #IO.puts("%% logs of stale gets: #{inspect(stale_gets)}")
      #IO.puts("%% logs of objects with stale keys: #{inspect(stale_debug)}")
    end
    get_count = Enum.count(logs_get)
    # Logger.info("logs of get: #{inspect(logs_get)} (total #{get_count} get requests)")
    #IO.puts("%% logs of get: #{inspect(logs_get)} (total #{get_count} get requests)")

    logs_stale = Enum.filter(logs, fn {req_id, log_entry} -> log_entry.is_stale end)
    stale_count = Enum.count(logs_stale)
    # Logger.info("stale rate: #{stale_count}/#{get_count} = #{stale_count/get_count}")
    IO.puts("%% stale rate: #{stale_count}/#{get_count} = #{stale_count/get_count}")
  end

  @spec generate_random_get(pos_integer()) :: {}
  def generate_random_get(keys) do
    key = Enum.random(1..keys)
    {:get, "#{key}", nil}
  end

  @spec generate_random_put(pos_integer(), pos_integer()) :: {}
  def generate_random_put(keys, values) do
    key = Enum.random(1..keys)
    value = Enum.random(1..values)
    {:put, "#{key}", value}
  end

  @spec generate_request(pid(), pid(), [], %{}) :: %{}
  def generate_request(load_balancer, observer, requests_list, context_map) do
    [head | tail] = requests_list
    {type, key, value} = head
    send(load_balancer, if type == :get do {type, key} else {type, key, value, Map.get(context_map, key, [])} end)
    send_ts = :os.system_time(:millisecond)
    Logger.info("Send request #{inspect(key)} - #{inspect(value)} - #{inspect(Map.get(context_map, key, []))}")
    receive do
      {sender, %KvStore.GetResponse{objects: objects, type: type, req_id: req_id}} ->
        Logger.info("Receive response of #{req_id} from #{sender} with #{inspect(objects)}")
        recv_ts = :os.system_time(:millisecond)
        send(:observer, KvStore.ClientRequestLog.new(req_id, whoami(), send_ts, recv_ts))
        context_map = Map.put(context_map, key, Enum.map(objects, fn obj -> obj.context end))
        Logger.debug("context_map: #{inspect(context_map)}")
        if length(tail) > 0 do generate_request(load_balancer, observer, tail, context_map) else context_map end
      {sender, %KvStore.PutResponse{context: context, type: type, req_id: req_id}} ->
        Logger.info("Receive response of #{req_id} from #{sender} with context: #{inspect(context)}")
        recv_ts = :os.system_time(:millisecond)
        send(:observer, KvStore.ClientRequestLog.new(req_id, whoami(), send_ts, recv_ts))
        context_map = Map.put(context_map, key, [context | []])
        if length(tail) > 0 do generate_request(load_balancer, observer, tail, context_map) else context_map end
      unknown ->
        Logger.info("client receive unknown msg: #{inspect(unknown)}")
        if length(tail) > 0 do generate_request(load_balancer, observer, tail, context_map) else context_map end
    after
      2000 ->
        send(:observer, KvStore.ClientTimeoutLog.new(whoami()))
        if length(tail) > 0 do generate_request(load_balancer, observer, tail, context_map) else context_map end
    end

  end

  @spec generate_drop(pid(), pid(), MapSet[pid()], MapSet[pid()], non_neg_integer(), non_neg_integer()) :: MapSet[pid()]
  def generate_drop(load_balancer, observer, down_nodes, nodes, drop_prob, alive_prob) do
    dropStat = :rand.uniform()
    aliveStat = :rand.uniform()
    IO.puts("dropStat is #{dropStat}")
    IO.puts("aliveStat is #{aliveStat}")
    liveNodes = MapSet.difference(nodes, down_nodes)
    new_down_nodes = if dropStat < drop_prob && !Enum.empty?(liveNodes) do
      to_down = Enum.random(liveNodes)
      IO.puts("Generating :node_down to #{inspect(to_down)}")
      send(load_balancer, {:node_down, to_down})
      MapSet.put(down_nodes, to_down)
    else
      down_nodes
    end
    new_down_nodes_up = if aliveStat < alive_prob and !Enum.empty?(down_nodes) do
      to_up = Enum.random(down_nodes)
      IO.puts("Generating :node_up to #{inspect(to_up)}")
      send(load_balancer, {:node_up, to_up})
      MapSet.delete(new_down_nodes, to_up)
    else
      new_down_nodes
    end
    new_down_nodes_up
  end

  @spec generate_requests(non_neg_integer(), pid(), pid(), non_neg_integer(), non_neg_integer(), pos_integer(), pos_integer(), %{}, [pid()], MapSet[pid()], float(), float()) :: nil
  def generate_requests(round, load_balancer, observer, gets, puts, keys, values, context_map, nodes, down_nodes, down_prob, up_prob) do
    Emulation.mark_unfuzzable()
    Logger.info("#{inspect(whoami())} generate requests round[#{round}]")
    new_down_nodes = generate_drop(load_balancer, observer, down_nodes, MapSet.new(nodes), down_prob, up_prob)
    Logger.info("#{inspect(whoami())} Got new down nodes as #{inspect(new_down_nodes)}")
    requests_list = Enum.shuffle(Enum.map(1..gets, fn _ -> generate_random_get(keys) end) ++ Enum.map(1..puts, fn _ -> generate_random_put(keys, values) end))
    context_map = generate_request(load_balancer, observer, requests_list, context_map)

    if round - 1 > 0 do generate_requests(round - 1, load_balancer, observer, gets, puts, keys, values, context_map, nodes, new_down_nodes, down_prob, up_prob) end
  end

  @spec handle_monitor(pos_integer(), non_neg_integer()) :: nil
  def handle_monitor(count, start_time) do
    receive do
      {:DOWN, _, _, _, _} ->
        IO.puts("client complete")
        if count - 1 > 0 do
          handle_monitor(count - 1, start_time)
        else
          total_time = :os.system_time(:millisecond) - start_time
          client_log = spawn(:client_log, fn ->
            Emulation.mark_unfuzzable()
            send(:observer, :get_log)
            receive do
              {_, {logs, timeouts, down_times}} ->
                TestCase.latency_stat(logs)
                TestCase.staleness_stat(logs)
                IO.puts("%% timeouts: #{timeouts}")
                IO.puts("%% node down times: #{inspect(down_times)}")
                IO.puts("%% down_time %: #{inspect(Map.new(down_times, fn {node, down_time} -> {node, down_time / total_time} end ))}")
              unknown ->
                # Logger.debug("client_log receive unknown msg #{inspect(unknown)}")
            end
          end)
          monitor_log = Process.monitor(client_log)
          receive do
            {:DOWN, ^monitor_log, _, _, _} -> true
          after
            100_000 -> assert false
          end
        end
      unknown ->
        IO.puts("monitor handler unknown message: #{inspect(unknown)}")
    # after
    #   100_000 -> assert false
    end
  end

  #@tag capture_log: false
  @tag timeout: 120000
  test "rounds=1000__gets=2__puts=1__kvnodes=(9,5,5)__keys=5__clients=3__delay=0" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(1), Fuzzers.drop(0.005)])
    Emulation.mark_unfuzzable()

    # parameters
    rounds = 1000
    gets = 2
    puts = 1
    keys = 5
    rep_factor = 5
    r_quorum = 3
    w_quorum = 3
    kv_nodes = [:a, :b, :c, :d, :e, :f, :g, :h, :i]
    assert rep_factor <= length(kv_nodes)
    clients = [:client_a, :client_b, :client_c]
    down_prob = 0.01
    up_prob = 0.3
    spawn(:observer, fn -> KvStore.Observer.run(KvStore.Observer.init(:observer)) end)
    lb_base_config =
      KvStore.LoadBalancer.init(kv_nodes, rep_factor, :observer)
    Logger.info("Base config: #{inspect(lb_base_config)}")
    spawn(:lb, fn -> KvStore.LoadBalancer.run(lb_base_config) end)


    kv_base_config =
      KvStore.init(kv_nodes, rep_factor, r_quorum, w_quorum, :observer)

    Enum.each(kv_nodes, fn node -> spawn(node, fn -> KvStore.run(kv_base_config) end) end)

    Logger.info("Spawned all nodes")
    start_time = :os.system_time(:millisecond)
    Enum.each(clients, fn client ->
      Process.monitor(
        spawn(client, fn ->
          TestCase.generate_requests(rounds, :lb, :observer, gets, puts, keys, 1000, %{}, kv_nodes, MapSet.new(), down_prob, up_prob)
        end)
      )
    end)

    TestCase.handle_monitor(length(clients), start_time)

  after
    Emulation.terminate()
  end

end
