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
    IO.puts("%% latencies: #{inspect(latencies)}")

    latencies_get = Enum.filter(latencies, fn %{type: t, latency: l} -> t == :get end) |> Enum.map(fn %{type: t, latency: l} -> l end)
    # Logger.info("latencies of get: #{inspect(latencies_get)}, mean: #{Enum.sum(latencies_get)/Enum.count(latencies_get)}")
    IO.puts("%% latencies of get: #{inspect(latencies_get)}, mean: #{Enum.sum(latencies_get)/Enum.count(latencies_get)}")

    latencies_put = Enum.filter(latencies, fn %{type: t, latency: l} -> t == :put end) |> Enum.map(fn %{type: t, latency: l} -> l end)
    # Logger.info("latencies of put: #{inspect(latencies_put)}, mean: #{Enum.sum(latencies_put)/Enum.count(latencies_put)}")
    IO.puts("%% latencies of put: #{inspect(latencies_put)}, mean: #{Enum.sum(latencies_put)/Enum.count(latencies_put)}")
  end

  '''
  Get staleness stats
  '''
  @spec staleness_stat(%KvStore.LogEntry{}) :: nil
  def staleness_stat(logs) do 
    # Logger.info("STALENESS STAT")
    IO.puts("%% STALENESS STAT")
    logs_get = Enum.filter(logs, fn {req_id, log_entry} -> log_entry.type == :get end)
    get_count = Enum.count(logs_get)
    # Logger.info("logs of get: #{inspect(logs_get)} (total #{get_count} get requests)")
    IO.puts("%% logs of get: #{inspect(logs_get)} (total #{get_count} get requests)")
    
    logs_stale = Enum.filter(logs, fn {req_id, log_entry} -> log_entry.is_stale end)
    stale_count = Enum.count(logs_stale)
    # Logger.info("stale rate: #{stale_count}/#{get_count} = #{stale_count/get_count}")
    IO.puts("%% stale rate: #{stale_count}/#{get_count} = #{stale_count/get_count}")
  end

  @spec generate_random_get(pos_integer()) :: {}
  def generate_random_get(keys) do 
    key = Enum.random(1..keys)
    {:get, "#{key}"}
  end

  @spec generate_random_put(pos_integer(), pos_integer()) :: {}
  def generate_random_put(keys, values) do 
    key = Enum.random(1..keys)
    value = Enum.random(1..values)
    {:put, "#{key}", value, []}
  end

  @spec generate_requests(pid(), pid(), non_neg_integer(), non_neg_integer(), pos_integer(), pos_integer()) :: nil
  def generate_requests(load_balancer, observer, gets, puts, keys, values) do 
    Logger.info("generate requests")
    requests_list = Enum.map(1..gets, fn _ -> generate_random_get(keys) end) ++ Enum.map(1..puts, fn _ -> generate_random_put(keys, values) end)
    Enum.each(Enum.shuffle(requests_list), fn req -> 
      send(load_balancer, req)
      send_ts = :os.system_time(:millisecond)
      Logger.info("Send request #{inspect(req)}")
      receive do 
        {sender, %KvStore.GetResponse{objects: objects, type: type, req_id: req_id}} ->
          Logger.info("Receive response of #{req_id} from #{sender} with #{inspect(objects)}")
          recv_ts = :os.system_time(:millisecond)
          send(:observer, KvStore.ClientRequestLog.new(req_id, :client_c, send_ts, recv_ts))
        {sender, %KvStore.PutResponse{context: context, type: type, req_id: req_id}} ->
          Logger.info("Receive response of #{req_id} from #{sender} with context: #{inspect(context)}")
          recv_ts = :os.system_time(:millisecond)
          send(:observer, KvStore.ClientRequestLog.new(req_id, :client_b, send_ts, recv_ts))
        unknown -> 
          Logger.info("client receive unknown msg: #{inspect(unknown)}")
      end
    end)
  end

  @spec handle_monitor(pos_integer()) :: nil
  def handle_monitor(count) do
    receive do 
      {:DOWN, _, _, _, _} -> 
        IO.puts("client complete")
        if count - 1 > 0 do 
          handle_monitor(count - 1)
        else 
          client_log = spawn(:client_log, fn -> 
            send(:observer, :get_log)
            receive do 
              {_, logs} -> 
                TestCase.latency_stat(logs)
                TestCase.staleness_stat(logs)
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

  test "gets=2000__puts=1000__kvnodes=1(1,1,1)__keys=5__clients=2__delay=0" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(0)])
    
    # parameters
    rep_factor = 1
    r_quorum = 1
    w_quorum = 1
    kv_nodes = [:a]
    clients = [:client_a, :client_b]

    spawn(:observer, fn -> KvStore.Observer.run(KvStore.Observer.init(:observer)) end)
    lb_base_config =
      KvStore.LoadBalancer.init(kv_nodes, rep_factor, :observer)
    Logger.info("Base config: #{inspect(lb_base_config)}")
    spawn(:lb, fn -> KvStore.LoadBalancer.run(lb_base_config) end)
    
    
    kv_base_config =
      KvStore.init(kv_nodes, rep_factor, r_quorum, w_quorum, :observer)

    Enum.each(kv_nodes, fn node -> spawn(node, fn -> KvStore.run(kv_base_config) end) end)

    Logger.info("Spawned all nodes")

    Enum.each(clients, fn client -> 
      Process.monitor(
        spawn(client, fn -> 
          Enum.each(1..1000, fn iter ->
            TestCase.generate_requests(:lb, :observer, 2, 1, 5, 1000) 
            end)
        end)
      )
    end)

    TestCase.handle_monitor(length(clients))

  after
    Emulation.terminate()
  end

end