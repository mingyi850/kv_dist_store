defmodule TestCase do
  use ExUnit.Case
  doctest KvStore.Observer
  import Emulation, only: [spawn: 2, send: 2, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn_link: 1, spawn_link: 3, send: 2, spawn: 2]

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
    Logger.info("LATENCY STAT")
    # Logger.debug("logs: #{inspect(logs)}")
    latencies = Enum.map(logs, fn {req_id, log_entry} -> %{type: log_entry.type, latency: log_entry.latency} end)
    Logger.info("latencies: #{inspect(latencies)}")

    latencies_get = Enum.filter(latencies, fn %{type: t, latency: l} -> t == :get end) |> Enum.map(fn %{type: t, latency: l} -> l end)
    Logger.info("latencies of get: #{inspect(latencies_get)}, mean: #{Enum.sum(latencies_get)/Enum.count(latencies_get)}")

    latencies_put = Enum.filter(latencies, fn %{type: t, latency: l} -> t == :put end) |> Enum.map(fn %{type: t, latency: l} -> l end)
    Logger.info("latencies of put: #{inspect(latencies_put)}, mean: #{Enum.sum(latencies_put)/Enum.count(latencies_put)}")
  end

  '''
  Get staleness stats
  '''
  @spec staleness_stat(%KvStore.LogEntry{}) :: nil
  def staleness_stat(logs) do 
    Logger.info("STALENESS STAT")
    logs_get = Enum.filter(logs, fn {req_id, log_entry} -> log_entry.type == :get end)
    get_count = Enum.count(logs_get)
    # Logger.info("logs of get: #{inspect(logs_get)} (total #{get_count} get requests)")
    
    logs_stale = Enum.filter(logs, fn {req_id, log_entry} -> log_entry.is_stale end)
    stale_count = Enum.count(logs_stale)
    Logger.info("stale rate: #{stale_count}/#{get_count} = #{stale_count/get_count}")
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

  @spec generate_requests(pid(), pid(), pos_integer(), non_neg_integer(), non_neg_integer(), pos_integer(), pos_integer()) :: nil
  def generate_requests(load_balancer, observer, rounds, gets, puts, keys, values) do 
    Logger.info("generate requests round[#{rounds}]")
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

    if rounds > 1 do TestCase.generate_requests(load_balancer, observer, rounds - 1, gets, puts, keys, values) end
  end


  test "100 gets / 50 puts / 3 kv_nodes / 5 keys" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])
    
    spawn(:observer, fn -> KvStore.Observer.run(KvStore.Observer.init(:observer)) end)
    lb_base_config =
      KvStore.LoadBalancer.init([:a, :b, :c], 1, :observer)
    Logger.info("Base config: #{inspect(lb_base_config)}")
    spawn(:lb, fn -> KvStore.LoadBalancer.run(lb_base_config) end)
    
    kv_base_config =
      KvStore.init([:a, :b, :c], 1, 1, 1, :observer)

    spawn(:a, fn -> KvStore.run(kv_base_config) end)
    spawn(:b, fn -> KvStore.run(kv_base_config) end)
    spawn(:c, fn -> KvStore.run(kv_base_config) end)

    Logger.info("Spawned all nodes")

    client_a = spawn(:client_a, fn -> TestCase.generate_requests(:lb, :observer, 25, 2, 1, 5, 1000) end)
    client_b = spawn(:client_b, fn -> TestCase.generate_requests(:lb, :observer, 25, 2, 1, 5, 1000) end)

    monitor_a = Process.monitor(client_a)
    monitor_b = Process.monitor(client_b)

    receive do
      {:DOWN, ^monitor_a, _, _, _} -> 
        Logger.debug("client_a complete")
        receive do 
          {:DOWN, ^monitor_b, _, _, _} -> 
            Logger.debug("client_b complete")
            client_log = spawn(:client_log, fn -> 
              send(:observer, :get_log)
              receive do 
                {_, logs} -> 
                  TestCase.latency_stat(logs)
                  TestCase.staleness_stat(logs)
                unknown -> 
                  Logger.debug("client_log receive unknown msg #{inspect(unknown)}")
              end
            end)
            monitor_log = Process.monitor(client_log)
            receive do 
              {:DOWN, ^monitor_log, _, _, _} -> true
            after 
              10_000_000 -> assert false 
            end
        after 
          1_000_000 -> assert false
        end
      {:DOWN, ^monitor_b, _, _, _} -> 
        Logger.debug("client_b complete")
        receive do 
          {:DOWN, ^monitor_a, _, _, _} -> 
            Logger.debug("client_a complete")
            client_log = spawn(:client_log, fn -> 
              send(:observer, :get_log)
              receive do 
                {_, logs} -> 
                  TestCase.latency_stat(logs)
                  TestCase.staleness_stat(logs)
                unknown -> 
                  Logger.debug("client_log receive unknown msg #{inspect(unknown)}")
              end
            end)
            monitor_log = Process.monitor(client_log)
            receive do 
              {:DOWN, ^monitor_log, _, _, _} -> true
            after 
              10_000_000 -> assert false 
            end
        after 
          1_000_000 -> assert false
        end
    after
      10_000_000 -> assert false
    end

  after
    Emulation.terminate()
  end

end