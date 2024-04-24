defmodule ObserverTest do
  use ExUnit.Case
  doctest KvStore.Observer
  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2, spawn: 2]

  require KvStore.TestClient
  import KvStore.TestClient
  import KvStore.LoadBalancer
  import KvStore.Observer

  require Logger

  test "Test Observer" do
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
    spawn(:client_a, fn ->
      send(:lb, {:get, "key1"})
      send_ts = :os.system_time(:millisecond)
      Logger.info("Send first request: get 'key1'")
      receive do 
        {sender, %KvStore.GetResponse{objects: objects, type: type, req_id: req_id}} ->
          Logger.info("Receive response of #{req_id} from #{sender} with objects: #{inspect(objects)}")
          recv_ts = :os.system_time(:millisecond)
          send(:observer, KvStore.ClientRequestLog.new(req_id, :client_a, send_ts, recv_ts))
        unknown -> 
          Logger.info("client receive unknown msg: #{inspect(unknown)}")
      end
      receive do
      after
        5_000 -> true
      end
    end)
    spawn(:client_b, fn ->
      send(:lb, {:put, "key1", 123, []})
      send_ts = :os.system_time(:millisecond)
      Logger.info("Send second request: put 'key1' with '123'")
      receive do 
        {sender, %KvStore.PutResponse{context: context, type: type, req_id: req_id}} ->
          Logger.info("Receive response of #{req_id} from #{sender} with context: #{inspect(context)}")
          recv_ts = :os.system_time(:millisecond)
          send(:observer, KvStore.ClientRequestLog.new(req_id, :client_b, send_ts, recv_ts))
        unknown -> 
          Logger.info("client receive unknown msg: #{inspect(unknown)}")
      end
      receive do
      after
        5_000 -> true
      end
    end)
    spawn(:client_c, fn ->
      send(:lb, {:get, "key1"})
      send_ts = :os.system_time(:millisecond)
      Logger.info("Send third request: get 'key1'")
      receive do 
        {sender, %KvStore.GetResponse{objects: objects, type: type, req_id: req_id}} ->
          Logger.info("Receive response of #{req_id} from #{sender} with #{inspect(objects)}")
          recv_ts = :os.system_time(:millisecond)
          send(:observer, KvStore.ClientRequestLog.new(req_id, :client_c, send_ts, recv_ts))
        unknown -> 
          Logger.info("client receive unknown msg: #{inspect(unknown)}")
      end
      receive do
      after
        5_000 -> true
      end
    end)

    #Timeout.
    receive do
    after
      10_000 -> true
    end
  after
    Emulation.terminate()
  end

  test "Test staleness" do
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
    spawn(:client_a, fn ->
      send(:lb, {:put, "key1", 123, [], :delay, 5_000})
      send_ts = :os.system_time(:millisecond)
      Logger.info("Send first request: put 'key1' with '123'")
      receive do 
        {sender, %KvStore.PutResponse{context: context, type: type, req_id: req_id}} ->
          Logger.info("Receive response of #{req_id} from #{sender} with context: #{inspect(context)}")
          recv_ts = :os.system_time(:millisecond)
          send(:observer, KvStore.ClientRequestLog.new(req_id, :client_b, send_ts, recv_ts))
        unknown -> 
          Logger.info("client receive unknown msg: #{inspect(unknown)}")
      end
    end)
    spawn(:client_b, fn ->
      send(:lb, {:get, "key1"})
      send_ts = :os.system_time(:millisecond)
      Logger.info("Send second request: get 'key1'")
      receive do 
        {sender, %KvStore.GetResponse{objects: objects, type: type, req_id: req_id}} ->
          Logger.info("Receive response of #{req_id} from #{sender} with #{inspect(objects)}")
          recv_ts = :os.system_time(:millisecond)
          send(:observer, KvStore.ClientRequestLog.new(req_id, :client_c, send_ts, recv_ts))
        unknown -> 
          Logger.info("client receive unknown msg: #{inspect(unknown)}")
      end
    end)
    spawn(:client_c, fn ->
      receive do 
      after 
        10_000 -> 
          Logger.info("Ask for stale stat")
          send(:observer, :get_stale_stat)
      end
      receive do 
        {_, %{get_count: get_count, stale_count: stale_count}} -> 
          Logger.info("Stale stat: get_count = #{inspect(get_count)}, stale_count = #{inspect(stale_count)}")
        unknown ->
          Logger.debug("unknown message: #{inspect(unknown)}")
      end
    end)

    #Timeout.
    receive do
    after
      20_000 -> true
    end
  after
    Emulation.terminate()
  end

end