defmodule LoadBalancerTest do
  use ExUnit.Case
  doctest KvStore.LoadBalancer
  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2, spawn: 2]

  require KvStore.TestClient
  import KvStore.TestClient
  import KvStore.LoadBalancer

  require Logger

  test "Load balancer distributes loads based on hash" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    base_config =
      KvStore.LoadBalancer.init([:a, :b, :c], 1)
    Logger.info("Base config: #{inspect(base_config)}")
    spawn(:lb, fn -> KvStore.LoadBalancer.run(base_config) end)
    spawn(:a, fn -> KvStore.TestClient.kvStub() end)
    spawn(:b, fn -> KvStore.TestClient.kvStub() end)
    spawn(:c, fn -> KvStore.TestClient.kvStub() end)

    Logger.info("Spawned all nodes")
    client =
      spawn(:client, fn ->
        first = KvStore.TestClient.testClientSend("key1", :lb)
        Logger.info("Got first as #{inspect(first)}")
        second = KvStore.TestClient.testClientSend("key3123", :lb)
        Logger.info("Got second as #{inspect(second)}")
        third = KvStore.TestClient.testClientSend("key142", :lb)
        Logger.info("Got third as #{inspect(third)}")
        assert first != second || first != third
      end)
    handle = Process.monitor(client)

    #Timeout.
    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      10_000 -> assert false
    end
  after
    Emulation.terminate()
  end
end
