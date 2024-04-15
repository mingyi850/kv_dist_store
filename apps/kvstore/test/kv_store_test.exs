defmodule KvStoreTest do
  use ExUnit.Case
  doctest KvStore
  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2, spawn: 2]

  require KvStore.TestClient
  import KvStore.TestClient
  import KvStore

  require Logger

  test "KV Store receives and processes updates" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    base_config =
      KvStore.init([:a, :b, :c], 1)

    spawn(:a, fn -> KvStore.run(base_config) end)

    client =
      spawn(:client, fn ->
        first = KvStore.TestClient.testClientSend(KvStore.GetRequest.new("key1", :client, :a), :a)
        Logger.info("Got first as #{inspect(first)}")
        assert first.object == nil
        second = KvStore.TestClient.testClientSend(KvStore.PutRequest.new("key1", 123, nil, :client, :a), :a)
        Logger.info("Got second as #{inspect(second)}")
        assert second.context != nil
        third = KvStore.TestClient.testClientSend(KvStore.GetRequest.new("key1", :client, :a), :a)
        Logger.info("Got third as #{inspect(third)}")
        assert third.context == second.context
        assert third.object == 123
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
