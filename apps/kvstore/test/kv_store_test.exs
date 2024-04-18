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
      KvStore.init([:a, :b, :c], 1, 1, 1)

    spawn(:a, fn -> KvStore.run(base_config) end)

    client =
      spawn(:client, fn ->
        first = KvStore.TestClient.testClientSend(KvStore.GetRequest.new("key1", :client, :a), :a)
        Logger.info("Got first as #{inspect(first)}")
        assert first.objects == []
        second = KvStore.TestClient.testClientSend(KvStore.PutRequest.new("key1", 123, [], :client, :a), :a)
        Logger.info("Got second as #{inspect(second)}")
        assert second.context != nil
        third = KvStore.TestClient.testClientSend(KvStore.GetRequest.new("key1", :client, :a), :a)
        Logger.info("Got third as #{inspect(third)}")
        assert third.objects != []
        assert hd(third.objects).object == 123
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

  test "KV Store receives and processes updates from any coordinator node" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    nodes = [:a, :b, :c, :d, :e]
    base_config =
      KvStore.init(nodes, 3, 2, 2)

    nodes |> Enum.each(fn node -> spawn(node, fn -> KvStore.run(base_config) end) end)

    client =
      spawn(:client, fn ->
        first = KvStore.TestClient.testClientSend(KvStore.GetRequest.new("key1", :client, :a), :b)
        Logger.info("Got first as #{inspect(first)}")
        assert first.objects == []
        second = KvStore.TestClient.testClientSend(KvStore.PutRequest.new("key1", 123, [], :client, :a), :a)
        Logger.info("Got second as #{inspect(second)}")
        assert second.context != nil
        third = KvStore.TestClient.testClientSend(KvStore.GetRequest.new("key1", :client, :a), :c)
        Logger.info("Got third as #{inspect(third)}")
        assert third.objects != []
        assert hd(third.objects).object == 123
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
