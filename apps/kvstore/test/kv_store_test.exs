defmodule KvStoreTest do
  use ExUnit.Case
  doctest KvStore
  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2, spawn: 2]

  require KvStore.TestClient
  import KvStore.TestClient
  import KvStore
  import KvStore.Utils
  import KvStoreTest.Utils

  require Logger

  test "KV Store receives and processes updates" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    base_config =
      KvStore.init([:a, :b, :c], 1, 1, 1, :observer)

    spawn(:a, fn -> KvStore.run(base_config) end)

    client =
      spawn(:client, fn ->
        first = KvStore.TestClient.testClientSend(KvStore.GetRequest.new("key1", :client, :a, 1), :a)
        Logger.info("Got first as #{inspect(first)}")
        assert first.objects == []
        second = KvStore.TestClient.testClientSend(KvStore.PutRequest.new("key1", 123, [], :client, :a, 2), :a)
        Logger.info("Got second as #{inspect(second)}")
        assert second.context != nil
        third = KvStore.TestClient.testClientSend(KvStore.GetRequest.new("key1", :client, :a, 3), :a)
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

  test "KV Store receives and processes updates without context" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    base_config =
      KvStore.init([:a, :b, :c], 1, 1, 1, :observer)

    spawn(:a, fn -> KvStore.run(base_config) end)

    client =
      spawn(:client, fn ->
        first = KvStore.TestClient.testClientSend(KvStore.GetRequest.new("key1", :client, :a, 1), :a)
        Logger.info("Got first as #{inspect(first)}")
        assert first.objects == []
        second = KvStore.TestClient.testClientSend(KvStore.PutRequest.new("key1", 123, [], :client, :a, 2), :a)
        Logger.info("Got second as #{inspect(second)}")
        assert second.context != nil
        third = KvStore.TestClient.testClientSend(KvStore.PutRequest.new("key1", 123, [], :client, :a, 3), :a)
        Logger.info("Got second as #{inspect(second)}")
        assert third.context != nil
        fourth = KvStore.TestClient.testClientSend(KvStore.GetRequest.new("key1", :client, :a, 4), :a)
        Logger.info("Got third as #{inspect(third)}")
        assert fourth.objects != []
        assert length(fourth.objects) == 1
        assert hd(fourth.objects).object == 123
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
      KvStore.init(nodes, 3, 2, 2, :observer)

    nodes |> Enum.each(fn node -> spawn(node, fn -> KvStore.run(base_config) end) end)

    client =
      spawn(:client, fn ->
        first = KvStore.TestClient.testClientSend(KvStore.GetRequest.new("key1", :client, :a, 1), :b)
        Logger.info("Got first as #{inspect(first)}")
        assert first.objects == []
        second = KvStore.TestClient.testClientSend(KvStore.PutRequest.new("key1", 123, [], :client, :a, 2), :a)
        Logger.info("Got second as #{inspect(second)}")
        assert second.context != nil
        third = KvStore.TestClient.testClientSend(KvStore.GetRequest.new("key1", :client, :a, 3), :c)
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

  test "Gossip protocol allows eventual failure detection and rejoining" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    nodes = [:a, :b, :c, :d, :e]
    base_config =
      KvStore.init(nodes, 3, 2, 2, :observer)

    nodes |> Enum.each(fn node -> spawn(node, fn -> KvStore.run(base_config) end) end)

    client =
      spawn(:client, fn ->
        first = KvStore.TestClient.testClientSend(KvStore.GetRequest.new("key1", :client, :a, 1), :b)
        Logger.info("Got first as #{inspect(first)}")
        assert first.objects == []
        second = KvStore.TestClient.testClientSend(KvStore.PutRequest.new("key1", 123, [], :client, :a, 2), :a)
        Logger.info("Got second as #{inspect(second)}")
        assert second.context != nil
        third = KvStore.TestClient.testClientSend(KvStore.GetRequest.new("key1", :client, :a, 3), :c)
        Logger.info("Got third as #{inspect(third)}")
        assert third.objects != []
        assert hd(third.objects).object == 123

        #Kill a node
        send(:a, :node_down)
        receive do
        after
          5000 -> :ok
        end
        #Check that all nodes agree that node is down
        live_nodes = nodes |> Enum.reject(fn node -> node == :a end)
        states = live_nodes |> Enum.map(fn node -> KvStore.TestClient.testClientSend(:get_state, node).live_nodes end)
        assert Enum.all?(states, fn x -> !MapSet.member?(x, :a) end)

        send(:a, :node_up)
        receive do
        after
          5000 -> :ok
        end
        #Check that all nodes agree that node is up
        states = nodes |> Enum.map(fn node -> KvStore.TestClient.testClientSend(:get_state, node).live_nodes end)
        assert Enum.all?(states, fn x -> MapSet.member?(x, :a) end)
      end)
    handle = Process.monitor(client)

    #Timeout.
    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      15_000 -> assert false
    end
  after
    Emulation.terminate()
  end

  test "Merkle tree sync repairs missed data after outage" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    nodes = [:a, :b, :c, :d, :e]
    base_config =
      KvStore.init(nodes, 3, 2, 2, :observer)

    sorted_nodes = KvStore.Utils.sort_nodes(nodes)
    node_hashes = Enum.map(nodes, fn node -> {node, hash(node)} end) |> Enum.into(%{})
    live_nodes = MapSet.new(sorted_nodes)
    state = %{sorted_nodes: sorted_nodes, node_hashes: node_hashes, live_nodes: live_nodes}
    owned_key = KvStoreTest.Utils.generate_key(:a, state)
    replicated_key = KvStoreTest.Utils.generate_key(KvStore.Utils.get_previous_node(:a, %{sorted_nodes: sorted_nodes}), state)
    nodes |> Enum.each(fn node -> spawn(node, fn -> KvStore.run(base_config) end) end)

    sorted_nodes = nodes |> Enum.sort()
    client =
      spawn(:client, fn ->
        receive do
        after
          1000 -> :ok
        end
        #Kill :a
        send(:a, :node_down)
        #Populate Nodes
        first = KvStore.TestClient.testClientSend(KvStore.PutRequest.new(owned_key, 123, [], :client, :a, 1), :b)
        Logger.info("Got first as #{inspect(first)}")
        second = KvStore.TestClient.testClientSend(KvStore.PutRequest.new(replicated_key, 23123, [], :client, :a, 2), :c)
        assert first.context != nil
        assert second.context != nil
        third = KvStore.TestClient.testClientSend(KvStore.GetRequest.new(owned_key, :client, :a, 3), :b)
        Logger.info("Got third as #{inspect(third)}")
        assert hd(third.objects).object == 123

        #Kill a node
        send(:a, :node_up)
        receive do
        after
          5000 -> :ok
        end
        #Check that all nodes agree that node is down
        owned_key_resp = KvStore.TestClient.testClientSend({:get_value_at, owned_key}, :a)
        replicated_key_resp = KvStore.TestClient.testClientSend({:get_value_at, replicated_key}, :a)
        #Check that all nodes agree that node is up
        assert hd(owned_key_resp).object == 123
        assert hd(replicated_key_resp).object == 23123
      end)
    handle = Process.monitor(client)

    #Timeout.
    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      15_000 -> assert false
    end
  after
    Emulation.terminate()
  end
end
