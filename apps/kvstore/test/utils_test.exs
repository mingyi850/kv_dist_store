defmodule UtilsTest do
  use ExUnit.Case
  alias KvStore.Utils

  import KvStore.Utils

  require Logger

  test "hash/1 hashes atom to a number" do
    assert Utils.hash(:a) == 91634880152443617534842621287039938041581081254914058002978601050179556493499
  end

  test "sort_nodes/1 sorts nodes by hash" do
    assert Utils.sort_nodes([:c, :b, :a]) == [:c, :b, :a]
    assert Utils.sort_nodes([:a, :b, :c]) == [:c, :b, :a]
    assert Utils.sort_nodes([:b, :a, :c]) == [:c, :b, :a]
  end

  test "consistent_hash returns the correct node" do
    nodes = Utils.sort_nodes([:c, :b, :a])
    hashes = nodes |> Enum.map(fn node -> {node, Utils.hash(node)} end) |> Enum.into(%{})

    state = %{
      node_hashes: hashes,
      live_nodes: MapSet.new(nodes),
      sorted_nodes: nodes
    }
    assert Utils.consistent_hash("key1", state) == {:a, :a}
    assert Utils.consistent_hash("key2", state) == {:a, :a}
    assert Utils.consistent_hash("key3", state) == {:c, :c}

    #Handle deleted nodes
    state = %{state | live_nodes: MapSet.delete(state.live_nodes, :a)}
    assert Utils.consistent_hash("key1", state) == {:a, :c}
  end

  test "get preference list returns a correct list of nodes" do
    nodes = Utils.sort_nodes([:a, :b, :c, :d, :e])
    Logger.debug("Nodes: #{inspect(nodes)}")
    hashes = nodes |> Enum.map(fn node -> {node, Utils.hash(node)} end) |> Enum.into(%{})
    state = %{
      node_hashes: hashes,
      live_nodes: MapSet.new(nodes),
      sorted_nodes: nodes
    }
    assert Utils.get_preference_list("key1", state, 3) == [:a, :d, :c]
    assert Utils.get_preference_list("key3", state, 3) == [:d, :c, :b]

    #Handle deleted nodes
    state = %{state | live_nodes: MapSet.delete(state.live_nodes, :d)}
    assert Utils.get_preference_list("key1", state, 3) == [:a, :c, :b]
    assert Utils.get_preference_list("key3", state, 3) == [:c, :b, :e]
    state = %{state | live_nodes: MapSet.delete(state.live_nodes, :b)}
    state = %{state | live_nodes: MapSet.delete(state.live_nodes, :c)}
    assert Utils.get_preference_list("key1", state, 3) == [:a, :e]
    assert Utils.get_preference_list("key3", state, 3) == [:e, :a]
  end

  test "get previous node" do
    nodes = [:a, :b, :c, :d, :e]
    state = %{
      sorted_nodes: nodes
    }
    assert Utils.get_previous_node(:a, state) == :e
    assert Utils.get_previous_node(:b, state) == :a
    assert Utils.get_previous_node(:e, state) == :d
  end
  test "get responsible range" do
    nodes = [:a, :b, :c, :d, :e]
    state = %{
      sorted_nodes: nodes,
      replication_factor: 3
    }
    assert Utils.get_responsible_range(:a, state) == MapSet.new([:d, :e, :a])
    assert Utils.get_responsible_range(:b, state) == MapSet.new([:e, :a, :b])
  end


end
