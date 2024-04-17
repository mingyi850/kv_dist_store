defmodule UtilsTest do
  use ExUnit.Case
  alias KvStore.Utils

  import KvStore.Utils

  require Logger

  test "hash/1 hashes atom to a number" do
    assert Utils.hash(:a) == 259
    assert Utils.hash(:b) == 125
    assert Utils.hash(:c) == 118
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
    assert Utils.consistent_hash("key2", state) == {:c, :c}
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
    assert Utils.get_preference_list("key1", state, 3) == [:a, :e, :d]
    assert Utils.get_preference_list("key2", state, 3) == [:d, :c, :b]

    #Handle deleted nodes
    state = %{state | live_nodes: MapSet.delete(state.live_nodes, :d)}
    assert Utils.get_preference_list("key1", state, 3) == [:a, :e, :c]
    assert Utils.get_preference_list("key2", state, 3) == [:c, :b, :a]
  end


end
