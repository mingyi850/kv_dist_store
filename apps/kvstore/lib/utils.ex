
defmodule KvStore.Utils do
  require Logger

  @spec hash(any()) :: integer()
  def hash(value) do
    strValue = to_string(value)
    :crypto.hash(:sha256, strValue)
    |> :binary.decode_unsigned()
    |> rem(360)
  end

  # Get the next node (should be used when requests to the original node fail)
  @spec get_next_node(atom(), %{sorted_nodes: [atom()], live_nodes: MapSet.t(atom())}) :: atom()
  def get_next_node(node, state) do
    node_index = Enum.find_index(state.sorted_nodes, fn n -> n == node end)
    next_index = rem(node_index + 1, length(state.sorted_nodes))
    next_node = Enum.at(state.sorted_nodes, next_index)
    # Check if next node is valid, otherwise, return the following node
    if Map.has_key?(state.live_nodes, next_node) do
      next_node
    else
      get_next_node(next_node, state)
    end
  end

@spec consistent_hash(any(), %{sorted_nodes: [atom()], node_hashes: map(), live_nodes: MapSet.t(atom())}) ::
          {any(), any()}
  def consistent_hash(key, state) do
    key_hash = hash(key)
    Logger.info("Hash for key #{key} is #{key_hash}")
    sorted_nodes = state.sorted_nodes
    # Show all nodes with higher hash value
    Enum.each(sorted_nodes, fn node -> if state.node_hashes[node] >= key_hash do Logger.info("Node #{node} has hash #{state.node_hashes[node]}") end end)
    original_node = Enum.find(sorted_nodes, fn node -> (state.node_hashes[node] >= key_hash) end) || hd(sorted_nodes)
    node = Enum.find(sorted_nodes, fn node -> (state.node_hashes[node] >= key_hash) && MapSet.member?(state.live_nodes, node) end) || hd(sorted_nodes)
    {original_node, node}
  end
end
