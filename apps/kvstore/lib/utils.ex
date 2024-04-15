
defmodule KvStore.Utils do
  def hash(value) do
    value
    |> to_string()
    |> :crypto.hash(:sha256)
    |> :binary.decode_unsigned()
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
    sorted_nodes = state.sorted_nodes
    original_node = Enum.find(sorted_nodes, fn node -> (state.node_hashes[node] >= key_hash) end) || hd(sorted_nodes)
    node = Enum.find(sorted_nodes, fn node -> (state.node_hashes[node] >= key_hash) && Map.has_key?(state.live_nodes, node) end) || hd(sorted_nodes)
    {original_node, node}
  end
end
