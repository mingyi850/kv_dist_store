defmodule LoadBalancer do

  defstruct(
    sorted_nodes: [],
    live_nodes: {},
    replication_factor: 1,
    node_hashes: %{}
  )

  @spec init([atom()], integer()) :: %LoadBalancer{}
  def init(nodes, replication_factor) do
    node_hashes = Enum.map(nodes, fn node -> {node, hash(node)} end) |> Enum.into(%{})
    %LoadBalancer{
      sorted_nodes: Enum.sort_by(nodes, fn node -> node_hashes[node] end),
      live_nodes: Enum.into(nodes, %{}),
      replication_factor: replication_factor,
      node_hashes: node_hashes
    }
  end

  @spec handle_call(any(), atom(), %LoadBalancer{}) :: %LoadBalancer{}
  def handle_call(message, sender, state) do
    case message do
      {:get, key} ->
        node = consistent_hash(key, state.sorted_nodes)
        node.send({:get, key}, sender)
        state

      {:put, key, object, context} ->
        node = consistent_hash(key, state.sorted_nodes)
        node.send({:put, key, object, context}, sender)
        state
    end
  end

  def handle_cast({:forward, node, key, object, context}, state) do
    forward_request(node, key, object, context)
    {:noreply, state}
  end

  defp get_next_node(node, state) do
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

  defp consistent_hash(key, state) do
    key_hash = hash(key)
    sorted_nodes = state.sorted_nodes
    node = Enum.find(sorted_nodes, fn node -> (state.node_hashes[node] >= key_hash) && Map.has_key?(state.live_nodes, node) end) || hd(sorted_nodes)
    node
  end

  defp hash(value) do
    value
    |> to_string()
    |> :crypto.hash(:sha256)
    |> :binary.decode_unsigned()
  end

  defp forward_request(node, key, object, context) do
    # Implement logic to forward the request to the specified node
  end
end
