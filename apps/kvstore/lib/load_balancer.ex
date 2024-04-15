defmodule KvStore.LoadBalancer do

  require KvStore.Utils
  require KvStore.PutRequest
  require KvStore.GetRequest
  import KvStore.Utils
  import Emulation

  #Client should send messages to the LoadBalancer actor as messages with form
  #{:get, key} or {:put, key, object, context}.
  #The LoadBalancer actor will then route the requests to the appropriate servers
  #For the purpose of the simulation - we will assume that the LoadBalancer is aware of down servers.
  #This can be triggered by sending :node_down and :node_up messages to the LoadBalancer actor.

  defstruct(
    sorted_nodes: [],
    live_nodes: MapSet.new(),
    replication_factor: 1,
    node_hashes: %{}
  )

  @spec init([atom()], integer()) :: %KvStore.LoadBalancer{}
  def init(nodes, replication_factor) do
    node_hashes = Enum.map(nodes, fn node -> {node, hash(node)} end) |> Enum.into(%{})
    %KvStore.LoadBalancer{
      sorted_nodes: Enum.sort_by(nodes, fn node -> node_hashes[node] end),
      live_nodes: Enum.into(nodes, %{}),
      replication_factor: replication_factor,
      node_hashes: node_hashes
    }
  end

  @spec receive(%KvStore.LoadBalancer{}) :: %KvStore.LoadBalancer{}
  def receive(state) do
    receive do
      {sender, {:get, key}} ->
        {original_node, node} = consistent_hash(key, state.sorted_nodes)
        node.send(KvStore.GetRequest.new(key, original_node, sender))
        state
      {sender, {:put, key, object, context}} ->
        {original_node, node} = consistent_hash(key, state.sorted_nodes)
        node.send(KvStore.PutRequest.new(context, key, object, original_node, sender))
        state
      {_, {:node_down, node}} ->
        state = %{state | live_nodes: MapSet.delete(state.live_nodes, node)}
        state
      {_, {:node_up, node}} ->
        state = %{state | live_nodes: MapSet.put(state.live_nodes, node)}
        state
    end
  end

  @spec handle_call(any(), atom(), %KvStore.LoadBalancer{}) :: %KvStore.LoadBalancer{}
  @spec handle_call(
          {:get, binary()} | {:put, any(), pid(), binary()},
          pid(),
          atom()
          | %{
              :sorted_nodes => %{
                live_nodes: MapSet.t(),
                node_hashes: map(),
                sorted_nodes: [atom()]
              },
              optional(any()) => any()
            }
        ) ::
          atom()
          | %{
              :sorted_nodes => %{
                live_nodes: MapSet.t(),
                node_hashes: map(),
                sorted_nodes: [atom()]
              },
              optional(any()) => any()
            }
  def handle_call(message, sender, state) do
    case message do
      {:get, key} ->
        {original_node, node} = consistent_hash(key, state.sorted_nodes)
        node.send(KvStore.GetRequest.new(key, original_node, sender))
        state

      {:put, key, object, context} ->


        {original_node, node} = consistent_hash(key, state.sorted_nodes)
        node.send(KvStore.PutRequest.new(context, key, object, original_node, sender))
        state
    end
  end
end
