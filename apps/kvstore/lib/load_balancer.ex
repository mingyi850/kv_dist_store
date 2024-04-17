defmodule KvStore.LoadBalancer do

  require KvStore.Utils
  require KvStore.PutRequest
  require KvStore.GetRequest
  import KvStore.Utils
  import Emulation
  import Kernel, except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Logger
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
    Logger.info("Initializing LoadBalancer with nodes: #{inspect(nodes)}")
    node_hashes = Enum.map(nodes, fn node -> {node, hash(node)} end) |> Enum.into(%{})
    %KvStore.LoadBalancer{
      sorted_nodes: sort_nodes(nodes),
      live_nodes: MapSet.new(nodes),
      replication_factor: replication_factor,
      node_hashes: node_hashes
    }
  end

  @spec run(%KvStore.LoadBalancer{}) :: %KvStore.LoadBalancer{}
  def run(state) do
    Logger.info("Starting LoadBalancer with state #{inspect(state)}")
    receive do
      {sender, {:get, key}} ->
        {original_node, node} = consistent_hash(key, state)
        send(node, KvStore.GetRequest.new(key, sender, original_node))
        run(state)
      {sender, {:put, key, object, context}} ->
        {original_node, node} = consistent_hash(key, state)
        send(node, KvStore.PutRequest.new(key, object, context, sender, original_node))
        run(state)
      {_, {:node_down, node}} ->
        state = %{state | live_nodes: MapSet.delete(state.live_nodes, node)}
        run(state)
      {_, {:node_up, node}} ->
        state = %{state | live_nodes: MapSet.put(state.live_nodes, node)}
        run(state)
      {sender, :get_live_nodes} ->
        sender |> send(state.live_nodes)
        run(state)
      unknown ->
        Logger.error("LB Unknown message received: #{inspect(unknown)}")
        run(state)
    end
  end

  @spec handle_call(any(), atom(), %KvStore.LoadBalancer{}) :: %KvStore.LoadBalancer{}
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
