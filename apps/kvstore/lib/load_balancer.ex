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
    node_hashes: %{},
    req_id: 0,
    observer: nil
  )

  @spec init([atom()], integer(), atom()) :: %KvStore.LoadBalancer{}
  def init(nodes, replication_factor, observer) do
    Logger.info("Initializing LoadBalancer with nodes: #{inspect(nodes)}")
    node_hashes = Enum.map(nodes, fn node -> {node, hash(node)} end) |> Enum.into(%{})
    %KvStore.LoadBalancer{
      sorted_nodes: sort_nodes(nodes),
      live_nodes: MapSet.new(nodes),
      replication_factor: replication_factor,
      node_hashes: node_hashes,
      req_id: 0,
      observer: observer
    }
  end

  @spec run(%KvStore.LoadBalancer{}) :: %KvStore.LoadBalancer{}
  def run(state) do
    receive do
      {sender, {:get, key}} ->
        Logger.info("lb receive get from #{inspect(sender)} (#{state.req_id})")
        {original_node, _} = consistent_hash(key, state)
        preference_list = get_preference_list(key, state, state.replication_factor)
        node = Enum.random(preference_list)
        #TODO: Redirect messages to any node in the preference list instead of the first node.
        send(node, KvStore.GetRequest.new(key, sender, original_node, state.req_id))
        # send actual get_timestamp to observer
        send(state.observer, {:get_start_time, state.req_id, :os.system_time(:millisecond)})
        run(%{state | req_id: state.req_id + 1})
      {sender, {:put, key, object, context}} ->
        Logger.info("lb receive put from #{inspect(sender)}")
        {original_node, _} = consistent_hash(key, state)
        preference_list = get_preference_list(key, state, state.replication_factor)
        node = Enum.random(preference_list)
        #TODO: Redirect messages to any node in the preference list instead of the first node.
        request = KvStore.PutRequest.new(key, object, context, sender, original_node, state.req_id)
        send(node, request)
        send(state.observer, KvStore.PutRequestLog.new(request.req_id, request.key, request.object, %{}, :os.system_time(:millisecond), 0, whoami()))
        run(%{state | req_id: state.req_id + 1})
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
end
