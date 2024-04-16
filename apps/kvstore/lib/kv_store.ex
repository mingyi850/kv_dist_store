

defmodule KvStore do

  import Emulation
  import Kernel, except: [send: 2]
  import KvStore.Utils


  import KvStore.PutRequest
  import KvStore.GetRequest
  import KvStore.GetResponse
  import KvStore.PutResponse
  import KvStore.CacheEntry

  defstruct(
    sorted_nodes: [],
    live_nodes: {},
    replication_factor: 1,
    node_hashes: %{},
    clock: 0,
    data: %{}
  )
  @moduledoc """
  Documentation for `KvStore`.
  """

  @spec init([atom()], integer()) :: %KvStore{}
  def init(nodes, replication_factor) do
    node_hashes = Enum.map(nodes, fn node -> {node, hash(node)} end) |> Enum.into(%{})
    %KvStore{
      sorted_nodes: Enum.sort_by(nodes, fn node -> node_hashes[node] end),
      live_nodes: MapSet.new(nodes),
      replication_factor: replication_factor,
      node_hashes: node_hashes,
      clock: 0,
      data: %{}
    }
  end


  @spec run(%KvStore{}) :: %KvStore{}
  def run(state) do
    receive do
      {_, %KvStore.GetRequest{} = request} ->
        state = handle_get_request(state, request)
        run(state)
      {_, %KvStore.PutRequest{} = request} ->
        state = handle_put_request(state, request)
        run(state)
      {_, {:node_down, node}} ->
        state = %{state | live_nodes: MapSet.delete(state.live_nodes, node)}
        run(state)
      {_, {:node_up, node}} ->
        state = %{state | live_nodes: MapSet.put(state.live_nodes, node)}
        run(state)
    end
  end

  @spec handle_get_request(%KvStore{}, %KvStore.GetRequest{}) :: %KvStore{}
  def handle_get_request(state, request) do
    result = Map.get(state.data, request.key, nil)
    if result == nil do
      send(request.sender, KvStore.GetResponse.new(KvStore.CacheEntry.new(nil, nil)))
    else
      send(request.sender, KvStore.GetResponse.new(result))
    end
    state
  end

  @spec handle_put_request(%KvStore{}, %KvStore.PutRequest{}) :: %KvStore{}
  def handle_put_request(state, request) do
    request = handle_nil_context(state, request)
    state = %{state | data: Map.put(state.data, request.key, KvStore.CacheEntry.new(request.object, request.context))}
    send(request.sender, KvStore.PutResponse.new(request.context)) #Will need to change with new vector clock
    state
  end

  defp handle_nil_context(state, request) do
    if request.context != nil do
      request
    else
      new_context = KvStore.Context.new()
      new_context = %{new_context | vector_clock: Map.put(new_context.vector_clock, whoami(), state.clock)}
      request = %{request | context: new_context}
      request
    end
  end
end
