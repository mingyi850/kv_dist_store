

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
    read_quorum: 1,
    write_quorum: 1,
    node_hashes: %{},
    clock: %{},
    data: %{},
    pending_requests: %{},
    alternate_data: %{}
  )
  @moduledoc """
  Documentation for `KvStore`.
  """

  @spec init([atom()], integer(), integer(), integer()) :: %KvStore{}
  def init(nodes, replication_factor, read_quorum, write_quorum) do
    node_hashes = Enum.map(nodes, fn node -> {node, hash(node)} end) |> Enum.into(%{})
    %KvStore{
      sorted_nodes: Enum.sort_by(nodes, fn node -> node_hashes[node] end),
      live_nodes: MapSet.new(nodes),
      replication_factor: replication_factor,
      read_quorum: read_quorum,
      write_quorum: write_quorum,
      node_hashes: node_hashes,
      clock: %{whoami() => 0},
      data: %{},
      pending_requests: %{},
      alternate_data: %{}
    }
  end


  @spec run(%KvStore{}) :: %KvStore{}
  def run(state) do
    state = %{state | clock: update_vector_clock(whoami(), state.clock)}
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
    request = update_request_clock(state, request)
    state = %{state | data: Map.put(state.data, request.key, KvStore.CacheEntry.new(request.object, request.context))}
    send(request.sender, KvStore.PutResponse.new(request.context)) #Will need to change with new vector clock
    state
  end

  defp update_request_clock(state, request) do
    if request.context != nil do
      %{request | context: combine_vector_clocks(state.clock, request.context.vector_clock)}
    else
      new_context = KvStore.Context.new()
      new_context = %{new_context | vector_clock: state.clock}
      request = %{request | context: new_context}
      request
    end
  end

end
