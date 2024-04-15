defmodule KvStore do

  import KvStore.Utils
  import KvStore.PutRequest
  import KvStore.GetRequest
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
      live_nodes: Enum.into(nodes, %{}),
      replication_factor: replication_factor,
      node_hashes: node_hashes,
      clock: 0,
      data: %{}
    }
  end

  @spec receive(%KvStore{}) :: %KvStore{}
  def receive(state) do
    receive do
      {_, %KvStore.GetRequest{} = request} ->
        handle_get_request(state, request)
      {_, %KvStore.PutRequest{} = request} ->
        handle_put_request(state, request)
      {_, {:node_down, node}} ->
        state = %{state | live_nodes: MapSet.delete(state.live_nodes, node)}
        state
      {_, {:node_up, node}} ->
        state = %{state | live_nodes: MapSet.put(state.live_nodes, node)}
        state
    end
  end

  @spec handle_get_request(%KvStore{}, %KvStore.GetRequest{}) :: %KvStore{}
  def handle_get_request(state, request) do
    result = Map.get(state.data, request.key)
    request.sender.send(result)
    state
  end

  @spec handle_put_request(%KvStore{}, %KvStore.PutRequest{}) :: %KvStore{}
  def handle_put_request(state, request) do
    state = %{state | data: Map.put(state.data, request.key, request.object)}
    request.sender.send(:ok)
    state
  end
end
