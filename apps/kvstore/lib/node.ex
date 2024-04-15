defmodule Node do

  import Kvstore.Utils
  defstruct(
    sorted_nodes: [],
    live_nodes: {},
    replication_factor: 1,
    node_hashes: %{},
    clock: 0,
    data: %{}
  )
  @moduledoc """
  Documentation for `Node`.
  """

  @spec init([atom()], integer()) :: %Node{}
  def init(nodes, replication_factor) do
    node_hashes = Enum.map(nodes, fn node -> {node, hash(node)} end) |> Enum.into(%{})
    %Node{
      sorted_nodes: Enum.sort_by(nodes, fn node -> node_hashes[node] end),
      live_nodes: Enum.into(nodes, %{}),
      replication_factor: replication_factor,
      node_hashes: node_hashes,
      clock: 0,
      data: %{}
    }
  end
  def hello do
    :world
  end
end
