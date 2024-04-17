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


  # Vector clock utils
  # Combine a single component in a vector clock.
  @spec combine_component(
          non_neg_integer(),
          non_neg_integer()
        ) :: non_neg_integer()
  defp combine_component(current, received) do
    max(current, received)
  end

   @doc """
  Combine vector clocks: this is called whenever a
  message is received, Returns the clock
  from combining the two.
  """
  @spec combine_vector_clocks(map(), map()) :: map()
  def combine_vector_clocks(current, received) do
    Map.merge(current, received, fn _k, c, r -> combine_component(c, r) end)
  end

   @doc """
  This function is called by the process `proc` whenever an
  event occurs.
  """
  @spec update_vector_clock(atom(), map()) :: map()
  def update_vector_clock(proc, clock) do
    Map.update(clock, proc, 1, fn existing -> existing + 1 end)
  end

   # Produce a new vector clock that is a copy of v1,
  # except for any keys (processes) that appear only
  # in v2, which we add with a 0 value.
  @spec make_vectors_equal_length(map(), map()) :: map()
  defp make_vectors_equal_length(v1, v2) do
    v1_add = for {k, _} <- v2, !Map.has_key?(v1, k), do: {k, 0}
    Map.merge(v1, Enum.into(v1_add, %{}))
  end

  # Compare two components of a vector clock c1 and c2.
  # Return @before if a vector of the form [c1] happens before [c2].
  # Return @after if a vector of the form [c2] happens before [c1].
  # Return @concurrent if neither of the above two are true.
  @spec compare_component(
          non_neg_integer(),
          non_neg_integer()
        ) :: :before | :after | :concurrent
  defp compare_component(c1, c2) do
    # TODO: Compare c1 and c2.
    cond do
    c1 == c2 -> @concurrent
    c1 < c2 -> @before
    c1 > c2 -> @hafter
    end
  end

  @doc """
  Compare two vector clocks v1 and v2.
  Returns @before if v1 happened before v2.
  Returns @hafter if v2 happened before v1.
  Returns @concurrent if neither of the above hold.
  """
  @spec compare_vectors(map(), map()) :: :before | :after | :concurrent
  def compare_vectors(v1, v2) do
    # First make the vectors equal length.
    v1 = make_vectors_equal_length(v1, v2)
    v2 = make_vectors_equal_length(v2, v1)
    # `compare_result` is a list of elements from
    # calling `compare_component` on each component of
    # `v1` and `v2`. Given this list you need to figure
    # out whether
    compare_result =
      Map.values(
        Map.merge(v1, v2, fn _k, c1, c2 -> compare_component(c1, c2) end)
      )

    cond do
      Enum.all?(compare_result, fn x -> x == @concurrent end) -> @concurrent
      Enum.all?(compare_result, fn x -> (x == @hafter || x == @concurrent)  end) -> @hafter
      Enum.all?(compare_result, fn x -> (x == @before || x == @concurrent) end) -> @before
      true -> @concurrent
    end
  end
end
