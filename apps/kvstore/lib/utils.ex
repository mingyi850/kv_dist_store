defmodule KvStore.Utils do
  require Logger

  import Kernel, except: [send: 2]
  import Emulation, only: [send: 2]

  @spec hash(any()) :: integer()
  def hash(value) do
    strValue = to_string(value)
    :crypto.hash(:sha256, strValue)
    |> :binary.decode_unsigned()
  end

  @spec sort_nodes([atom()]) :: [atom()]
  def sort_nodes(nodes) do
    Enum.sort_by(nodes, fn node -> hash(node) end)
  end

  @spec get_next_live_node(integer(), %{sorted_nodes: [atom()], live_nodes: MapSet.t(atom())}, integer()) :: {atom(), integer()}
  def get_next_live_node(index, state, count) do
    #Logger.debug("Sorted nodes is #{inspect(state.sorted_nodes)}")
    #Logger.debug("Index is #{index}")
    #Logger.debug("Count is #{count}")
    #Logger.debug("Live nodes is #{inspect(state.live_nodes)}")
    if count == 0 do
      {nil, index}
    else
      next_index = rem(index + 1, length(state.sorted_nodes))
      next_node = Enum.at(state.sorted_nodes, next_index)
      #Logger.debug("Next node is #{next_node}, with Index #{next_index}")
      # Check if next node is valid, otherwise, return the following node
      if MapSet.member?(state.live_nodes, next_node) do
        {next_node, next_index}
      else
        get_next_live_node(next_index, state, count - 1)
      end
    end
  end

  @spec get_next_live_nodes(integer(), %{sorted_nodes: [atom()], live_nodes: MapSet.t(atom())}, integer(), [atom()]) :: [atom()]
  def get_next_live_nodes(index, state, num_nodes, accum) do
    if num_nodes == 0 do
      Enum.reverse(accum)
    else
      {next_node, next_index} = get_next_live_node(index, state, length(state.sorted_nodes) - 1)
      if next_node == nil or Enum.member?(accum, next_node) do
        Enum.reverse(accum)
      else
        get_next_live_nodes(next_index, state, num_nodes - 1, [next_node | accum])
      end
    end
  end

  @spec consistent_hash(any(), %{sorted_nodes: [atom()], node_hashes: map(), live_nodes: MapSet.t(atom())}) ::
            {any(), any()}
  def consistent_hash(key, state) do
    key_hash = hash(key)
    sorted_nodes = state.sorted_nodes
    # Show all nodes with higher hash value
    original_node = Enum.find(sorted_nodes, fn node -> (state.node_hashes[node] >= key_hash) end) || hd(sorted_nodes)
    node = Enum.find(sorted_nodes, fn node -> ((state.node_hashes[node] >= key_hash) && MapSet.member?(state.live_nodes, node)) end) || get_first_live_node(state)
    #Logger.debug("Original node is #{original_node}, Node is #{node}")
    {original_node, node}
  end

  @spec get_first_live_node(%{sorted_nodes: [atom()], live_nodes: MapSet.t(atom())}) :: atom()
  def get_first_live_node(state) do
    Enum.find(state.sorted_nodes, fn node -> MapSet.member?(state.live_nodes, node) end)
  end

  @spec get_first_responsible_node(atom(), %{sorted_nodes: [atom()], live_nodes: MapSet.t(atom()), replication_factor: integer()}) :: atom()
  def get_first_responsible_node(node, state) do
    preference_list = get_preference_list(node, state, state.replication_factor)
    if preference_list != [] do
      hd(preference_list)
    else
      nil
    end
  end

  def get_second_responsible_node(node, state) do
    preference_list = Enum.drop(get_preference_list(node, state, 2), 1)
    if preference_list != [] do
      hd(preference_list)
    else
      nil
    end
  end

  @spec get_preference_list(atom(), %{sorted_nodes: [atom()], live_nodes: MapSet.t(atom())}, integer()) :: [atom()]
  def get_preference_list(key, state, num_nodes) do
    {_, node} = consistent_hash(key, state)
    get_next_live_nodes(Enum.find_index(state.sorted_nodes, fn n -> n == node end), state, num_nodes - 1, [node])
  end

  #Returns the nodes a node holds replicas for
  @spec get_responsible_range(atom(), %{sorted_nodes: [atom()], replication_factor: integer()}) :: MapSet.t(atom())
  def get_responsible_range(node, state) do
    node_index = Enum.find_index(state.sorted_nodes, fn n -> n == node end)
    node_list = Enum.map((state.replication_factor - 1 .. 0), fn i ->
      index = rem(node_index - i, length(state.sorted_nodes))
      Enum.at(state.sorted_nodes, index)
    end)
    MapSet.new(node_list)
  end

  @spec get_previous_node(atom(), %{sorted_nodes: [atom()]}) :: atom()
  def get_previous_node(node, state) do
    node_index = Enum.find_index(state.sorted_nodes, fn n -> n == node end)
    previous_index = rem(node_index - 1, length(state.sorted_nodes))
    Enum.at(state.sorted_nodes, previous_index)
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
    if received == nil do
      current
    else
      Map.merge(current, received, fn _k, c, r -> combine_component(c, r) end)
    end
  end

  @doc """
  Combine a list of vector clocks
  """
  @spec combine_vector_clocks([map()]) :: map()
  def combine_vector_clocks([]), do: %{}
  def combine_vector_clocks([clock | clocks]) do
    combine_vector_clocks(clock, combine_vector_clocks(clocks))
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
  # Return :before if a vector of the form [c1] happens before [c2].
  # Return :after if a vector of the form [c2] happens before [c1].
  # Return :concurrent if neither of the above two are true.
  @spec compare_component(
          non_neg_integer(),
          non_neg_integer()
        ) :: :before | :after | :concurrent
  defp compare_component(c1, c2) do
    # TODO: Compare c1 and c2.
    cond do
    c1 == c2 -> :concurrent
    c1 < c2 -> :before
    c1 > c2 -> :after
    end
  end

  @doc """
  Compare two vector clocks v1 and v2.
  Returns :before if v1 happened before v2.
  Returns :after if v2 happened before v1.
  Returns :concurrent if neither of the above hold.
  """
  @spec compare_vectors(map(), map()) :: :before | :after | :concurrent
  def compare_vectors(v1, v2) do
    # First make the vectors equal length.
    v1 = make_vectors_equal_length(v1, v2)
    v2 = make_vectors_equal_length(v2, v1)
    compare_result =
      Map.values(
        Map.merge(v1, v2, fn _k, c1, c2 -> compare_component(c1, c2) end)
      )

    cond do
      Enum.all?(compare_result, fn x -> x == :concurrent end) -> :concurrent
      Enum.all?(compare_result, fn x -> (x == :after || x == :concurrent)  end) -> :after
      Enum.all?(compare_result, fn x -> (x == :before || x == :concurrent) end) -> :before
      true -> :concurrent
    end
  end

  @spec compare_contexts(%KvStore.Context{}, %KvStore.Context{}) :: :before | :after | :concurrent
  def compare_contexts(context1, context2) do
    compare_vectors(context1.vector_clock, context2.vector_clock)
  end

  @spec broadcast([pid()], any()) :: :ok
  def broadcast(pids, message) do
    Enum.each(pids, fn pid -> send(pid, message) end)
    :ok
  end

end
