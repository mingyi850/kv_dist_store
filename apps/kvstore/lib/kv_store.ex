

defmodule KvStore do

  import Emulation
  import Kernel, except: [send: 2]
  import KvStore.Utils


  import KvStore.PutRequest
  import KvStore.GetRequest
  import KvStore.GetResponse
  import KvStore.PutResponse
  import KvStore.CacheEntry
  import KvStore.InternalGetRequest
  import KvStore.InternalPutRequest

  require Logger

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
    request_responses: %{},
    alternate_data: %{},
    counter: 1
  )
  @moduledoc """
  Documentation for `KvStore`.
  """
alias KvStore.GetResponse

  @spec init([atom()], integer(), integer(), integer()) :: %KvStore{}
  def init(nodes, replication_factor, read_quorum, write_quorum) do
    node_hashes = Enum.map(nodes, fn node -> {node, hash(node)} end) |> Enum.into(%{})
    %KvStore{
      sorted_nodes: sort_nodes(nodes),
      live_nodes: MapSet.new(nodes),
      replication_factor: replication_factor,
      read_quorum: read_quorum,
      write_quorum: write_quorum,
      node_hashes: node_hashes,
      clock: %{},
      data: %{},
      pending_requests: %{},
      alternate_data: %{}
    }
  end


  @spec run(%KvStore{}) :: %KvStore{}
  def run(state) do
    Logger.info("#{inspect(whoami())} Starting KvStore with state #{inspect(state)}")
    state = %{state | clock: update_vector_clock(whoami(), state.clock)}
    receive do
      {_, %KvStore.GetRequest{} = request} ->
        state = handle_get_request(state, request)
        #Logger.info("Current state: #{inspect(state)}")
        run(state)
      {_, %KvStore.PutRequest{} = request} ->
        state = handle_put_request(state, request)
        #Logger.info("Current state: #{inspect(state)}")
        run(state)
      {sender, %KvStore.InternalGetRequest{} = request} ->
        state = handle_get_req_internal(state, request, sender)
        #Logger.info("Current state: #{inspect(state)}")
        run(state)
      {sender, %KvStore.InternalGetResponse{} = response} ->
        state = handle_get_response(state, response, sender)
        #Logger.info("Current state: #{inspect(state)}")
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
    Logger.debug("Handling external get request: #{inspect(request)}")
    internal_request = KvStore.InternalGetRequest.new(request, state.counter)
    state = %{state |
      pending_requests: Map.put(state.pending_requests, state.counter, request),
      request_responses: Map.put(state.request_responses, state.counter, %{}),
      counter: state.counter + 1
    }
    preference_list = get_preference_list(request.key, state, state.replication_factor)
    Logger.debug("Preference list: #{inspect(preference_list)}")
    KvStore.Utils.broadcast(preference_list, internal_request)
    state
  end

  @spec handle_get_req_internal(%KvStore{}, %KvStore.InternalGetRequest{}, atom()) :: %KvStore{}
  def handle_get_req_internal(state, request, sender) do
    Logger.debug("#{whoami()} Handling internal get request: #{inspect(request)}")
    result = Map.get(state.data, request.request.key, nil)
    if result == nil do
      Logger.warning("#{inspect(whoami())} Key not found: #{inspect(request.request.key)}")
      send(sender, KvStore.InternalGetResponse.new(KvStore.GetResponse.new([]), request.index))
    else
      Logger.info("#{inspect(whoami())} Found key: #{inspect(request.request.key)}, with value #{inspect(result)}")
      send(sender, KvStore.InternalGetResponse.new(KvStore.GetResponse.new(result), request.index))
    end
    state
  end

  @spec handle_put_internal(%KvStore{}, %KvStore.InternalPutRequest{}, atom()) :: %KvStore{}
  def handle_put_internal(state, request, sender) do
    existing = Map.get(state.data, request.key, [])
    state = %{state | data: Map.put(state.data, request.key, [KvStore.CacheEntry.new(request.request.object, request.request.context) | existing])} #Might need to change to perform reconciliation to avoid overwritting newer data
    send(sender, KvStore.InternalPutResponse.new(KvStore.PutResponse.new(request.context), request.index)) #Will need to change with new vector clock
    state
  end

  @spec handle_get_response(%KvStore{}, %KvStore.InternalGetResponse{}, atom()) :: %KvStore{}
  def handle_get_response(state, response, sender) do
    request = Map.get(state.pending_requests, response.index, nil)
    if request != nil do
      existing_responses = Map.get(state.request_responses, response.index, %{})
      Logger.debug("#{inspect(whoami())} Received response for request: #{inspect(request)}, currently have #{map_size(existing_responses)} responses.")
      appended_responses = Map.put(existing_responses, sender, response.response)
      state = %{state | request_responses: Map.put(state.request_responses, response.index, appended_responses)}
      state = if map_size(state.request_responses) >= state.read_quorum do
        handle_get_response_quorum(state, request, response.index)
      else state
      end
      state
    else
      Logger.warning("#{inspect(whoami())} Received response for request: #{inspect(response.index)}, but request not found.")
      state
    end

  end


  @spec handle_put_request(%KvStore{}, %KvStore.PutRequest{}) :: %KvStore{}
  def handle_put_request(state, request) do
    request = update_request_clock(state, request)
    existing = Map.get(state.data, request.key, [])
    state = if existing == [] do
      %{state | data: Map.put(state.data, request.key, [KvStore.CacheEntry.new(request.object, request.context)])}
    else
      new_data = [KvStore.CacheEntry.new(request.object, request.context) | existing]
      |> get_latest_entries()
      %{state | data: Map.put(state.data, request.key, new_data)}
    end
    send(request.sender, KvStore.PutResponse.new(request.context))
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

  @spec handle_get_response_quorum(%KvStore{}, %KvStore.GetRequest{}, integer()) :: %KvStore{}
  def handle_get_response_quorum(state, request, index) do
    Logger.debug("#{inspect(whoami())} Handling get response quorum for request: #{inspect(request)}")
    responses = Map.get(state.request_responses, index, %{})
    combined_response = get_updated_responses(responses)
    stale_nodes = get_stale_nodes(responses, combined_response)
    # Should send stale_nodes ReadRepairRequest with combined_response, then delete records
    send(request.sender, combined_response)
    %{state |
      request_responses: Map.delete(state.request_responses, index),
      pending_requests: Map.delete(state.pending_requests, index)}
  end


  # Identify nodes which returned stale responses and need to be updated based on the newly constructed request
  @spec get_stale_nodes(%{atom() => %GetResponse{}}, %GetResponse{}) :: [atom()]
  def get_stale_nodes(responses, new_response) do
    Enum.filter(responses, fn {node, response} -> is_response_stale(response, new_response) end)
    |> Enum.map(fn {node, _} -> node end)
  end


  #Checks for the original response, if any objects are before any of the new objects
  @spec is_response_stale(%GetResponse{}, %GetResponse{}) :: boolean()
  def is_response_stale(response, combined_response) do
    Enum.any?(response.objects, fn obj ->
      Enum.any?(combined_response.objects, fn new_obj -> compare_contexts(obj.context, new_obj.context) == :before  end)
    end)
  end
  #Compares all the responses for a given key and constructs a list of the most recent ones
  @spec get_updated_responses(%{atom() => %GetResponse{}}) :: %GetResponse{}
  defp get_updated_responses(responses) do
    #Get the most recent response
    Logger.info("Getting updated responses for responses: #{inspect(responses)}")
    cache_entries = Enum.flat_map(responses, fn {_, response} -> response.objects end)
    latest_entries = get_latest_entries(cache_entries)
    Logger.info("Latest entries: #{inspect(latest_entries)}")
    KvStore.GetResponse.new(latest_entries)
  end

  #Iterates through a list of cache_entries and returns the most recent cache_entries
  @spec get_latest_entries([%KvStore.CacheEntry{}]) :: [%KvStore.CacheEntry{}]
  defp get_latest_entries(cache_entries) do
    to_exclude = get_exclude_list(cache_entries, cache_entries, MapSet.new())
    Enum.filter(cache_entries, fn entry -> !MapSet.member?(to_exclude, entry.object) end)
  end

  @spec get_exclude_list([%KvStore.CacheEntry{}], [%KvStore.CacheEntry{}], MapSet.t()) :: MapSet.t()
  defp get_exclude_list(remaining_entries, all_entries, exclude) do
    if Enum.empty?(remaining_entries) do
      exclude
    else
      [head | remaining] = remaining_entries
      # if any context is before new context, add context to exclude list
      to_exclude = Enum.filter(all_entries, fn entry -> KvStore.Utils.compare_contexts(entry.context, head.context) == :before end)
      # if any context is after current context, add current to exclude list
      exclude_list = if Enum.any?(all_entries, fn entry -> KvStore.Utils.compare_contexts(entry.context, head.context) == :after end) do
        MapSet.put(exclude, head.object)
      else
        exclude
      end
      exclude_list = MapSet.union(exclude_list, MapSet.new(to_exclude))
      get_exclude_list(remaining, all_entries, exclude_list)
    end
  end



end
