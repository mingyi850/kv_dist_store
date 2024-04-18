

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
  import KvStore.FailedResponse

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
    read_repairs: %{},
    alternate_data: %{},
    timers: %{},
    counter: 1,
    internal_timeout: 100,
    max_retries: 3
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
      request_responses: %{},
      read_repairs: %{},
      alternate_data: %{},
      timers: %{},
      counter: 1,
      internal_timeout: 100,
      max_retries: 3
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
      {sender, %KvStore.InternalPutRequest{} = request} ->
        state = handle_put_req_internal(state, request, sender)
        #Logger.info("Current state: #{inspect(state)}")
        run(state)
      {sender, %KvStore.InternalPutResponse{} = response} ->
        state = handle_put_response(state, response, sender)
        #Logger.info("Current state: #{inspect(state)}")
        run(state)
      {_, {:timeout, index, retries}} ->
        state = handle_timeout(state, index, retries)
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
    timer = Emulation.timer(state.internal_timeout, {:timeout, state.counter, 1})
    state = %{state |
      pending_requests: Map.put(state.pending_requests, state.counter, internal_request),
      request_responses: Map.put(state.request_responses, state.counter, %{}),
      timers: Map.put(state.timers, state.counter, timer),
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

  @spec handle_get_response(%KvStore{}, %KvStore.InternalGetResponse{}, atom()) :: %KvStore{}
  def handle_get_response(state, response, sender) do
    request = Map.get(state.pending_requests, response.index, nil)
    if request != nil do
      existing_responses = Map.get(state.request_responses, response.index, %{})
      Logger.debug("#{inspect(whoami())} Received response for request: #{inspect(request)}, currently have #{map_size(existing_responses)} responses.")
      appended_responses = Map.put(existing_responses, sender, response.response)
      state = %{state | request_responses: Map.put(state.request_responses, response.index, appended_responses)}
      response_count = map_size(appended_responses)
      cond do
        response_count == state.read_quorum ->
          handle_get_response_quorum(state, request, response.index)
        response_count > state.read_quorum ->
          combined_response = Map.get(state.read_repairs, response.index, nil)
          if combined_response != nil  and is_response_stale(response.response, combined_response) do
            # TODO: Construct read repair response and send to stale node
            # send(sender, read_repair_response)
          end
          if response_count == state.replication_factor do
            remove_request(state, response.index)
          else
            state
          end
      end
    else
      Logger.warning("#{inspect(whoami())} Received response for request: #{inspect(response.index)}, but request not found.")
      state
    end
  end

  @spec handle_put_request(%KvStore{}, %KvStore.PutRequest{}) :: %KvStore{}
  def handle_put_request(state, request) do
    updated_context = get_updated_context(state, request)
    preference_list = get_preference_list(request.key, state, state.replication_factor)
    internal_request = KvStore.InternalPutRequest.new(request, updated_context, state.counter)
    timer = Emulation.timer(state.internal_timeout, {:timeout, state.counter, 1})
    KvStore.Utils.broadcast(preference_list, internal_request)
    %{state |
      pending_requests: Map.put(state.pending_requests, state.counter, internal_request),
      request_responses: Map.put(state.request_responses, state.counter, %{}),
      timers: Map.put(state.timers, state.counter, timer),
      counter: state.counter + 1
    }
  end

  @spec handle_put_req_internal(%KvStore{}, %KvStore.InternalPutRequest{}, atom()) :: %KvStore{}
  def handle_put_req_internal(state, request, sender) do
    client_request = request.request
    existing = Map.get(state.data, client_request.key, [])
    state = if existing == [] do
      %{state | data: Map.put(state.data, client_request.key, [KvStore.CacheEntry.new(client_request.object, request.context)])}
    else
      new_data = [KvStore.CacheEntry.new(client_request.object, request.context) | existing]
      |> get_latest_entries()
      %{state | data: Map.put(state.data, client_request.key, new_data)}
    end
    send(sender, KvStore.InternalPutResponse.new(KvStore.PutResponse.new(request.context), request.index))
    state
  end

  @spec handle_put_response(%KvStore{}, %KvStore.InternalPutResponse{}, atom()) :: %KvStore{}
  def handle_put_response(state, response, sender) do
    request = Map.get(state.pending_requests, response.index, nil)
    if request != nil do
      existing_responses = Map.get(state.request_responses, response.index, %{})
      Logger.debug("#{inspect(whoami())} Received response for request: #{inspect(request)}, currently have #{map_size(existing_responses)} responses.")
      appended_responses = Map.put(existing_responses, sender, response.response)
      state = %{state | request_responses: Map.put(state.request_responses, response.index, appended_responses)}
      cond do
        map_size(appended_responses) == state.write_quorum ->
          handle_put_response_quorum(state, request, response.index)
        map_size(appended_responses) == state.replication_factor ->
          remove_request(state, response.index)
        true ->
          state
      end
    else
      Logger.warning("#{inspect(whoami())} Received response for request: #{inspect(response.index)}, but request not found.")
      state
    end
  end

  @spec handle_put_response_quorum(%KvStore{}, %KvStore.InternalPutRequest{}, integer()) :: %KvStore{}
  def handle_put_response_quorum(state, request, index) do
    Logger.debug("#{inspect(whoami())} Handling put response quorum for request: #{inspect(request)}")
    responses = Map.get(state.request_responses, index, %{})
    Logger.debug("Responses for request: #{inspect(responses)}")
    context = hd(Map.values(responses)).context
    send(request.request.sender, KvStore.PutResponse.new(context))
    %{state |
      request_responses: Map.delete(state.request_responses, index),
      pending_requests: Map.delete(state.pending_requests, index),
    }
  end

  #Combines current clock with node's clock to get latest clock
  defp get_updated_context(state, request) do
    if request.contexts != [] do
      KvStore.Context.new(combine_vector_clocks([state.clock | Enum.map(request.contexts, fn ctx -> ctx.vector_clock end)]))
    else
      KvStore.Context.new(state.clock)
    end
  end

  @spec handle_get_response_quorum(%KvStore{}, %KvStore.InternalGetRequest{}, integer()) :: %KvStore{}
  def handle_get_response_quorum(state, request, index) do
    Logger.debug("#{inspect(whoami())} Handling get response quorum for request: #{inspect(request)}")
    responses = Map.get(state.request_responses, index, %{})
    combined_response = get_updated_responses(responses)
    stale_nodes = get_stale_nodes(responses, combined_response)
    # TODO: Should send stale_nodes ReadRepairRequest with combined_response
    send(request.request.sender, combined_response)
    %{state |
      request_responses: Map.delete(state.request_responses, index),
      pending_requests: Map.delete(state.pending_requests, index),
      read_repairs: Map.put(state.read_repairs, index, combined_response)}
  end

  @spec handle_get_response_complete(%KvStore{},  integer()) :: %KvStore{}
  def handle_get_response_complete(state, index) do

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

  @spec handle_timeout(%KvStore{}, integer(), integer()) :: %KvStore{}
  def handle_timeout(state, index, retries) do
    Logger.warning("#{inspect(whoami())} Timeout for request: #{inspect(index)}")
    request = Map.get(state.pending_requests, index, nil)
    if request != nil do
      if retries >= state.max_retries do
        Logger.warning("#{inspect(whoami())} Timeout for request: #{inspect(index)}, max retries reached.")
        send(request.reqeust.sender, KvStore.FailedResponse.new(request.request))
        state = remove_request(state, index)
        state
      else
        responded_nodes = MapSet.new(Map.keys(Map.get(state.request_responses, index, %{}))) # Get a set of all nodes that have responded to this request
        priority_list = get_preference_list(request.request.key, state, state.replication_factor)
        remaining_nodes = priority_list |> Enum.filter(fn node -> !MapSet.member?(responded_nodes, node) end)
        broadcast(remaining_nodes, request)
        timer = Emulation.timer(state.internal_timeout, {:timeout, index, retries + 1})
        %{state | timers: Map.put(state.timers, index, timer)}
      end
    else
      Logger.warning("#{inspect(whoami())} Timeout for request: #{inspect(index)}, but request not found.")
      state
    end
  end

  @spec remove_request(%KvStore{}, integer()) :: %KvStore{}
  def remove_request(state, index) do
    timer = Map.get(state.timers, index, nil)
    if timer != nil do
      Emulation.cancel_timer(timer)
    end
    %{state |
      pending_requests: Map.delete(state.pending_requests, index),
      request_responses: Map.delete(state.request_responses, index),
      read_repairs: Map.delete(state.read_repairs, index),
      timers: Map.delete(state.timers, index)
    }
  end



end
