

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
    merkle_trees: %{},
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
    internal_timeout: 300,
    max_retries: 3,
    heartbeat_counter: 1,
    peer_heartbeats: %{},
    heartbeat_frequency: 500,
    observer: nil
  )
  @moduledoc """
  Documentation for `KvStore`.
  """
alias KvStore.FixedMerkleTree
alias KvStore.GetResponse

  @spec init([atom()], integer(), integer(), integer(), pid()) :: %KvStore{}
  def init(nodes, replication_factor, read_quorum, write_quorum, observer) do
    node_hashes = Enum.map(nodes, fn node -> {node, hash(node)} end) |> Enum.into(%{})
    sorted_nodes = sort_nodes(nodes)
    %KvStore{
      sorted_nodes: sort_nodes(nodes),
      live_nodes: MapSet.new(nodes),
      merkle_trees: Map.new(nodes, fn node -> {node, build_merkle_tree(node, sorted_nodes)} end),
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
      internal_timeout: 300,
      max_retries: 3,
      heartbeat_counter: 1,
      peer_heartbeats: %{},
      heartbeat_frequency: 500,
      observer: observer
    }
  end


  @spec run(%KvStore{}) :: %KvStore{}
  def run(state) do
    #Logger.info("#{inspect(whoami())} KvStore with state #{inspect(state)}")
    state = %{state | clock: update_vector_clock(whoami(), state.clock)}
    |> start_heartbeat_timer()
    receive do
      {_, %KvStore.GetRequest{} = request} ->
        state = handle_get_request(state, request)
        #Logger.info("Current state: #{inspect(state)}")
        run(state)
      {_, %KvStore.PutRequest{} = request} ->
        state = handle_put_request(state, request)
        # Logger.info("Current state: #{inspect(state)}")
        Logger.debug("PutRequest: #{inspect(request)}")
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
      {_, %KvStore.ReadRepairRequest{} = request} ->
        state = handle_read_repair(state, request)
        #Logger.info("Current state: #{inspect(state)}")
        run(state)
      {sender, %KvStore.SyncMerkleTree{} = sync_request} ->
        state = handle_merkle_tree_sync(state, sync_request.headers, sync_request.to_sync, sender)
        run(state)
      {sender, %KvStore.UnsyncedMerkleTree{} = unsync_response} ->
        state = handle_unsynced_merkle_tree(state, unsync_response.header, unsync_response.to_sync, sender)
        run(state)
      {_, %KvStore.MerkleSyncComplete{} = sync_complete} ->
        state = handle_merkle_sync_complete(state, sync_complete.to_sync)
        run(state)
      {:timeout, index, retries} ->
        state = handle_timeout(state, index, retries)
        run(state)
      {_, :node_down} ->
        Logger.warning("#{inspect(whoami())} Node is down")
        down(state)
      {sender, {:heartbeat, new_heartbeats, req_resp}} ->
        state = handle_heartbeat(state, new_heartbeats, sender, req_resp)
        run(state)
      {_, :heartbeat_now} ->
        Logger.debug("#{inspect(whoami())} Got heartbeat_now command")
        state = send_heartbeat(state)
        timer = Emulation.timer(state.heartbeat_frequency, {whoami(), :heartbeat_now})
        state = %{state | timers: Map.put(state.timers, :heartbeat, timer)}
        run(state)
      # Debugging functions
      {sender, :get_state} ->
        send(sender, state)
        run(state)
      {sender, {:get_value_at, key}} ->
        Logger.info("Got request for value at key: #{inspect(key)}")
        send(sender, Map.get(state.data, key, nil))
        run(state)
      anything ->
        Logger.error("#{inspect(whoami())} Unknown message received: #{inspect(anything)}")
        run(state)
    end
  end

  def down(state) do
    receive do
      {_, :node_up} ->
        Logger.info("#{inspect(whoami())} Node is up")
        run(state)
      {_, :heartbeat_now} ->
        # Reset heartbeat timer and remain down
        Logger.warning("#{inspect(whoami())} Got heartbeat_now command while down")
        timer = Emulation.timer(state.heartbeat_frequency, {whoami(), :heartbeat_now})
        state = %{state | timers: Map.put(state.timers, :heartbeat, timer)}
        down(state)
      {_, {:heartbeat, _, _}} ->
        Logger.warning("#{inspect(whoami())} Got heartbeat command while down")
        down(state)
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
    # Logger.debug("Preference list: #{inspect(preference_list)}")
    KvStore.Utils.broadcast(preference_list, internal_request)
    state
  end

  @spec handle_get_req_internal(%KvStore{}, %KvStore.InternalGetRequest{}, atom()) :: %KvStore{}
  def handle_get_req_internal(state, request, sender) do
    # Logger.debug("#{whoami()} Handling internal get request: #{inspect(request)}")
    result = Map.get(state.data, request.request.key, nil)
    if result == nil do
      Logger.warning("#{inspect(whoami())} Key not found: #{inspect(request.request.key)}")
      send(sender, KvStore.InternalGetResponse.new(KvStore.GetResponse.new([], request.request.req_id), request.index))
    else
      Logger.info("#{inspect(whoami())} Found key: #{inspect(request.request.key)}, with value #{inspect(result)}")
      send(sender, KvStore.InternalGetResponse.new(KvStore.GetResponse.new(result, request.request.req_id), request.index))
    end
    state
  end

  @spec handle_get_response(%KvStore{}, %KvStore.InternalGetResponse{}, atom()) :: %KvStore{}
  def handle_get_response(state, response, sender) do
    request = Map.get(state.pending_requests, response.index, nil)
    if request != nil do
      existing_responses = Map.get(state.request_responses, response.index, %{})
      # Logger.debug("#{inspect(whoami())} Received response for request: #{inspect(request)}, currently have #{map_size(existing_responses)} responses.")
      appended_responses = Map.put(existing_responses, sender, response.response)
      state = %{state | request_responses: Map.put(state.request_responses, response.index, appended_responses)}
      response_count = map_size(appended_responses)
      handle_get_response_quorum(state, request, response.index, response_count)
      |> handle_send_read_repair(response, response_count, sender)
      |> handle_all_response_complete(response.index, response_count)
    else
      Logger.warning("#{inspect(whoami())} Received response for request: #{inspect(response.index)}, but request not found.")
      state
    end
  end

  @spec handle_put_request(%KvStore{}, %KvStore.PutRequest{}) :: %KvStore{}
  def handle_put_request(state, request) do
    existing = Map.get(state.data, request.key, [])
    updated_context = get_updated_context(state, request, existing)
    state = %{state | clock: updated_context.vector_clock}
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
    new_objects = [KvStore.CacheEntry.new(client_request.object, request.context)]
    Logger.info("New objects are #{inspect(new_objects)}")
    state = update_data(state, client_request.key, new_objects)
    send(sender, KvStore.InternalPutResponse.new(KvStore.PutResponse.new(request.context, request.request.req_id), request.index))
    state
  end

  @spec handle_put_response(%KvStore{}, %KvStore.InternalPutResponse{}, atom()) :: %KvStore{}
  def handle_put_response(state, response, sender) do
    request = Map.get(state.pending_requests, response.index, nil)
    if request != nil do
      existing_responses = Map.get(state.request_responses, response.index, %{})
      # Logger.debug("#{inspect(whoami())} Received response for request: #{inspect(request)}, currently have #{map_size(existing_responses)} responses.")
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
    # Logger.debug("Responses for request: #{inspect(responses)}")
    context = hd(Map.values(responses)).context
    send(request.request.sender, KvStore.PutResponse.new(context, request.request.req_id))

    # send log to observer
    send(state.observer, KvStore.PutRequestLog.new(request.request.req_id, request.request.key, request.request.object, request.context, 0, :os.system_time(:millisecond), whoami()))

    state
  end

  @spec handle_read_repair(%KvStore{}, %KvStore.ReadRepairRequest{}) :: %KvStore{}
  def handle_read_repair(state, request) do
    Logger.debug("#{inspect(whoami())} Handling read repair request: #{inspect(request)}")
    # update_data(state, request.key, request.objects)
    update_data(state, request.key, request.entries.objects)
  end

  defp update_data(state, key, objects) do
    existing = Map.get(state.data, key, [])
    {original_node, _} = consistent_hash(key, state)
    if existing == [] do
      %{state | data: Map.put(state.data, key, objects), merkle_trees: Map.put(state.merkle_trees, original_node, FixedMerkleTree.insert(Map.get(state.merkle_trees, original_node, nil), key, objects))}
    else
      new_data = (objects ++ existing)
      |> get_latest_entries()
      %{state | data: Map.put(state.data, key, new_data), merkle_trees: Map.put(state.merkle_trees, original_node, FixedMerkleTree.insert(Map.get(state.merkle_trees, original_node, nil), key, new_data))}
    end
  end

  #Combines current clock with node's clock to get latest clock
  @spec get_updated_context(%KvStore{}, %KvStore.PutRequest{}, [%KvStore.CacheEntry{}]) :: %KvStore.Context{}
  defp get_updated_context(state, request, existing) do
    existing_contexts = Enum.map(existing, fn entry -> entry.context end)
    all_contexts = existing_contexts ++ request.contexts
    KvStore.Context.new(combine_vector_clocks([state.clock | Enum.map(all_contexts, fn ctx -> ctx.vector_clock end)]))
  end

  @spec handle_get_response_quorum(%KvStore{}, %KvStore.InternalGetRequest{}, integer(), integer()) :: %KvStore{}
  def handle_get_response_quorum(state, request, index, response_count) do
     Logger.debug("#{inspect(whoami())} Handling get response quorum for request: #{inspect(request)}")
    if response_count == state.read_quorum do
        responses = Map.get(state.request_responses, index, %{})
        combined_response = KvStore.GetResponse.new(get_updated_responses(responses), request.request.req_id)
        stale_nodes = get_stale_nodes(responses, combined_response)
        read_repair_request = KvStore.ReadRepairRequest.new(request.request.key, combined_response)
        broadcast(stale_nodes, read_repair_request)
        send(request.request.sender, combined_response)

        # send log to observer
        send(state.observer, KvStore.GetRequestLog.new(request.request.req_id, request.request.key, combined_response.objects, whoami()))

        %{state |
          read_repairs: Map.put(state.read_repairs, index, combined_response)
        }
    else
      state
    end
  end

  @spec handle_send_read_repair(%KvStore{}, %KvStore.InternalGetResponse{}, integer(), atom()) :: %KvStore{}
  def handle_send_read_repair(state, response, response_count, sender) do
    pending_request = Map.get(state.pending_requests, response.index, nil)
    if response_count > state.read_quorum do
      combined_response = Map.get(state.read_repairs, response.index, nil)
      if combined_response != nil  and is_response_stale(response.response, combined_response) do
        read_repair_request = KvStore.ReadRepairRequest.new(pending_request.request.key, combined_response)
        send(sender, read_repair_request)
      end
    end
    state
  end

  @spec handle_all_response_complete(%KvStore{}, integer(), integer()) :: %KvStore{}
  def handle_all_response_complete(state, index, response_count) do
    if response_count == state.replication_factor do
      remove_request(state, index)
    else
      state
    end
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
  # @spec get_updated_responses(%{atom() => %GetResponse{}}) :: %GetResponse{}
  @spec get_updated_responses(%{atom() => %GetResponse{}}) :: [%KvStore.CacheEntry{}]
  defp get_updated_responses(responses) do
    #Get the most recent response
    Logger.info("Getting updated responses for responses: #{inspect(responses)}")
    cache_entries = Enum.flat_map(responses, fn {_, response} -> response.objects end)
    cache_entries = deduplicate_entries(cache_entries)
    latest_entries = get_latest_entries(cache_entries)
    Logger.info("Latest entries: #{inspect(latest_entries)}")
    # KvStore.GetResponse.new(latest_entries)
    latest_entries
  end

  @spec deduplicate_entries([%KvStore.CacheEntry{}]) :: [%KvStore.CacheEntry{}]
  def deduplicate_entries(entries) do
    Enum.uniq_by(entries, fn entry -> entry.context end)
  end
  #Iterates through a list of cache_entries and returns the most recent cache_entries
  @spec get_latest_entries([%KvStore.CacheEntry{}]) :: [%KvStore.CacheEntry{}]
  defp get_latest_entries(cache_entries) do
    #Logger.info("Getting latest entries between #{inspect(cache_entries)}")
    to_exclude = get_exclude_list(cache_entries, cache_entries, MapSet.new())
    #Logger.info("Excluding entries: #{inspect(to_exclude)}")
    Enum.filter(cache_entries, fn entry -> !MapSet.member?(to_exclude, entry) end)
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
        MapSet.put(exclude, head)
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
        req_type = Map.get(state.pending_requests, index, %{}).request.type
        quorumSize = if req_type == :get do state.read_quorum else state.write_quorum end
        if map_size(Map.get(state.request_responses, index, [])) < quorumSize do
          send(request.request.sender, KvStore.FailedResponse.new(request.request))
        end
        state = remove_request(state, index)
        state
      else
        responded_nodes = MapSet.new(Map.keys(Map.get(state.request_responses, index, %{}))) # Get a set of all nodes that have responded to this request
        priority_list = get_preference_list(request.request.key, state, state.replication_factor)
        remaining_nodes = priority_list |> Enum.filter(fn node -> !MapSet.member?(responded_nodes, node) end)
        Logger.info("#{whoami()} Resending messages to remaining nodes: #{inspect(remaining_nodes)}")
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

  @spec send_heartbeat(%KvStore{}) :: %KvStore{}
  def send_heartbeat(state) do
    state = %{state | heartbeat_counter: state.heartbeat_counter + 1, peer_heartbeats: Map.put(state.peer_heartbeats, whoami(), state.heartbeat_counter + 1)}
    Logger.info("#{whoami()} Starting to send heartbeats: #{inspect(state.peer_heartbeats)}")
    msg = {:heartbeat, state.peer_heartbeats, true}
    peers = Enum.take_random(Enum.filter(state.sorted_nodes, fn node -> node != whoami() end), state.replication_factor)
    Logger.info("#{whoami()} Broadcasting heartbeat to: #{inspect(peers)}")
    broadcast(peers, msg)
    state
  end

  @spec handle_heartbeat(%KvStore{}, map(), atom(), boolean()) :: %KvStore{}
  def handle_heartbeat(state, new_heartbeat_counter, sender, req_resp) do
    Logger.debug("#{inspect(whoami())} Handling heartbeat from #{inspect(sender)} with hearbeats: #{inspect(new_heartbeat_counter)} - previous heartbeats: #{inspect(state.peer_heartbeats)}")
    state = %{state | peer_heartbeats: reconcile_heartbeats(state.peer_heartbeats, new_heartbeat_counter)}
    state = %{state | heartbeat_counter: Enum.max(Map.values(state.peer_heartbeats)) + 1}
    state = %{state | peer_heartbeats: Map.put(state.peer_heartbeats, whoami(), state.heartbeat_counter)}
    Logger.debug("#{inspect(whoami())} Handling heartbeat from #{inspect(sender)} - New heartbeats are: #{inspect(state.peer_heartbeats)}")
    if req_resp do
      msg = {:heartbeat, state.peer_heartbeats, false}
      send(sender, msg)
    end
    check_live_node_changes(state)
  end

  @spec reconcile_heartbeats(map(), map()) :: %{}
  def reconcile_heartbeats(peer_heartbeats, new_peer_heartbeats) do
    combine_vector_clocks(peer_heartbeats, new_peer_heartbeats) # Take the max value of each k-v pair
  end

  @spec check_live_node_changes(%KvStore{}) :: %KvStore{}
  def check_live_node_changes(state) do
    heartbeats = state.peer_heartbeats
    Logger.info("#{inspect(whoami())} - Heartbeats: #{inspect(heartbeats)}")
    max_heartbeat = Enum.max(Map.values(heartbeats))
    down_nodes = Enum.filter(Map.keys(heartbeats), fn node -> max_heartbeat - Map.get(heartbeats, node) > 60 end)
    Logger.debug("Down nodes: #{inspect(down_nodes)}")
    live_nodes = MapSet.new(Enum.filter(state.sorted_nodes, fn node -> !Enum.member?(down_nodes, node) end))
    new_live_nodes = MapSet.difference(live_nodes, state.live_nodes)
    Logger.debug("New live nodes: #{inspect(new_live_nodes)}")
    state = %{state |
      live_nodes: live_nodes
    }
    Enum.reduce(new_live_nodes, state, fn node, _ -> initiate_merkle_tree_sync(state, node) end)
  end

  @spec start_heartbeat_timer(%KvStore{}) :: %KvStore{}
  def start_heartbeat_timer(state) do
    if Map.get(state.timers, :heartbeat, nil) == nil do
      send(whoami(), :heartbeat_now)
    end
    state
  end

  @spec build_merkle_tree(atom(), [atom()]) :: %KvStore.FixedMerkleTree{}
  def build_merkle_tree(node, sorted_nodes) do
    # Logger.debug("Building merkle tree for node: #{inspect(node)}")
    range_start = hash(KvStore.Utils.get_previous_node(node, %{sorted_nodes: sorted_nodes}))
    range_end = hash(node)
    KvStore.FixedMerkleTree.build_tree(range_start, range_end, 4, 4)
  end

  @spec initiate_merkle_tree_sync(%KvStore{}, atom()) :: %KvStore{}
  def initiate_merkle_tree_sync(state, node) do
    # Logger.debug("Initiating merkle tree sync for node: #{inspect(node)}")
    nodes_to_sync = get_responsible_range(node, state) |> MapSet.delete(node)
    syncing_nodes = Enum.map(nodes_to_sync, fn n -> {n, get_first_responsible_node(n, state)} end) ++ [{node, get_second_responsible_node(node, state)}]
    sync_map = Enum.reduce(syncing_nodes,
      %{},
      fn {to_sync, responsible_node}, acc ->
        Map.update(acc, responsible_node, [to_sync], fn existing ->
          [to_sync | existing]
        end)
      end
    )
    # Logger.debug("Sync map: #{inspect(sync_map)}")
    # First live node which replicats to the node should initiate sync for it's own set of keys
    # if :a is responsible for [:d, :c, :a]: and all nodes are live, :d should sync it's replicas for :d to :a
    # if :a is responsible for [:d, :c, :a]: and :d is not live, :c should sync its replicas for [:d and :c] to :a
    # First available replica should sync original node's keys back to it.
    # Now, we know that :a needs it's sync back online:
    # If keys in :a are replicated to [:b, :e] -> and :b is down, :e should sync it's replicas for :a to :a
    my_responsibilities = Map.get(sync_map, whoami(), nil)
    if my_responsibilities != nil do
      Enum.each(my_responsibilities, fn to_sync ->
        merkle_tree = Map.get(state.merkle_trees, to_sync, nil)
        if merkle_tree != nil do
          # Logger.debug("Starting sync for node: #{inspect(to_sync)} to node: #{inspect(node)}")
          send(node, KvStore.SyncMerkleTree.new(to_sync, [FixedMerkleTree.getHeader(merkle_tree)]))
        end
      end)
    end
    state
  end

  @spec handle_merkle_tree_sync(%KvStore{}, [%KvStore.MerkleTreeHeader{}], atom(), atom()) :: %KvStore{}
  def handle_merkle_tree_sync(state, headers, to_sync, sender) do
    Logger.debug("#{inspect(whoami())} Handling merkle tree sync for node: #{inspect(to_sync)}")
    merkle_tree = Map.get(state.merkle_trees, to_sync, nil)
    if merkle_tree != nil do
      #Logger.debug("Merkle tree for node: #{inspect(whoami())} is: #{inspect(merkle_tree)}")
      matching_trees = Enum.map(headers, fn header -> {FixedMerkleTree.find_matching_node(merkle_tree, header), header} end)
      unmatched_trees = Enum.filter(matching_trees, fn {tree, header} -> tree.hash != header.hash end)
      if unmatched_trees == [] do
        send(sender, KvStore.MerkleSyncComplete.new(to_sync))
      else
        Enum.each(unmatched_trees, fn {_, header} -> send(sender, KvStore.UnsyncedMerkleTree.new(to_sync, header)) end)
      end
    end
    state
  end

  @spec handle_unsynced_merkle_tree(%KvStore{}, %KvStore.MerkleTreeHeader{}, atom(), atom()) :: %KvStore{}
  def handle_unsynced_merkle_tree(state, header, to_sync, sender) do
    # Logger.debug("#{inspect(whoami())}: Handling unsynced merkle tree for node: #{inspect(to_sync)} from node: #{inspect(sender)}")
    merkle_tree = Map.get(state.merkle_trees, to_sync, nil)
    matching_tree = FixedMerkleTree.find_matching_node(merkle_tree, header)
    if !matching_tree.is_leaf do
      children_headers = matching_tree.child_values |> Enum.map(fn child -> FixedMerkleTree.getHeader(child) end)
      send(sender, KvStore.SyncMerkleTree.new(to_sync, children_headers))
    else
      kv_stores = matching_tree.bucket
      Enum.map(kv_stores, fn {key, entries} -> KvStore.ReadRepairRequest.new(key, entries) end)
      |> Enum.each(fn request -> send(sender, request) end)
    end
    state
  end

  @spec handle_merkle_sync_complete(%KvStore{}, atom()) :: %KvStore{}
  def handle_merkle_sync_complete(state, node) do
    # Logger.debug("Handling merkle sync complete for node: #{inspect(node)}")
    state
  end

end
