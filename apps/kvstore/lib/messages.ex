defmodule KvStore.Context do

    defstruct(
      vector_clock: %{}
    )

    @spec new() :: %KvStore.Context{}
    def new() do
      %KvStore.Context{
        vector_clock: %{}
      }
    end

    @spec new(map()) :: %KvStore.Context{}
    def new(vector_clock) do
      %KvStore.Context{
        vector_clock: vector_clock
      }
    end


end

'''
`req_id` is the unique index given by load balancer to indicate the order of all requests
'''

defmodule KvStore.GetRequest do

  defstruct(
    key: "",
    sender: nil,
    original_recipient: nil,
    type: :get,
    req_id: 0
  )

  @spec new(String.t(), pid(), pid(), non_neg_integer()) :: %KvStore.GetRequest{}
  def new(key, sender, original_recipient, req_id) do
    %KvStore.GetRequest{
      key: key,
      sender: sender,
      original_recipient: original_recipient,
      type: :get,
      req_id: req_id
    }
  end

end

defmodule KvStore.InternalGetRequest do
  defstruct(
    request: nil,
    index: 0
  )

  @spec new(KvStore.GetRequest.t(), integer()) :: %KvStore.InternalGetRequest{}
  def new(request, index) do
    %KvStore.InternalGetRequest{
      request: request,
      index: index
    }
  end
end

defmodule KvStore.InternalGetResponse do
  defstruct(
    response: nil,
    index: 0
  )

  @spec new(KvStore.GetResponse.t(), integer()) :: %KvStore.InternalGetResponse{}
  def new(response, index) do
    %KvStore.InternalGetResponse{
      response: response,
      index: index
    }
  end

end

defmodule KvStore.PutRequest do

  defstruct(
    key: "",
    object: nil,
    contexts: [],
    sender: nil,
    original_recipient: nil,
    type: :put,
    req_id: 0
  )

  @spec new(String.t(), any(), [%KvStore.Context{}], pid(), pid(), non_neg_integer()) :: %KvStore.PutRequest{}
  def new(key, object, contexts, sender, original_recipient, req_id) do
    %KvStore.PutRequest{
      key: key,
      object: object,
      contexts: contexts,
      sender: sender,
      original_recipient: original_recipient,
      type: :put,
      req_id: req_id
    }
  end
end

defmodule KvStore.InternalPutRequest do
  defstruct(
    request: nil,
    context: nil,
    index: 0
  )

  @spec new(KvStore.PutRequest.t(), %KvStore.Context{}, integer()) :: %KvStore.InternalPutRequest{}
  def new(request, context, index) do
    %KvStore.InternalPutRequest{
      request: request,
      context: context,
      index: index
    }
  end
end

defmodule KvStore.InternalPutResponse do
  defstruct(
    response: nil,
    index: 0
  )

  @spec new(KvStore.PutResponse.t(), integer()) :: %KvStore.InternalPutResponse{}
  def new(response, index) do
    %KvStore.InternalPutResponse{
      response: response,
      index: index
    }
  end
end


defmodule KvStore.CacheEntry do
  defstruct(
    object: nil,
    context: nil
  )

  @spec new(any(), %KvStore.Context{}) :: %KvStore.CacheEntry{}
  def new(object, context) do
    %KvStore.CacheEntry{
      object: object,
      context: context
    }
  end
end

defmodule KvStore.GetResponse do
    defstruct(
      objects: [],
      type: :get_response,
      req_id: 0
    )

    @spec new([%KvStore.CacheEntry{}], non_neg_integer()) :: %KvStore.GetResponse{}
    def new(entries, req_id) do
      %KvStore.GetResponse{
        objects: entries,
        type: :get_response,
        req_id: req_id
      }
    end

    @spec get_contexts(%KvStore.GetResponse{}) :: [%KvStore.Context{}]
    def get_contexts(response) do
      Enum.map(response.objects, fn entry -> entry.context end)
    end
end

defmodule KvStore.PutResponse do
    defstruct(
      context: nil,
      type: :put_response,
      req_id: 0
    )

    @spec new(%KvStore.Context{}, non_neg_integer()) :: %KvStore.PutResponse{}
    def new(context, req_id) do
      %KvStore.PutResponse{
        context: context,
        type: :put_response,
        req_id: req_id
      }
    end
end

defmodule KvStore.FailedResponse do
    defstruct(
      request: nil,
      type: :failed_response
    )

    @spec new(%KvStore.GetRequest{} | %KvStore.PutRequest{}) :: %KvStore.FailedResponse{}
    def new(request) do
      %KvStore.FailedResponse{
        request: request,
        type: :failed_response
      }
    end
end

defmodule KvStore.ReadRepairRequest do
  defstruct(
    key: "",
    entries: nil,
    type: :read_repair
  )

  @spec new(String.t(), %KvStore.GetResponse{}) :: %KvStore.ReadRepairRequest{}
  def new(key, entries) do
    %KvStore.ReadRepairRequest{
      key: key,
      entries: entries,
      type: :read_repair
    }
  end
end

defmodule KvStore.SyncMerkleTree do

    defstruct(
      to_sync: nil,
      headers: [],
      type: :sync_merkle_tree
    )

    @spec new(atom(), [%KvStore.MerkleTreeHeader{}]) :: %KvStore.SyncMerkleTree{}
    def new(to_sync, headers) do
      %KvStore.SyncMerkleTree{
        to_sync: to_sync,
        headers: headers,
        type: :sync_merkle_tree
      }
    end
end

defmodule KvStore.UnsyncedMerkleTree do

      defstruct(
        to_sync: nil,
        header: nil,
        type: :unsynced_merkle_tree
      )

      @spec new(atom(), %KvStore.MerkleTreeHeader{}) :: %KvStore.UnsyncedMerkleTree{}
      def new(to_sync, header) do
        %KvStore.UnsyncedMerkleTree{
          to_sync: to_sync,
          header: header,
          type: :unsynced_merkle_tree
        }
      end
end

defmodule KvStore.MerkleSyncComplete do

      defstruct(
        to_sync: nil,
        type: :merkle_sync_complete
      )

      @spec new(atom()) :: %KvStore.MerkleSyncComplete{}
      def new(to_sync) do
        %KvStore.MerkleSyncComplete{
          to_sync: to_sync,
          type: :merkle_sync_complete
        }
      end
end

"""
message that kvnodes use to inform observer about get request (timestamp can be retrieved by `:os.system_time(:millisecond)`)
"""
defmodule KvStore.GetRequestLog do

  defstruct(
    req_id: 0,
    key: "",
    object: nil,
    sender: nil,
    # recv_ts: 0,
    # resp_ts: 0,
    type: :get_log
  )

  # @spec new(non_neg_integer(), String.t(), any(), pid(), non_neg_integer(), non_neg_integer()) :: %KvStore.GetRequestLog{}
  # def new(req_id, key, object, sender, recv_ts, resp_ts) do
  @spec new(non_neg_integer(), String.t(), any(), pid()) :: %KvStore.GetRequestLog{}
  def new(req_id, key, object, sender) do
    %KvStore.GetRequestLog{
      req_id: req_id,
      key: key,
      object: object,
      sender: sender,
      # recv_ts: recv_ts,
      # resp_ts: resp_ts,
      type: :get_log
    }
  end

end

"""
message that kvnodes use to inform observer about put request (timestamp can be retrieved by `:os.system_time(:millisecond)`)
"""
defmodule KvStore.PutRequestLog do

  defstruct(
    req_id: 0,
    key: "",
    object: nil,
    sender: nil,
    # recv_ts: 0,
    # resp_ts: 0,
    start_ts: 0,
    end_ts: 0,
    type: :put_log
  )

  # @spec new(non_neg_integer(), String.t(), any(), pid(), non_neg_integer(), non_neg_integer()) :: %KvStore.PutRequestLog{}
  # def new(req_id, key, object, sender, recv_ts, resp_ts) do
  @spec new(non_neg_integer(), String.t(), any(), non_neg_integer(), non_neg_integer(), pid()) :: %KvStore.PutRequestLog{}
  def new(req_id, key, object, start_ts, end_ts, sender) do
    %KvStore.PutRequestLog{
      req_id: req_id,
      key: key,
      object: object,
      sender: sender,
      # recv_ts: recv_ts,
      # resp_ts: resp_ts,
      start_ts: start_ts,
      end_ts: end_ts,
      type: :put_log
    }
  end
end

"""
message that client nodes use to inform observer about latency of requests (timestamp can be retrieved by `:os.system_time(:millisecond)`)
"""
defmodule KvStore.ClientRequestLog do

  defstruct(
    req_id: 0,
    sender: nil,
    send_ts: 0,
    recv_ts: 0,
    type: :client_log
  )

  @spec new(non_neg_integer(), pid(), non_neg_integer(), non_neg_integer()) :: %KvStore.ClientRequestLog{}
  def new(req_id, sender, send_ts, recv_ts) do
    %KvStore.ClientRequestLog{
      req_id: req_id,
      sender: sender,
      send_ts: send_ts,
      recv_ts: recv_ts,
      type: :client_log
    }
  end

end
