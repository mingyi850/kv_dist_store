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

defmodule KvStore.GetRequest do

  defstruct(
    key: "",
    sender: nil,
    original_recipient: nil,
    type: :get
  )

  @spec new(String.t(), pid(), pid()) :: %KvStore.GetRequest{}
  def new(key, sender, original_recipient) do
    %KvStore.GetRequest{
      key: key,
      sender: sender,
      original_recipient: original_recipient,
      type: :get
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
    type: :put
  )

  @spec new(String.t(), any(), [%KvStore.Context{}], pid(), pid()) :: %KvStore.PutRequest{}
  def new(key, object, contexts, sender, original_recipient) do
    %KvStore.PutRequest{
      key: key,
      object: object,
      contexts: contexts,
      sender: sender,
      original_recipient: original_recipient,
      type: :put
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
      type: :get_response
    )

    @spec new([%KvStore.CacheEntry{}]) :: %KvStore.GetResponse{}
    def new(entries) do
      %KvStore.GetResponse{
        objects: entries,
        type: :get_response
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
      type: :put_response
    )

    @spec new(%KvStore.Context{}) :: %KvStore.PutResponse{}
    def new(context) do
      %KvStore.PutResponse{
        context: context,
        type: :put_response
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

  @spec new(String.t(), [%KvStore.CacheEntry{}]) :: %KvStore.ReadRepairRequest{}
  def new(key, entries) do
    %KvStore.ReadRepairRequest{
      key: key,
      entries: entries,
      type: :read_repair
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
    recv_ts: 0,
    resp_ts: 0,
    type: :get_log
  )

  @spec new(non_neg_integer(), String.t(), any(), pid(), non_neg_integer(), non_neg_integer()) :: %KvStore.GetRequestLog{}
  def new(req_id, key, object, sender, recv_ts, resp_ts) do
    %KvStore.GetRequestLog{
      req_id: req_id,
      key: key,
      object: object,
      sender: sender,
      recv_ts: recv_ts,
      resp_ts: resp_ts,
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
    recv_ts: 0,
    resp_ts: 0,
    type: :put_log
  )

  @spec new(non_neg_integer(), String.t(), any(), pid(), non_neg_integer(), non_neg_integer()) :: %KvStore.PutRequestLog{}
  def new(req_id, key, object, sender) do
    %KvStore.PutRequestLog{
      req_id: req_id,
      key: key,
      object: object,
      sender: sender,
      recv_ts: recv_ts,
      resp_ts: resp_ts,
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
    send_ts: 0,
    recv_ts: 0,
    type: :client_log
  )

  @spec new(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: %KvStore.GetRequestLog{}
  def new(req_id, send_ts, recv_ts) do
    %KvStore.GetRequestLog{
      req_id: req_id,
      send_ts: send_ts,
      recv_ts: recv_ts,
      type: :client_log
    }
  end

end
