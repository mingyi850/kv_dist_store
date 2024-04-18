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
    context: nil,
    sender: nil,
    original_recipient: nil,
    type: :put
  )

  @spec new(String.t(), any(), %KvStore.Context{}, pid(), pid()) :: %KvStore.PutRequest{}
  def new(key, object, context, sender, original_recipient) do
    %KvStore.PutRequest{
      key: key,
      object: object,
      context: context,
      sender: sender,
      original_recipient: original_recipient,
      type: :put
    }
  end
end

defmodule KvStore.InternalPutRequest do
  defstruct(
    request: nil,
    index: 0
  )

  @spec new(KvStore.PutRequest.t(), integer()) :: %KvStore.InternalPutRequest{}
  def new(request, index) do
    %KvStore.InternalPutRequest{
      request: request,
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
