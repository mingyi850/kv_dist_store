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

defmodule KvStore.PutRequest do

  defstruct(
    key: "",
    object: nil,
    context: nil,
    sender: nil,
    original_recipient: nil,
    type: :put
  )

  @spec new(String.t(), any(), pid(), %KvStore.Context{}, pid()) :: %KvStore.PutRequest{}
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
      object: nil,
      context: nil,
      type: :get_response
    )

    @spec new(%KvStore.CacheEntry{}) :: %KvStore.GetResponse{}
    def new(entry) do
      %KvStore.GetResponse{
        object: entry.object,
        context: entry.context,
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
