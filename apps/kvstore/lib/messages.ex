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
    original_recipient: nil
  )

  @spec new(String.t(), pid(), pid()) :: %KvStore.GetRequest{}
  def new(key, sender, original_recipient) do
    %KvStore.GetRequest{
      key: key,
      sender: sender,
      original_recipient: original_recipient
    }
  end

end

defmodule KvStore.PutRequest do

  defstruct(
    key: "",
    object: nil,
    context: nil,
    sender: nil,
    original_recipient: nil

  )

  @spec new(String.t(), any(), pid(), %KvStore.Context{}, pid()) :: %KvStore.PutRequest{}
  def new(key, object, sender, context, original_recipient) do
    %KvStore.PutRequest{
      key: key,
      object: object,
      context: context,
      sender: sender,
      original_recipient: original_recipient
    }
  end
end
