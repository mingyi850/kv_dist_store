defmodule Context do

    defstruct(
      vector_clock: %{}
    )

    @spec new() :: %Context{}
    def new() do
      %Context{
        vector_clock: %{}
      }
    end


end

defmodule GetRequest do

  defstruct(
    key: "",
    sender: nil,
    original_recipient: nil
  )

  @spec new(String.t(), pid(), pid()) :: %GetRequest{}
  def new(key, sender, original_recipient) do
    %GetRequest{
      key: key,
      sender: sender,
      original_recipient: original_recipient
    }
  end

end

defmodule PutRequest do

    defstruct(
      key: "",
      object: nil,
      context: nil,
      sender: nil,
      original_recipient: nil

    )

    @spec new(String.t(), any(), pid(), %Context{}, pid()) :: %PutRequest{}
    def new(key, object, sender, context, original_recipient) do
      %PutRequest{
        key: key,
        object: object,
        context: context,
        sender: sender,
        original_recipient: original_recipient
      }
    end
end
