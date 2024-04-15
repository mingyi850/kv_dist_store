
defmodule KvStore.TestClient do
  import Kernel, except: [send: 2]
  import Emulation, only: [spawn: 2, send: 2, whoami: 0]

  require Logger
  def testClientSend(key, target) do
    IO.puts("Starting test client")
    send(target, {:get, key})
    IO.puts("Sent message {:get, #{key}} to #{inspect(target)}")
    receive do
      {sender, :ok} ->
        Logger.info("TestClient received ok")
        sender
      anything ->
        Logger.error("TestClient received unknown message #{inspect(anything)}")
        testClientSend(key, target)
    end
  end

  def kvStub() do
    receive do
      {_, message} ->
        Logger.info("#{whoami()} Received message #{inspect(message)}")
        send(message.sender, :ok)
        kvStub()
      _ ->
        Logger.error("#{whoami()} Received unknown message")
        kvStub()
    end
  end
end
