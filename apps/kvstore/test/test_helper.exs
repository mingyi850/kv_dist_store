
defmodule KvStore.TestClient do
  import Kernel, except: [send: 2]
  import Emulation, only: [spawn: 2, send: 2, whoami: 0]

  require Logger
  def testClientSend(msg, target) do
    IO.puts("Starting test client")
    send(target, msg)
    IO.puts("Sent message #{inspect(msg)} to #{inspect(target)}")
    receive do
      {sender, :ok} ->
        Logger.info("TestClient received ok")
        sender
      {sender, response} ->
        Logger.info("TestClient received response #{inspect(response)}")
        response
      anything ->
        Logger.error("TestClient received unknown message #{inspect(anything)}")
        testClientSend(msg, target)
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
