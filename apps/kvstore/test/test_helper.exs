
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

defmodule KvStoreTest.Utils do
  import KvStore.Utils
  require Logger

  @spec generate_key(atom(), %{sorted_nodes: [atom()], node_hashes: map(), live_nodes: MapSet.t(atom())}) :: String.t()
  def generate_key(target, state) do
    Logger.info("Sorted nodes are #{inspect(state.sorted_nodes)}, node hashes are #{inspect(state.node_hashes)}")
    candidate_keys = 1..10000
    |> Enum.map(fn n -> "key#{n}" end)
    |> Enum.filter(fn s ->
      {_, original_node} = consistent_hash(s, state)
      #Logger.info("Original node for #{s} is #{original_node}")
      original_node == target
    end)
    Logger.debug("Got candidate keys #{inspect(candidate_keys)}")
    Enum.random(candidate_keys)
  end
end
