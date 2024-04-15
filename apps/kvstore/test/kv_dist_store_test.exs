defmodule KvDistStoreTest do
  use ExUnit.Case
  doctest KvDistStore

  test "greets the world" do
    assert KvDistStore.hello() == :world
  end
end
