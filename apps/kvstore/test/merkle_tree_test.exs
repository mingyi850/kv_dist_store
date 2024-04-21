defmodule MerkleTreeTest do
  use ExUnit.Case
  doctest KvStore.FixedMerkleTree

  import KvStore.FixedMerkleTree
  require Logger

  test("Merkle tree can be created") do
    range_start = 10000
    range_end = 20000
    height = 4
    num_partitions = 4
    tree = KvStore.FixedMerkleTree.build_tree(range_start, range_end, height, num_partitions)
    #Logger.info("Tree: #{inspect(tree)}")
    tree = insert(tree, "key1", 10001, "value1")
    tree = insert(tree, "key2", 15000, "value2")

    leafNode1 = getLeafNode(tree, 10001)
    leafNode2 = getLeafNode(tree, 15000)
    assert leafNode1.bucket == %{"key1" => "value1"}
    assert leafNode2.bucket == %{"key2" => "value2"}

  end
end
