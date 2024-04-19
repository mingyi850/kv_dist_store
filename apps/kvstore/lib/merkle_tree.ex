
defmodule KvStore.MerkleTreeHeader do
  defstruct(
    range_start: 0,
    range_end: 0,
    hash: 0,
    is_leaf: false,
    parent: nil
  )

  @spec new(integer(), integer(), integer(), boolean()) :: %KvStore.MerkleTreeHeader{}
  def new(range_start, range_end, hash, is_leaf) do
    %KvStore.MerkleTreeHeader{
      range_start: range_start,
      range_end: range_end,
      hash: hash,
      is_leaf: is_leaf
    }
  end
end

defmodule KvStore.FixedMerkleTree do
  # Construct a merkle tree for a given range between (primary node) and (secondary node). This will be the tree belonging to the secondary node.
  # Each tree will have a fixed number of partitions with buckets. - This is to ensure that the structure of the tree is consistent across all nodes.
  # Say we have a tree from :a - :b with hash values 10000 - 20000.
  #   - We dictate that the tree will have a fixed height of 4. (4 exchange rounds)
  #   - Each node will have 4 children (4 partitions)
  #   - So for a node with hash values [10000 - 20000]
  #     - The root node will have children with keyranges [10000 - 12500], [12500 - 15000], [15000 - 17500], [17500 - 20000]
  #     - the first node of the second level with key ranges [10000 - 12500] will have key ranges [10000 - 10625], [10625 - 11250], [11250 - 11875], [11875 - 12500]
  #     - and so on.
  #  - The leaf nodes will contain the actual data.

  #Sync process:
  #  - The node initiating sync (primary) will send it's root node to the node which requires sync (secondary).
  #  - The secondary node will check if the hash of it's root node matches the hash of the root node of the primary node.
  #   - If the hash matches, the secondary node will respond with a :sync_complete message.
  #   - If it does not match, the secondary node will respond with :sync_mismatch message, with the range key of the root node.
  #   - The primary node will then send the next layer of nodes to the secondary node.
  #       - The seconday node iterates through the list and checks all hashes against it's own second layer of nodes.
  #       - If a hash does not match, the secondary node will respond with a :sync_mismatch message, with the range key of the node which doesn't match.
  #       - If all hashes match, the secondary node will respond with a :sync_complete message.
  #   - This process continues until the leaf nodes are reached.
  #   - When the primary node sends the value of the leaf nodes which have mismatches, the secondary node will reconcile the values against it's own k-v store.
  #   - The secondary node will then respond with a :sync_complete message with the updated values of each key and value. The primary node can use these values to reconcile new values as well.

  import KvStore.Utils

  require Logger
  defstruct(
    range_start: 0,
    range_end: 0,
    hash: 0,
    child_values: [],
    bucket: %{},
    isleaf: false
  )

  @spec new(integer(), integer(), [%KvStore.FixedMerkleTree{}], %{}, boolean()) :: %KvStore.FixedMerkleTree{}
  def new(range_start, range_end, child_values, bucket, isleaf) do
    if isleaf do
      %KvStore.FixedMerkleTree{
      range_start: range_start,
      range_end: range_end,
      hash: hash(Enum.sum(Enum.map(Map.to_list(bucket), fn {key, value} -> hash("#{key}:#{inspect(value)}") end))),
      child_values: child_values,
      bucket: bucket,
      isleaf: isleaf
    }
    end
    %KvStore.FixedMerkleTree{
      range_start: range_start,
      range_end: range_end,
      hash: hash(Enum.sum(Enum.map(child_values, fn child -> child.hash end))),
      child_values: child_values,
      bucket: bucket,
      isleaf: isleaf
    }
  end

  @spec build_tree(integer(), integer(), integer(), integer()) :: %KvStore.FixedMerkleTree{}
  def build_tree(range_start, range_end, height, num_partitions) do
    #Logger.debug("Building tree with range: #{range_start} - #{range_end} and height: #{height}")
    if height == 0 do
      new(range_start, range_end, [], %{}, true)
    else
      partition_size = (range_end - range_start) / num_partitions
      child_values = Enum.map(0..num_partitions-1, fn i ->
        build_tree(range_start + i * partition_size, range_start + (i + 1) * partition_size, height - 1, num_partitions)
      end)

      new(range_start, range_end, child_values, %{}, false)
    end
  end

  @spec insert(%KvStore.FixedMerkleTree{}, String.t(), any()) :: %KvStore.FixedMerkleTree{}
  def insert(tree, key, value) do
    keyhash = hash(key)
    insert(tree, key, keyhash, value)
  end

  @spec insert(%KvStore.FixedMerkleTree{}, String.t(), integer(), any()) :: %KvStore.FixedMerkleTree{}
  def insert(tree, key, keyhash, value) do
    if keyhash < tree.range_end and keyhash >= tree.range_start do
      if tree.isleaf do
        #Insert key and value into tree
        new_bucket = Map.put(tree.bucket, key, value)
        new(tree.range_start, tree.range_end, tree.child_values, new_bucket, tree.isleaf)
      else
        #Insert key and value into child node
        new_child_values = tree.child_values |> Enum.map(fn child ->
          insert(child, key, keyhash, value)
        end)
        new(tree.range_start, tree.range_end, new_child_values, tree.bucket, tree.isleaf)
      end
    else
      tree
    end
  end

  @spec getHeader(%KvStore.FixedMerkleTree{}) :: %KvStore.MerkleTreeHeader{}
  def getHeader(tree) do
    %KvStore.MerkleTreeHeader{
      range_start: tree.range_start,
      range_end: tree.range_end,
      hash: tree.hash,
      is_leaf: tree.isleaf
    }
  end

  @spec getLeafNode(%KvStore.FixedMerkleTree{}, integer()) :: %KvStore.FixedMerkleTree{}
  def getLeafNode(tree, keyhash) do
    if keyhash < tree.range_end and keyhash >= tree.range_start do
      if tree.isleaf do
        tree
      else
        ranges = tree.child_values |> Enum.map(fn child -> {child.range_start, child.range_end} end)
        child_index = Enum.find_index(ranges, fn {range_start, range_end} -> keyhash < range_end and keyhash >= range_start end)
        getLeafNode(Enum.at(tree.child_values, child_index), keyhash)
      end
    else
      nil
    end
  end

  @spec get(%KvStore.FixedMerkleTree{}, String.t()) :: any()
  def get(tree, key) do
    keyhash = hash(key)
    leafNode = getLeafNode(tree, keyhash)
    if leafNode != nil do
      leafNode.bucket[key]
    else
      nil
    end
  end

end
