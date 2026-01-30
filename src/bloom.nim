import hashes, math

type
  BloomFilter* = object
    bitset*: seq[uint8]
    numHashes*: int
    size*: int

proc initBloomFilter*(expectedItems: int, falsePositiveRate: float = 0.01): BloomFilter =
  # m = -(n * ln(p)) / (ln(2)^2)
  let m = uint((-float(expectedItems) * ln(falsePositiveRate)) / (ln(2.0) * ln(2.0)))
  # k = (m/n) * ln(2)
  let k = int((float(m) / float(expectedItems)) * ln(2.0))
  
  result.size = int(m)
  result.numHashes = max(1, k)
  result.bitset = newSeq[uint8]((result.size + 7) div 8)

proc add*(bf: var BloomFilter, key: string) =
  let h1 = uint(hash(key))
  let h2 = uint(hash(key & "_salt"))
  for i in 0 ..< bf.numHashes:
    let idx = (h1 + uint(i) * h2) mod uint(bf.size)
    let bitIdx = idx mod 8
    let byteIdx = idx div 8
    bf.bitset[byteIdx] = bf.bitset[byteIdx] or (1'u8 shl bitIdx)

proc contains*(bf: BloomFilter, key: string): bool =
  let h1 = uint(hash(key))
  let h2 = uint(hash(key & "_salt"))
  for i in 0 ..< bf.numHashes:
    let idx = (h1 + uint(i) * h2) mod uint(bf.size)
    let bitIdx = idx mod 8
    let byteIdx = idx div 8
    if (bf.bitset[byteIdx] and (1'u8 shl bitIdx)) == 0:
      return false
  return true

when isMainModule:
  var bf = initBloomFilter(1000)
  bf.add("user_123")
  echo "Contains user_123: ", bf.contains("user_123")
  echo "Contains user_999: ", bf.contains("user_999")
