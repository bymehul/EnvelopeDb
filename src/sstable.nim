import streams, tables, bloom, memtable, os

type
  SSTable* = ref object
    path: string
    bloom: BloomFilter
    index: Table[string, (int64, int32)] # key -> (offset, size)

proc writeSSTable*(path: string, sl: MemTable[string, string]) =
  let fs = openFileStream(path, fmWrite)
  if fs == nil: return
  defer: fs.close()
  
  # 1. Placeholder for Bloom Filter and Index Offset
  fs.write("SST1") # Magic
  fs.write(0'i64)  # Index offset placeholder
  
  var index = initTable[string, (int64, int32)]()
  var bf = initBloomFilter(sl.count)
  
  # 2. Write Data
  for k, v in sl.items:
    bf.add(k)
    let offset = int64(fs.getPosition())
    fs.write(v)
    index[k] = (offset, int32(v.len))
    
  let indexOffset = fs.getPosition()
  
  # 3. Write Bloom Filter
  fs.write(int32(bf.numHashes))
  fs.write(int32(bf.size))
  fs.write(bf.bitset.len.int32)
  for b in bf.bitset: fs.write(b)
  
  # 4. Write Index
  fs.write(int32(index.len))
  for k, v in index:
    fs.write(int32(k.len))
    fs.write(k)
    fs.write(v[0]) # Offset
    fs.write(v[1]) # Size
    
  # 5. Patch Index Offset
  fs.setPosition(4)
  fs.write(indexOffset)

proc loadSSTable*(path: string): SSTable =
  let fs = openFileStream(path, fmRead)
  if fs == nil: return nil
  defer: fs.close()
  
  var magic = fs.readStr(4)
  if magic != "SST1": return nil
  
  let indexOffset = fs.readInt64()
  fs.setPosition(indexOffset)
  
  new(result)
  result.path = path
  
  # Load Bloom Filter
  let numHashes = fs.readInt32()
  let bloomSize = fs.readInt32()
  let bitsetLen = fs.readInt32()
  var bitset = newSeq[uint8](bitsetLen)
  for i in 0 ..< bitsetLen: bitset[i] = fs.readUint8()
  
  result.bloom = BloomFilter(
    numHashes: int(numHashes),
    size: int(bloomSize),
    bitset: bitset
  )
  
  # Load Index
  result.index = initTable[string, (int64, int32)]()
  let indexLen = fs.readInt32()
  for i in 0 ..< indexLen:
    let ksz = fs.readInt32()
    let k = fs.readStr(int(ksz))
    let offset = fs.readInt64()
    let sz = fs.readInt32()
    result.index[k] = (offset, sz)

proc get*(sst: SSTable, key: string): string =
  if not sst.bloom.contains(key): return "" # Skip if not in bloom
  if not sst.index.contains(key): return ""
  
  let (offset, size) = sst.index[key]
  let fs = openFileStream(sst.path, fmRead)
  if fs == nil: return ""
  defer: fs.close()
  
  fs.setPosition(offset)
  return fs.readStr(int(size))
