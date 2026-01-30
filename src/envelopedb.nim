import os, tables, streams, times, strutils, algorithm, posix, memtable, sstable, bloom

const
  HeaderSize = 4 + 8 + 4 + 4 + 4 # crc(4) + ts(8) + bsz(4) + ksz(4) + vsz(4)
  Tombstone = uint32.high
  MaxFileSize = 10 * 1024 * 1024 # 10MB limit for rotation
  MemTableLimit = 1000 # Keys before flush

# --- CRC32 Implementation ---
const crcTable = block:
  var table: array[256, uint32]
  for i in 0'u32 .. 255'u32:
    var c = i
    for j in 0 .. 7:
      if (c and 1) != 0:
        c = 0xEDB88320'u32 xor (c shr 1)
      else:
        c = c shr 1
    table[int(i)] = c
  table

proc crc32*(data: string): uint32 =
  result = 0xFFFFFFFF'u32
  for b in data:
    result = crcTable[int((result xor uint32(ord(b))) and 0xFF)] xor (result shr 8)
  result = result xor 0xFFFFFFFF'u32
# ---------------------------

type
  Entry* = object
    fileId*: int
    offset*: int64
    size*: int32
    timestamp*: int64

  MappedFile = object
    data: pointer
    size: int

  EnvelopeDBObj* = object
    dataDir*: string
    # Legacy Bitcask Index (kept for WAL recovery)
    buckets*: Table[string, Table[string, Entry]]
    activeFile*: FileStream
    activeFileId*: int
    # Mapped rotated files: fileId -> (pointer, size)
    mappedFiles: Table[int, MappedFile]

    # LSM-Tree specific fields
    memTables*: Table[string, MemTable[string, string]]
    sstables*: seq[SSTable]

  EnvelopeDB* = ref EnvelopeDBObj

proc getBucket(db: EnvelopeDB, bucketName: string): var Table[string, Entry] =
  if not db.buckets.contains(bucketName):
    db.buckets[bucketName] = initTable[string, Entry]()
  return db.buckets[bucketName]

proc getDataFilePath(dir: string, id: int): string =
  dir / "data_" & $id & ".evp"

proc mapFile(db: EnvelopeDB, fileId: int) =
  let path = getDataFilePath(db.dataDir, fileId)
  let fd = open(path.cstring, O_RDONLY)
  if fd < 0: return
  
  let size = getFileSize(path)
  if size == 0:
    discard close(fd)
    return
    
  let data = mmap(nil, size, PROT_READ, MAP_PRIVATE, fd, 0)
  discard close(fd) # Can close FD after mmap
  
  if data == MAP_FAILED:
    echo "⚠️ Failed to mmap file: ", path
    return
    
  db.mappedFiles[fileId] = MappedFile(data: data, size: int(size))

proc unmapFiles(db: EnvelopeDB) =
  for fileId, mf in db.mappedFiles:
    discard munmap(mf.data, mf.size)
  db.mappedFiles.clear()

proc openActiveFile(db: EnvelopeDB) =
  let path = getDataFilePath(db.dataDir, db.activeFileId)
  db.activeFile = openFileStream(path, fmAppend)
  if db.activeFile == nil:
    db.activeFile = openFileStream(path, fmWrite)
  
  if db.activeFile == nil:
    raise newException(IOError, "Could not open data file: " & path)

proc rotateFile(db: EnvelopeDB) =
  if db.activeFile != nil:
    db.activeFile.close()
    # Map the now-immutable file
    db.mapFile(db.activeFileId)
    
  db.activeFileId += 1
  db.openActiveFile()

proc getHintFilePath(dir: string, id: int): string =
  dir / "data_" & $id & ".hint"

proc writeHintFile(db: EnvelopeDB, fileId: int) =
  let path = getHintFilePath(db.dataDir, fileId)
  let fs = openFileStream(path, fmWrite)
  if fs == nil: return
  defer: fs.close()
  
  for bucketName, keys in db.buckets:
    for key, entry in keys:
      if entry.fileId == fileId:
        fs.write(uint32(bucketName.len))
        fs.write(uint32(key.len))
        fs.write(entry.size)
        fs.write(entry.timestamp)
        fs.write(entry.offset)
        fs.write(bucketName)
        fs.write(key)

proc loadIndexFromHint(db: EnvelopeDB, fileId: int): bool =
  let path = getHintFilePath(db.dataDir, fileId)
  if not fileExists(path): return false
  
  let fs = openFileStream(path, fmRead)
  if fs == nil: return false
  defer: fs.close()
  
  while not fs.atEnd():
    try:
      let bsz = fs.readUint32()
      let ksz = fs.readUint32()
      let sz = fs.readInt32()
      let ts = fs.readInt64()
      let offset = fs.readInt64()
      let bucketName = fs.readStr(int(bsz))
      let key = fs.readStr(int(ksz))
      
      if not db.buckets.contains(bucketName):
        db.buckets[bucketName] = initTable[string, Entry]()
      
      db.buckets[bucketName][key] = Entry(
        fileId: fileId,
        offset: offset,
        size: sz,
        timestamp: ts
      )
    except:
      return false
  return true


proc loadIndexFromFile(db: EnvelopeDB, fileId: int) =
  if db.loadIndexFromHint(fileId):
    return

  let path = getDataFilePath(db.dataDir, fileId)
  if not fileExists(path): return

  let fs = openFileStream(path, fmRead)
  if fs == nil: return
  defer: fs.close()

  while not fs.atEnd():
    try:
      let offset = fs.getPosition()
      let storedCrc = fs.readUint32()
      let ts = fs.readInt64()
      let bsz = fs.readUint32()
      let ksz = fs.readUint32()
      let vsz = fs.readUint32()
      let bucketName = fs.readStr(int(bsz))
      let key = fs.readStr(int(ksz))
      
      if not db.buckets.contains(bucketName):
        db.buckets[bucketName] = initTable[string, Entry]()

      if vsz == Tombstone:
        db.buckets[bucketName].del(key)
        continue

      fs.setPosition(fs.getPosition() + int64(vsz))
      
      let totalSize = int32(HeaderSize + int(bsz) + int(ksz) + int(vsz))
      db.buckets[bucketName][key] = Entry(
        fileId: fileId,
        offset: offset,
        size: totalSize,
        timestamp: ts
      )
    except:
      break

proc loadIndex(db: EnvelopeDB) =
  var ids: seq[int] = @[]
  for file in walkFiles(db.dataDir / "data_*.evp"):
    let filename = extractFilename(file)
    let idStr = filename.replace("data_", "").replace(".evp", "")
    try:
      ids.add(parseInt(idStr))
    except:
      discard
  
  ids.sort()
  for i, id in ids:
    db.loadIndexFromFile(id)
    if i < ids.len - 1:
      db.mapFile(id)
    
    if id > db.activeFileId:
      db.activeFileId = id

proc loadSSTables(db: EnvelopeDB) =
  for file in walkFiles(db.dataDir / "*.sst"):
    let sst = loadSSTable(file)
    if sst != nil:
      db.sstables.add(sst)
      echo "✅ Loaded SSTable: ", file

proc init*(dataDir: string): EnvelopeDB =
  new(result)
  result.dataDir = dataDir
  result.buckets = initTable[string, Table[string, Entry]]()
  result.activeFileId = 0
  result.mappedFiles = initTable[int, MappedFile]()
  result.memTables = initTable[string, MemTable[string, string]]()
  result.sstables = @[]
  
  if not dirExists(dataDir): createDir(dataDir)
  
  # Load SSTables first
  result.loadSSTables()
  
  # Load WAL index
  result.loadIndex()
  result.openActiveFile()

proc flushMemTable(db: EnvelopeDB, bucketName: string) =
  let sl = db.memTables[bucketName]
  if sl.count == 0: return
  
  let sstPath = db.dataDir / "sstable_" & $getTime().toUnix() & "_" & bucketName & ".sst"
  writeSSTable(sstPath, sl)
  
  let sst = loadSSTable(sstPath)
  if sst != nil:
    db.sstables.insert(sst, 0) # Add new SSTable to front (highest priority)
    echo "💾 Flushed MemTable to SSTable: ", sstPath

  # Clear MemTable
  db.memTables[bucketName] = newMemTable[string, string]()

proc put*(db: EnvelopeDB, key, value: string, bucketName: string = "default") =
  let ts = getTime().toUnix()
  let bsz = uint32(bucketName.len)
  let ksz = uint32(key.len)
  let vsz = uint32(value.len)
  let totalSize = int32(HeaderSize + bucketName.len + key.len + value.len)

  # 1. Write to WAL (Append-only Log)
  if db.activeFile.getPosition() + int64(totalSize) > MaxFileSize:
    db.rotateFile()
  
  var buf = newStringStream()
  buf.write(ts)
  buf.write(bsz)
  buf.write(ksz)
  buf.write(vsz)
  buf.write(bucketName)
  buf.write(key)
  buf.write(value)
  buf.setPosition(0)
  let dataToCrc = buf.readAll()
  let crc = crc32(dataToCrc)

  let offset = db.activeFile.getPosition()
  db.activeFile.write(crc)
  db.activeFile.write(dataToCrc)
  db.activeFile.flush()
  
  # Update internal Bitcask index (for WAL consistency)
  if not db.buckets.contains(bucketName):
    db.buckets[bucketName] = initTable[string, Entry]()
  db.buckets[bucketName][key] = Entry(
    fileId: db.activeFileId,
    offset: offset,
    size: totalSize,
    timestamp: ts
  )
  
  # 2. Update MemTable
  if not db.memTables.contains(bucketName):
    db.memTables[bucketName] = newMemTable[string, string]()
    
  db.memTables[bucketName].put(key, value)
  
  # 3. Check for Flush
  if db.memTables[bucketName].count >= MemTableLimit:
    db.flushMemTable(bucketName)

proc delete*(db: EnvelopeDB, key: string, bucketName: string = "default") =
  # Delete in LSM-Trees is just a put with a special "Tombstone" value
  # We reuse the existing logic but set vsz to Tombstone
  let ts = getTime().toUnix()
  let bsz = uint32(bucketName.len)
  let ksz = uint32(key.len)
  let vsz = Tombstone
  
  if db.activeFile.getPosition() + int64(HeaderSize + bucketName.len + key.len) > MaxFileSize:
    db.rotateFile()

  var buf = newStringStream()
  buf.write(ts)
  buf.write(bsz)
  buf.write(ksz)
  buf.write(vsz)
  buf.write(bucketName)
  buf.write(key)
  buf.setPosition(0)
  let crc = crc32(buf.readAll())

  db.activeFile.write(crc)
  db.activeFile.write(ts)
  db.activeFile.write(bsz)
  db.activeFile.write(ksz)
  db.activeFile.write(vsz)
  db.activeFile.write(bucketName)
  db.activeFile.write(key)
  db.activeFile.flush()
  
  # Update legacy index and MemTable
  if db.buckets.contains(bucketName): db.buckets[bucketName].del(key)
  if db.memTables.contains(bucketName): db.memTables[bucketName].put(key, "") # Empty string = delete in this simplified LSM

proc get*(db: EnvelopeDB, key: string, bucketName: string = "default"): string =
  # 1. Check MemTable
  if db.memTables.contains(bucketName):
    let (found, val) = db.memTables[bucketName].get(key)
    if found: return val
    
  # 2. Check SSTables
  for sst in db.sstables:
    # (Future: Check Bloom Filter here)
    let val = sst.get(key)
    if val != "": return val
    
  # 3. Fallback to Bitcask Index (for un-flushed legacy data/recovery)
  if not db.buckets.contains(bucketName): return ""
  if not db.buckets[bucketName].contains(key): return ""
    
  let entry = db.buckets[bucketName][key]
  
  # Optimized mmap/stream read logic (the rest of the original get proc)
  if db.mappedFiles.contains(entry.fileId):
    let mf = db.mappedFiles[entry.fileId]
    let storedCrc = cast[ptr uint32](cast[uint](mf.data) + uint(entry.offset))[]
    let dataLen = entry.size - 4
    let dataPtr = cast[ptr char](cast[uint](mf.data) + uint(entry.offset) + 4)
    var dataToCrc = newString(dataLen)
    copyMem(addr dataToCrc[0], dataPtr, dataLen)
    if crc32(dataToCrc) != storedCrc: return ""
    let vOffset = 8 + 4 + 4 + 4 + bucketName.len + key.len
    return dataToCrc[vOffset .. ^1]

  let path = getDataFilePath(db.dataDir, entry.fileId)
  let fs = openFileStream(path, fmRead)
  if fs == nil: return ""
  defer: fs.close()
  fs.setPosition(entry.offset)
  let storedCrc = fs.readUint32()
  let dataToCrc = fs.readStr(int(entry.size - 4))
  if crc32(dataToCrc) != storedCrc: return ""
  let vOffset = 8 + 4 + 4 + 4 + bucketName.len + key.len
  result = dataToCrc[vOffset .. ^1]

proc close*(db: EnvelopeDB) =
  if db.activeFile != nil:
    db.activeFile.flush()
    db.activeFile.close()
    db.activeFile = nil
  db.unmapFiles()

proc merge*(db: EnvelopeDB) =
  ## Compacts the database by rewriting all active keys into new files 
  ## and deleting the old ones.
  let mergeDir = db.dataDir / "merge"
  if dirExists(mergeDir): removeDir(mergeDir)
  createDir(mergeDir)
  
  # Temporary DB to write compacted data
  var tempDb = init(mergeDir)
  
  # Copy all current active keys to the new location across ALL buckets
  for bucketName, keys in db.buckets:
    for key, entry in keys:
      let val = db.get(key, bucketName)
      if val != "":
        tempDb.put(key, val, bucketName)
      
  # Write hint files for compacted data
  for id in 0 .. tempDb.activeFileId:
    tempDb.writeHintFile(id)
  
  # Close both DBs' active files before swapping
  db.activeFile.close()
  db.unmapFiles() # MUST unmap before deleting old files
  
  tempDb.close() # Safely close and unmap the temp DB
  
  # Remove old files (data and hints)
  for file in walkFiles(db.dataDir / "data_*.evp"): removeFile(file)
  for file in walkFiles(db.dataDir / "data_*.hint"): removeFile(file)
    
  # Move new files from merge dir to data dir
  for file in walkFiles(mergeDir / "data_*.*"):
    let filename = extractFilename(file)
    moveFile(file, db.dataDir / filename)
    
  removeDir(mergeDir)
  
  # Reset current DB state and reload
  db.activeFileId = tempDb.activeFileId
  db.buckets.clear()
  db.loadIndex()
  db.openActiveFile()

when isMainModule:
  # Simple example usage of Buckets
  if dirExists("bucket_test"): removeDir("bucket_test")
  var db = init("bucket_test")
  
  echo "--- Bucket Isolation Test ---"
  db.put("user_1", "John", "PROFILES")
  db.put("user_1", "Pizza", "ORDERS")
  
  echo "Profiles Bucket: ", db.get("user_1", "PROFILES")
  echo "Orders Bucket: ", db.get("user_1", "ORDERS")
  echo "Default Bucket: '", db.get("user_1", "default"), "'"
  
  db.merge()
  echo "After Merge - Profiles: ", db.get("user_1", "PROFILES")
  
  echo "\n🚀 EnvelopeDB Multi-Bucket ready."
