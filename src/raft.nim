import asyncdispatch, asyncnet, strutils, json, tables, times, random, os, envelopedb

type
  RaftState = enum
    Follower, Candidate, Leader

  LogEntry* = object
    term*: int
    command*: string # JSON command: {"op": "put", "key": "k", "val": "v"}

  RaftNode* = ref object
    id*: string
    peers*: seq[string] # "host:port"
    state*: RaftState
    currentTerm*: int
    votedFor*: string
    logEntries*: seq[LogEntry]
    commitIndex*: int
    lastApplied*: int
    
    # Leader state
    nextIndex: Table[string, int]
    matchIndex: Table[string, int]

    # Networking
    port*: Port
    server: AsyncSocket

    # Election Timer
    electionTimeout*: int
    lastHeartbeat*: float
    
    # State Machine
    db*: EnvelopeDB

proc logMsg*(n: RaftNode, msg: string) =
  echo "[RAFT-", n.id, "] ", msg

proc saveState(n: RaftNode) =
  let metaPath = n.db.dataDir & "/raft.meta"
  let state = %*{
    "term": n.currentTerm,
    "votedFor": n.votedFor,
    "log": %n.logEntries
  }
  writeFile(metaPath, $state)

proc loadState(n: RaftNode) =
  let metaPath = n.db.dataDir & "/raft.meta"
  if not fileExists(metaPath): return
  try:
    let state = parseJson(readFile(metaPath))
    n.currentTerm = state["term"].getInt()
    n.votedFor = state["votedFor"].getStr()
    n.logEntries = @[]
    for eJ in state["log"]:
      n.logEntries.add(to(eJ, LogEntry))
    n.logMsg("Recovered persisted state: Term " & $n.currentTerm & ", Logs " & $n.logEntries.len)
  except:
    n.logMsg("Failed to recover persisted state")

proc newRaftNode*(id: string, port: int, peers: seq[string], db: EnvelopeDB): RaftNode =
  new(result)
  result.id = id
  result.port = Port(port)
  result.peers = peers
  result.state = Follower
  result.currentTerm = 0
  result.votedFor = ""
  result.logEntries = @[]
  result.commitIndex = -1
  result.lastApplied = -1
  result.nextIndex = initTable[string, int]()
  result.matchIndex = initTable[string, int]()
  result.server = newAsyncSocket()
  result.db = db
  result.loadState()

proc handleAppendEntries*(n: RaftNode, term: int, leaderId: string, prevLogIndex: int, prevLogTerm: int, entries: seq[LogEntry], leaderCommit: int): bool =
  if term < n.currentTerm:
    return false
    
  n.currentTerm = term
  n.state = Follower
  n.lastHeartbeat = epochTime()
  n.saveState()

  # Check consistency
  if prevLogIndex >= 0:
    if prevLogIndex >= n.logEntries.len or n.logEntries[prevLogIndex].term != prevLogTerm:
      return false

  # Append entries
  if entries.len > 0:
    let oldLen = n.logEntries.len
    n.logEntries = n.logEntries[0 .. prevLogIndex]
    for entry in entries:
      n.logEntries.add(entry)
    
    if n.logEntries.len != oldLen:
      n.logMsg("Appended " & $entries.len & " new entries. Total: " & $n.logEntries.len)
      n.saveState()

  # Apply entries to DB
  if leaderCommit > n.commitIndex:
    n.commitIndex = min(leaderCommit, n.logEntries.len - 1)
    while n.lastApplied < n.commitIndex:
      n.lastApplied += 1
      let cmd = parseJson(n.logEntries[n.lastApplied].command)
      if cmd["op"].getStr() == "put":
        n.db.put(cmd["key"].getStr(), cmd["val"].getStr(), cmd.getOrDefault("bucket").getStr("default"))
      elif cmd["op"].getStr() == "delete":
        n.db.delete(cmd["key"].getStr(), cmd.getOrDefault("bucket").getStr("default"))
      n.logMsg("Applied to DB: " & $cmd["op"])

  return true

proc handleRequestVote*(n: RaftNode, term: int, candidateId: string, lastLogIndex: int, lastLogTerm: int): bool =
  if term < n.currentTerm:
    return false
  if term > n.currentTerm:
    n.currentTerm = term
    n.state = Follower
    n.votedFor = ""
    n.saveState()
    
  # Check if candidate's log is at least as up-to-date
  let myLastLogTerm = if n.logEntries.len > 0: n.logEntries[^1].term else: 0
  let myLastLogIndex = n.logEntries.len - 1
  
  let upToDate = (lastLogTerm > myLastLogTerm) or (lastLogTerm == myLastLogTerm and lastLogIndex >= myLastLogIndex)
    
  if (n.votedFor == "" or n.votedFor == candidateId) and upToDate:
    n.votedFor = candidateId
    n.saveState()
    return true
  return false

proc sendRPC*(target: string, data: JsonNode) {.async.} =
  let parts = target.split(':')
  let client = newAsyncSocket()
  try:
    await client.connect(parts[0], Port(parseInt(parts[1])))
    await client.send($data & "\n")
  except:
    discard
  finally:
    client.close()

proc sendRPCWithResponse*(target: string, data: JsonNode): Future[string] {.async.} =
  let parts = target.split(':')
  let client = newAsyncSocket()
  try:
    await client.connect(parts[0], Port(parseInt(parts[1])))
    await client.send($data & "\n")
    result = await client.recvLine()
  except:
    result = ""
  finally:
    client.close()

proc requestVoteFromPeer*(n: RaftNode, peer: string): Future[bool] {.async.} =
  let lastLogTerm = if n.logEntries.len > 0: n.logEntries[^1].term else: 0
  let lastLogIndex = n.logEntries.len - 1
  let msg = %*{
    "type": "RequestVote",
    "term": n.currentTerm,
    "candidateId": n.id,
    "lastLogIndex": lastLogIndex,
    "lastLogTerm": lastLogTerm
  }
  let resp = await sendRPCWithResponse(peer, msg)
  if resp == "": return false
  try:
    let j = parseJson(resp)
    return j["granted"].getBool()
  except:
    return false

proc sendHeartbeats*(n: RaftNode) {.async.} =
  while n.state == Leader:
    for peer in n.peers:
      let peerCopy = peer # Copy to capture safely in async closure
      let nextIdx = n.nextIndex.getOrDefault(peerCopy, 0)
      let prevLogIndex = nextIdx - 1
      let prevLogTerm = if prevLogIndex >= 0 and prevLogIndex < n.logEntries.len: n.logEntries[prevLogIndex].term else: 0
      
      var entriesToReplicate: seq[LogEntry] = @[]
      if n.logEntries.len > nextIdx:
        entriesToReplicate = n.logEntries[nextIdx .. ^1]

      let msg = %*{
        "type": "AppendEntries",
        "term": n.currentTerm,
        "leaderId": n.id,
        "prevLogIndex": prevLogIndex,
        "prevLogTerm": prevLogTerm,
        "entries": %entriesToReplicate,
        "leaderCommit": n.commitIndex
      }
      
      asyncCheck (proc() {.async.} =
        let resp = await sendRPCWithResponse(peerCopy, msg)
        if resp != "" and n.state == Leader:
          try:
            let j = parseJson(resp)
            if j["success"].getBool():
              n.matchIndex[peerCopy] = prevLogIndex + entriesToReplicate.len
              n.nextIndex[peerCopy] = n.matchIndex[peerCopy] + 1
              
              # Check for new commitIndex
              for N in countdown(n.logEntries.len - 1, n.commitIndex + 1):
                var count = 1 # Lead counts itself
                for p in n.peers:
                  if n.matchIndex.getOrDefault(p, 0) >= N:
                    count += 1
                if count > (n.peers.len + 1) div 2 and n.logEntries[N].term == n.currentTerm:
                  n.commitIndex = N
                  n.logMsg("Leader advanced commitIndex to " & $N)
                  break
            else:
              n.nextIndex[peerCopy] = max(0, n.nextIndex[peerCopy] - 1)
          except: discard
      )()
    
    # Leader applies its own committed entries
    while n.lastApplied < n.commitIndex:
      n.lastApplied += 1
      let cmd = parseJson(n.logEntries[n.lastApplied].command)
      if cmd["op"].getStr() == "put":
        n.db.put(cmd["key"].getStr(), cmd["val"].getStr(), cmd.getOrDefault("bucket").getStr("default"))
      elif cmd["op"].getStr() == "delete":
        n.db.delete(cmd["key"].getStr(), cmd.getOrDefault("bucket").getStr("default"))
      n.logMsg("Leader applied to local DB: " & $cmd["op"])

    await sleepAsync(100)

proc startElection*(n: RaftNode) {.async.} =
  n.state = Candidate
  n.currentTerm += 1
  n.votedFor = n.id
  n.saveState()
  n.logMsg("Starting election for term " & $n.currentTerm)
  
  var votes = 1
  let currentElectionTerm = n.currentTerm
  
  for peer in n.peers:
    let granted = await n.requestVoteFromPeer(peer)
    if granted and n.state == Candidate and n.currentTerm == currentElectionTerm:
      votes += 1
      if votes > (n.peers.len + 1) div 2:
        n.state = Leader
        n.logMsg("Elected as LEADER for term " & $n.currentTerm)
        # Initialize nextIndex for each peer
        for p in n.peers: n.nextIndex[p] = n.logEntries.len
        asyncCheck n.sendHeartbeats()
        break

proc proposeCommand*(n: RaftNode, command: string): bool =
  if n.state != Leader:
    return false
    
  let entry = LogEntry(term: n.currentTerm, command: command)
  n.logEntries.add(entry)
  n.saveState()
  n.logMsg("Leader proposed command: " & command)
  # In a full implementation, we'd wait for quorum before returning success
  # But for this phase, we'll let it replicate asynchronously
  return true

proc tick*(n: RaftNode) {.async.} =
  while true:
    await sleepAsync(50)
    let now = epochTime()
    if n.state != Leader and (now - n.lastHeartbeat) * 1000 > float(n.electionTimeout):
      await n.startElection()
      n.lastHeartbeat = epochTime()

proc start*(n: RaftNode) {.async.} =
  n.lastHeartbeat = epochTime()
  n.electionTimeout = rand(150..300)
  
  n.server.setSockOpt(OptReuseAddr, true)
  n.server.bindAddr(n.port)
  n.server.listen()
  n.logMsg("Listening on " & $int(n.port))
  
  asyncCheck n.tick()
  
  while true:
    let client = await n.server.accept()
    asyncCheck (proc() {.async.} =
      defer: client.close()
      while not client.isClosed():
        var line: string
        try:
          line = await client.recvLine()
        except: break
        
        if line == "": break
        try:
          let j = parseJson(line)
          if j["type"].getStr() == "AppendEntries":
            let term = j["term"].getInt()
            let leaderId = j["leaderId"].getStr()
            let prevLogIndex = j["prevLogIndex"].getInt()
            let prevLogTerm = j["prevLogTerm"].getInt()
            let leaderCommit = j["leaderCommit"].getInt()
            
            var entries: seq[LogEntry] = @[]
            for eJ in j["entries"]:
              entries.add(to(eJ, LogEntry))

            let success = n.handleAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)
            if not client.isClosed():
              await client.send($(%*{"success": success}) & "\n")
            
          elif j["type"].getStr() == "RequestVote":
            let term = j["term"].getInt()
            let candidateId = j["candidateId"].getStr()
            let lastLogIndex = j["lastLogIndex"].getInt()
            let lastLogTerm = j["lastLogTerm"].getInt()
            let granted = n.handleRequestVote(term, candidateId, lastLogIndex, lastLogTerm)
            if not client.isClosed():
              await client.send($(%*{"granted": granted}) & "\n")
        except:
          break
    )()

when isMainModule:
  let portStr = if paramCount() > 0: paramStr(1) else: "9001"
  let id = "node_" & portStr
  let port = parseInt(portStr)
  
  # Only clear if --clear is passed as 2nd arg
  if paramCount() > 1 and paramStr(2) == "--clear":
    if dirExists("db_" & id): removeDir("db_" & id)
  
  if not dirExists("db_" & id): createDir("db_" & id)
  let db = init("db_" & id)
  
  var peers: seq[string] = @[]
  for p in [9001, 9002, 9003]:
    if p != port:
      peers.add("localhost:" & $p)
      
  let node = newRaftNode(id, port, peers, db)
  
  # Log recovery status
  if node.logEntries.len > 0:
    node.logMsg("🔥 RECOVERED " & $node.logEntries.len & " entries from disk!")
  
  # Simulation: Any node that becomes leader proposes a command
  asyncCheck (proc() {.async.} =
    while true:
      await sleepAsync(1000)
      if node.state == Leader:
        node.logMsg("PROPOSING CLUSTER COMMAND...")
        let ok = node.proposeCommand($(%*{"op": "put", "key": "clu_key", "val": "clu_val", "bucket": "default"}))
        if ok: 
          await sleepAsync(2000) # Wait for replication
          node.logMsg("Verification (Leader): clu_key -> " & node.db.get("clu_key"))
          break
      elif node.logEntries.len > 0:
        await sleepAsync(2000)
        node.logMsg("Verification (Follower): clu_key -> " & node.db.get("clu_key"))
        break
  )()

  waitFor node.start()
