import asyncdispatch, asyncnet, strutils, envelopedb, json, tables, streams

proc processClient(client: AsyncSocket, db: EnvelopeDB) {.async.} =
  var currentBucket = "default"
  
  while true:
    let line = await client.recvLine()
    if line == "": break
    
    let parts = line.split()
    if parts.len == 0: continue
    
    let cmd = parts[0].toUpperAscii()
    case cmd
    of "SET":
      if parts.len < 3:
        await client.send("-ERR wrong number of arguments for 'SET'\c\l")
        continue
      db.put(parts[1], parts[2..^1].join(" "), currentBucket)
      await client.send("+OK\c\l")
      
    of "GET":
      if parts.len < 2:
        await client.send("-ERR wrong number of arguments for 'GET'\c\l")
        continue
      let val = db.get(parts[1], currentBucket)
      if val == "":
        await client.send("$-1\c\l")
      else:
        await client.send("$" & $val.len & "\c\l" & val & "\c\l")
        
    of "JGET":
      if parts.len < 3:
        await client.send("-ERR wrong number of arguments for 'JGET'\c\l")
        continue
      let val = db.get(parts[1], currentBucket)
      if val == "":
        await client.send("$-1\c\l")
      else:
        try:
          let j = parseJson(val)
          let field = parts[2]
          if j.hasKey(field):
            let res = if j[field].kind == JString: j[field].getStr() else: $j[field]
            await client.send("$" & $res.len & "\c\l" & res & "\c\l")
          else:
            await client.send("-ERR field not found\c\l")
        except:
          await client.send("-ERR invalid JSON\c\l")

    of "DEL":
      if parts.len < 2:
        await client.send("-ERR wrong number of arguments for 'DEL'\c\l")
        continue
      db.delete(parts[1], currentBucket)
      await client.send("+OK\c\l")
      
    of "USE":
      if parts.len < 2:
        await client.send("-ERR wrong number of arguments for 'USE'\c\l")
        continue
      currentBucket = parts[1]
      await client.send("+OK\c\l")
      
    of "LIST":
      if parts.len > 1 and parts[1].toUpperAscii() == "BUCKETS":
        var bucketList = ""
        for b in db.buckets.keys:
          bucketList &= b & " "
        await client.send("+" & bucketList.strip() & "\c\l")
      else:
        await client.send("-ERR unknown subcommand for 'LIST'\c\l")

    of "MERGE":
      db.merge()
      await client.send("+OK\c\l")
      
    of "FLUSH":
      db.activeFile.flush()
      await client.send("+OK\c\l")

    of "PING":
      await client.send("+PONG\c\l")
      
    of "QUIT":
      await client.send("+OK\c\l")
      client.close()
      break
      
    else:
      await client.send("-ERR unknown command '" & cmd & "'\c\l")

proc main() {.async.} =
  var db = envelopedb.init("db_data")
  let server = newAsyncSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(8888))
  server.listen()
  
  echo "🚀 EnvelopeDB Server started at :8888"
  echo "💡 Tip: Press Ctrl+C to shut down gracefully."
  
  while true:
    # Accept clients
    let acceptFuture = server.accept()
    yield acceptFuture
    if acceptFuture.failed: break
    
    let client = acceptFuture.read()
    echo "Client connected: ", client.getPeerAddr()
    asyncCheck processClient(client, db)

# Set up global shutdown logic
import os
setControlCHook(proc() {.noconv.} =
  echo "\n🛑 Shutting down EnvelopeDB gracefully..."
  # Note: In a real server, we would signal the loop to stop.
  # For now, we'll just exit, as 'close' is called in a simpler way below.
  quit(0)
)

waitFor main()
