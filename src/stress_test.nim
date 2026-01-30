import asyncdispatch, asyncnet, strutils, times, os

const
  NumClients = 50
  OpsPerClient = 200
  ServerAddr = "127.0.0.1"
  ServerPort = 8888

proc runClient(clientId: int) {.async.} =
  let socket = newAsyncSocket()
  try:
    await socket.connect(ServerAddr, Port(ServerPort))
    
    for i in 1..OpsPerClient:
      let key = "client_" & $clientId & "_key_" & $i
      let val = "value_" & $i
      
      # SET
      await socket.send("SET " & key & " " & val & "\c\l")
      let resSet = await socket.recvLine()
      
      # GET
      await socket.send("GET " & key & "\c\l")
      let header = await socket.recvLine()
      if header == "$-1":
        echo "Key not found: ", key
        continue
        
      let data = await socket.recvLine()
      if data != val:
        echo "Mismatch for ", key, ": expected ", val, " got ", data

      if i mod 50 == 0:
        echo "Client ", clientId, " progress: ", i, "/", OpsPerClient

  except Exception as e:
    echo "Error in client ", clientId, ": ", e.msg
  finally:
    socket.close()

proc main() {.async.} =
  echo "🔥 Starting Stress Test: ", NumClients, " clients, ", OpsPerClient, " ops each..."
  let start = cpuTime()
  
  var clients: seq[Future[void]] = @[]
  for i in 1..NumClients:
    clients.add(runClient(i))
    
  await all(clients)
  
  let elapsed = cpuTime() - start
  let totalOps = NumClients * OpsPerClient * 2 # SET + GET
  echo "✅ Finished!"
  echo "Total Operations: ", totalOps
  echo "Time Elapsed: ", elapsed.formatFloat(ffDecimal, 2), "s"
  echo "Throughput: ", (totalOps.float / elapsed).formatFloat(ffDecimal, 2), " ops/s"

waitFor main()
