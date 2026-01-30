import asyncnet, asyncdispatch, strutils

proc runTest() {.async.} =
  let client = newAsyncSocket()
  await client.connect("localhost", Port(8888))
  
  echo "--- Testing Namespaces ---"
  await client.send("USE PROFILES\c\l")
  echo "USE result: ", await client.recvLine()
  
  await client.send("SET user:1 {\"name\":\"John\",\"age\":30}\c\l")
  echo "SET result: ", await client.recvLine()
  
  await client.send("LIST BUCKETS\c\l")
  echo "LIST BUCKETS result: ", await client.recvLine()
  
  echo "--- Testing JSON JGET ---"
  await client.send("JGET user:1 name\c\l")
  echo "JGET name (size): ", await client.recvLine()
  echo "JGET name (val): ", await client.recvLine()
  
  await client.send("JGET user:1 age\c\l")
  echo "JGET age (size): ", await client.recvLine()
  echo "JGET age (val): ", await client.recvLine()
  
  client.close()

waitFor runTest()
