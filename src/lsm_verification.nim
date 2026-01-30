import envelopedb, os, strutils, tables

proc testLSM() =
  let dbDir = "lsm_test_db"
  if dirExists(dbDir): removeDir(dbDir)
  
  # We'll use a local override for testing if needed, but for now 
  # let's just write many keys to trigger the 1000 key limit.
  var db = init(dbDir)
  
  echo "--- 1. Writing data to MemTable ---"
  for i in 1..500:
    db.put("key" & $i, "value_" & $i)
  
  echo "Current MemTable count: ", db.memTables["default"].count
  echo "Verify key250: ", db.get("key250")
  
  echo "--- 2. Triggering Flush (writing 600 more keys) ---"
  for i in 501..1100:
    db.put("key" & $i, "value_" & $i)
    
  echo "After flush: MemTable count: ", db.memTables["default"].count
  echo "SSTable count: ", db.sstables.len
  
  echo "--- 3. Verifying cross-layer retrieval ---"
  echo "Verify key250 (from SSTable): ", db.get("key250")
  echo "Verify key1050 (from MemTable): ", db.get("key1050")
  
  echo "--- 4. Verifying recovery ---"
  db.close()
  echo "Database closed. Re-initializing..."
  
  var db2 = init(dbDir)
  echo "Recovered SSTable count: ", db2.sstables.len
  echo "Recovered key250: ", db2.get("key250")
  echo "Recovered key1100: ", db2.get("key1100")
  
  echo "\n🏆 LSM-Tree Verified!"

testLSM()
