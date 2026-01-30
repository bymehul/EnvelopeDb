import random

type
  MemTableNode[K, V] = ref object
    key: K
    value: V
    next: seq[MemTableNode[K, V]]

  MemTable*[K, V] = ref object
    head: MemTableNode[K, V]
    maxLevel: int
    level: int
    count*: int

proc newMemTable*[K, V](maxLevel: int = 16): MemTable[K, V] =
  new(result)
  result.maxLevel = maxLevel
  result.level = 0
  result.count = 0
  new(result.head)
  result.head.next = newSeq[MemTableNode[K, V]](maxLevel)

proc randomLevel(sl: MemTable): int =
  result = 0
  while rand(1.0) < 0.5 and result < sl.maxLevel - 1:
    inc result

proc put*[K, V](sl: MemTable[K, V], key: K, value: V) =
  var update = newSeq[MemTableNode[K, V]](sl.maxLevel)
  var curr = sl.head
  
  for i in countdown(sl.level, 0):
    while curr.next[i] != nil and curr.next[i].key < key:
      curr = curr.next[i]
    update[i] = curr
    
  curr = curr.next[0]
  
  if curr != nil and curr.key == key:
    curr.value = value
  else:
    let lvl = sl.randomLevel()
    if lvl > sl.level:
      for i in sl.level + 1 .. lvl:
        update[i] = sl.head
      sl.level = lvl
      
    let newNode = MemTableNode[K, V](key: key, value: value)
    newNode.next = newSeq[MemTableNode[K, V]](lvl + 1)
    for i in 0 .. lvl:
      newNode.next[i] = update[i].next[i]
      update[i].next[i] = newNode
    inc sl.count

proc get*[K, V](sl: MemTable[K, V], key: K): (bool, V) =
  var curr = sl.head
  for i in countdown(sl.level, 0):
    while curr.next[i] != nil and curr.next[i].key < key:
      curr = curr.next[i]
  
  curr = curr.next[0]
  if curr != nil and curr.key == key:
    return (true, curr.value)
  var empty: V
  return (false, empty)

iterator items*[K, V](sl: MemTable[K, V]): (K, V) =
  var curr = sl.head.next[0]
  while curr != nil:
    yield (curr.key, curr.value)
    curr = curr.next[0]

when isMainModule:
  var sl = newMemTable[string, string]()
  sl.put("a", "1")
  sl.put("c", "3")
  sl.put("b", "2")
  
  for k, v in sl.items:
    echo k, ": ", v
