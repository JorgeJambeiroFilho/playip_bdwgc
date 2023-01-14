

class LRUCacheNode:

    def __init__(self):
        self.next = None
        self.prev = None
        self.obj = None
        self.key = None

class LRUCache:

    async def load(self, key):
        raise Exception("Not implemented")

    async def save(self, key, obj):
        raise Exception("Not implemented")

    def __init__(self, limit):
        self.map = {}
        self.head = LRUCacheNode()
        self.head.next = self.head
        self.head.prev = self.head
        self.limit = limit
        self.num = 0
        self.closed = False

    def removeNode(self, node):
        node.prev.next = node.next
        node.next.prev = node.prev

    def addNodeAfter(self, node, prev_node):
        node.prev = prev_node
        node.next = prev_node.next
        node.prev.next = node
        node.next.prev = node

    def registerHit(self):
        pass

    async def get(self, key):
        if self.closed:
            raise Exception("Cache usado após fechamento")
        if key in self.map:
            node = self.map[key]
            self.removeNode(node)
            self.addNodeAfter(node, self.head)
            self.registerHit()
            return node.obj
        else:
            obj = await self.load(key)
            node = LRUCacheNode()
            node.obj = obj
            node.key = key
            self.addNodeAfter(node, self.head)
            self.map[key] = node
            self.num += 1
            if self.num > self.limit:
                node = self.head.prev
                self.removeNode(node)
                del self.map[node.key]
                await self.save(node.key, node.obj)
                self.num -= 1
            return obj

    async def close(self):
        self.closed = True
        while self.head.prev != self.head:
            node = self.head.prev
            self.removeNode(node)
            await self.save(node.key, node.obj)

