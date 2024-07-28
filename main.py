from RDQueue.client.dq import RDQueue
import asyncio

q = RDQueue(name='test', address=('192.168.0.101', 5000))
q.push('test1')
q.push('test2')
q.push('test3')

print(q.pop())
print(q.pop())
print(q.pop())
