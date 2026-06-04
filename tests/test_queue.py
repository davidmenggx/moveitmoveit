from moveitmoveit import Deque, Empty, Abort
import pytest

def test_construction():
    q = Deque('test')

def test_put_get():
    q = Deque('test')
    q.put(1)
    
    sz = q.qsize()
    assert sz == 1, f'Invalid size: expected 1, received {sz}'

    item = q.get()
    assert item == 1, f'Invalid item from `get()`: expected 1, received {item}'
    
    sz = q.qsize()
    assert sz == 0, f'Invalid size: expected 0, received {sz}'

    mt = q.empty()
    assert mt, f'Invalid size: expected `q.empty() == True`, received `q.empty() == {mt}`'

    with pytest.raises(Empty):
        item = q.get()

def test_various_type_objs():
    q = Deque('test')

    q.put(1)
    q.put('hello world')
    q.put([3, 'goodbye', 0.75])

    item = q.get()
    assert item == [3, 'goodbye', 0.75] # Get is FIFO

    item = q.get()
    assert item == 'hello world'

    item = q.get()
    assert item == 1

def test_steal_basic():
    q1 = Deque('test')
    q2 = Deque('test')

    with pytest.raises(Empty):
        item = q1.steal()
        item = q2.steal()

    q1.put('i like to')

    with pytest.raises(Empty): # `q1` receives `Empty` because `q2` is empty and you cannot steal from yourself
        item = q1.steal()

    item = q2.steal()
    assert item == 'i like to', f"Invalid item from `steal()`: expected 'i like to', received {item}"

    with pytest.raises(Empty):
        item = q1.steal()
        item = q2.steal()

def test_steal_longest():
    q1 = Deque('test')
    q2 = Deque('test')
    q3 = Deque('test')

    q2.put('apple')
    q3.put('banana')
    q3.put('pear')

    item = q1.steal(target_longest=True)
    assert item == 'banana', f"Invalid item from `steal()`: expected 'banana', received {item}" # Steal is LIFO, longest queue is q3 (size 2).

def test_many_queues():
    queues = []
    for i in range(64):
        queues.append(Deque('test'))

    # Maximum 64 processes in a queue group
    with pytest.raises(RuntimeError):
        invalid = Deque('test')

