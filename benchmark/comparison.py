import time
import multiprocessing as mp
import faster_fifo
import moveitmoveit

COUNTER_BATCH_SIZE = 1000

def standard_mpmc_producer(q, messages_to_send, start_barrier):
    start_barrier.wait()
    for _ in range(messages_to_send):
        q.put(1)

def standard_mpmc_consumer(q, start_barrier, counter):
    start_barrier.wait()
    local_count = 0
    while True:
        q.get()
        local_count += 1
        
        if local_count >= COUNTER_BATCH_SIZE:
            with counter.get_lock():
                counter.value += local_count
            local_count = 0

def ff_batch_mpmc_producer(q, messages_to_send, start_barrier):
    batch_size = 10_000
    start_barrier.wait()
    count = 0
    preallocated_batch = [1] * batch_size
    while count < messages_to_send:
        current_batch = min(batch_size, messages_to_send - count)
        q.put_many(preallocated_batch)
        count += current_batch

def ff_batch_mpmc_consumer(q, start_barrier, counter):
    start_barrier.wait()
    while True:
        try:
            messages = q.get_many(block=True) 
            with counter.get_lock():
                counter.value += len(messages)
        except faster_fifo.Empty:
            time.sleep(0.00001)
            continue

def moveit_mpmc_producer(queue_name, messages_to_send, start_barrier):
    q = moveitmoveit.Deque(queue_name)
    start_barrier.wait()
    count = 0
    while count < messages_to_send:
        try:
            q.put(1)
            count += 1
        except (moveitmoveit.Abort, moveitmoveit.Empty):
            time.sleep(0.00001)
            continue
    while True: # Sleep so the deque's data doesn't die before the consumer is finished.
        time.sleep(1)

def moveit_mpmc_consumer(queue_name, start_barrier, counter):
    q = moveitmoveit.Deque(queue_name)
    start_barrier.wait()
    local_count = 0
    while True:
        try:
            _ = q.steal(target_first=True)
            local_count += 1
            if local_count >= COUNTER_BATCH_SIZE:
                with counter.get_lock():
                    counter.value += local_count
                local_count = 0
        except (moveitmoveit.Abort, moveitmoveit.Empty):
            time.sleep(0.00001)
            continue

def moveit_view_mpmc_producer(queue_name, messages_to_send, start_barrier):
    q = moveitmoveit.Deque(queue_name)
    start_barrier.wait()
    count = 0
    while count < messages_to_send:
        try:
            q.put_buffer(b"1")
            count += 1
        except (moveitmoveit.Abort, moveitmoveit.Empty):
            time.sleep(0.00001)
            continue
    while True: # Sleep so the deque's data doesn't die before the consumer is finished.
        time.sleep(1)

def moveit_view_mpmc_consumer(queue_name, start_barrier, counter):
    q = moveitmoveit.Deque(queue_name)
    start_barrier.wait()
    local_count = 0
    while True:
        try:
            _ = q.steal_view(target_first=True)
            local_count += 1
            if local_count >= COUNTER_BATCH_SIZE:
                with counter.get_lock():
                    counter.value += local_count
                local_count = 0
        except (moveitmoveit.Abort, moveitmoveit.Empty):
            time.sleep(0.00001)
            continue

def run_mpmc_throughput_test(name, queue_arg, producer_fn, consumer_fn, num_messages, num_producers, num_consumers):
    print(f"--- Benchmarking {name} ({num_producers}P / {num_consumers}C) ---")
    
    # Barrier to prevent process start-up jitter
    start_barrier = mp.Barrier(num_producers + num_consumers + 1)
    consumed_counter = mp.Value('i', 0)
    
    producers = []
    consumers = []
    
    max_trapped_messages = num_consumers * COUNTER_BATCH_SIZE 
    
    total_to_produce = num_messages + max_trapped_messages + 5000 
    msgs_per_prod = total_to_produce // num_producers
    
    for _ in range(num_consumers):
        p = mp.Process(target=consumer_fn, args=(queue_arg, start_barrier, consumed_counter))
        p.daemon = True 
        p.start()
        consumers.append(p)
        
    for i in range(num_producers):
        count = msgs_per_prod + (total_to_produce % num_producers if i == 0 else 0)
        p = mp.Process(target=producer_fn, args=(queue_arg, count, start_barrier))
        p.start()
        producers.append(p)

    start_barrier.wait()
    start_time = time.perf_counter()
    
    while consumed_counter.value < num_messages:
        time.sleep(0.001)
        
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    for c in consumers:
        c.terminate()
        
    for p in producers:
        p.terminate()
        
    for c in consumers:
        c.join()
        
    for p in producers:
        p.join()
        
    throughput = num_messages / elapsed
    
    print(f"Messages sent/received: {num_messages:,}")
    print(f"Time elapsed:           {elapsed:.4f} seconds")
    print(f"Throughput:             {throughput:,.2f} msgs/sec\n")

if __name__ == "__main__":
    mp.set_start_method('spawn', force=True)
    
    NUM_MESSAGES = 10_000_000
    BUFFER_SIZE = 20_000
    PRODUCERS = 8
    CONSUMERS = 8
    
    mp_queue = mp.Queue()
    run_mpmc_throughput_test(
        name="multiprocessing.Queue",
        queue_arg=mp_queue,
        producer_fn=standard_mpmc_producer,
        consumer_fn=standard_mpmc_consumer,
        num_messages=NUM_MESSAGES,
        num_producers=PRODUCERS,
        num_consumers=CONSUMERS
    )
    
    ff_queue = faster_fifo.Queue(NUM_MESSAGES + BUFFER_SIZE) 
    run_mpmc_throughput_test(
        name="faster_fifo.Queue",
        queue_arg=ff_queue,
        producer_fn=standard_mpmc_producer,
        consumer_fn=standard_mpmc_consumer,
        num_messages=NUM_MESSAGES,
        num_producers=PRODUCERS,
        num_consumers=CONSUMERS
    )

    ff_batch_queue = faster_fifo.Queue(NUM_MESSAGES + BUFFER_SIZE) 
    run_mpmc_throughput_test(
        name="faster_fifo.Queue (batched)",
        queue_arg=ff_batch_queue,
        producer_fn=ff_batch_mpmc_producer,
        consumer_fn=ff_batch_mpmc_consumer,
        num_messages=NUM_MESSAGES,
        num_producers=PRODUCERS,
        num_consumers=CONSUMERS
    )

    run_mpmc_throughput_test(
        name="moveitmoveit.Deque",
        queue_arg="test normal",
        producer_fn=moveit_mpmc_producer,
        consumer_fn=moveit_mpmc_consumer,
        num_messages=NUM_MESSAGES,
        num_producers=PRODUCERS,
        num_consumers=CONSUMERS
    )

    run_mpmc_throughput_test(
        name="moveitmoveit.Deque (view)",
        queue_arg="test view",
        producer_fn=moveit_view_mpmc_producer,
        consumer_fn=moveit_view_mpmc_consumer,
        num_messages=NUM_MESSAGES,
        num_producers=PRODUCERS,
        num_consumers=CONSUMERS
    )

# All tests run on Ubuntu 26 VM (6 cores, 16 GB RAM), Python 3.14

# --- Version 1.0.0 ---

# --- 1 producer, 1 consumer ---
# multiprocessing = 404,195.33 msgs/sec 
# faster_fifo base = 338,045.42 msgs/sec 
# faster_fifo batched = 523,868.06 msgs/sec
# moveitmoveit base = 2,269,042.90 msgs/sec
# moveitmoveit view = 4,402,994.12 msgs/sec

# --- 2 producer, 2 consumer ---
# multiprocessing = 188,806.36 msgs/sec 
# faster_fifo = 462,771.59 msgs/sec
# faster_fifo batched = 772,413.21 msgs/sec
# moveitmoveit = 2,602,719.55 msgs/sec
# moveitmoveit view = 3,740,357.00 msgs/sec

# --- 4 producer, 4 consumer ---
# multiprocessing = 120,670.44 msgs/sec 
# faster_fifo = 512,383.61 msgs/sec
# faster_fifo batched = 1,198,709.41 msgs/sec
# moveitmoveit = 2,189,860.25 msgs/sec
# moveitmoveit view = 2,498,403.05 msgs/sec

# --- 8 producer, 8 consumer ---
# multiprocessing = 97,277.10 msgs/sec 
# faster_fifo = 442,678.35 msgs/sec
# faster_fifo batched = 1,406,440.11 msgs/sec
# moveitmoveit = 2,001,780.47 msgs/sec
# moveitmoveit view = 2,179,138.19 msgs/sec
