import multiprocessing
import time
from moveitmoveit import Deque, Empty

def run_worker(process_id: int):
    # Connects to the same IPC group using the unique group name
    q = Deque("shared_pool")
    
    # Process 0 acts as the producer
    if process_id == 0:
        for i in range(3):
            print(f"[Process {process_id}] Putting: Task {i}")
            q.put(f"Task {i}")
            time.sleep(0.1)
            
    # Process 1 acts as the consumer
    elif process_id == 1:
        time.sleep(0.2)  # Give process 0 a moment to populate its queue
        while True:
            try:
                # Local queue is empty, so we steal from Process 0
                item = q.steal()
                print(f"[Process {process_id}] Stole: {item}")
            except Empty:
                print(f"[Process {process_id}] No items left to steal. Exiting.")
                break

if __name__ == "__main__":
    p0 = multiprocessing.Process(target=run_worker, args=(0,))
    p1 = multiprocessing.Process(target=run_worker, args=(1,))
    
    p0.start()
    p1.start()
    
    p0.join()
    p1.join()

