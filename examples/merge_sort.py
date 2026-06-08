import multiprocessing
import os
import time
import random
from moveitmoveit import Deque, Empty, Abort

def merge_sort_parallel(arr, task_id, shared_results, group_name="mergesort_pool", next_id_container=None):
    q = Deque(group_name)
    
    if len(arr) <= 1:
        shared_results[task_id] = arr
        return

    mid = len(arr) // 2
    left_half = arr[:mid]
    right_half = arr[mid:]

    # Generate unique IDs for the sub-tasks
    # We use a managed list containing a counter to keep IDs unique across processes
    left_id = f"{task_id}_L"
    right_id = f"{task_id}_R"

    q.put((right_half, right_id))
    
    merge_sort_parallel(left_half, left_id, shared_results, group_name)

    # Check if a peer stole the right half
    try:
        task = q.get()
        merge_sort_parallel(task[0], task[1], shared_results, group_name)
    except Empty:
        # Work stolen
        pass

    while left_id not in shared_results or right_id not in shared_results:
        time.sleep(0.001)

    # Merge the results
    left_sorted = shared_results[left_id]
    right_sorted = shared_results[right_id]
    
    shared_results[task_id] = merge(left_sorted, right_sorted)

    del shared_results[left_id]
    del shared_results[right_id]


def merge(left, right):
    result = []
    i = j = 0
    while i < len(left) and j < len(right):
        if left[i] < right[j]:
            result.append(left[i])
            i += 1
        else:
            result.append(right[j])
            j += 1
    result.extend(left[i:])
    result.extend(right[j:])
    return result


def worker_loop(group_name, shared_results):
    q = Deque(group_name)
    
    while True:
        try:
            # Steal from the heaviest queue load
            sub_arr, task_id = q.steal(target_longest=True)
            
            merge_sort_parallel(sub_arr, task_id, shared_results, group_name)
        except Empty, Abort:
            time.sleep(0.01)
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    GROUP_NAME = "ms_ipc_group"
    data_to_sort = random.choices(range(1, 101), k=50)
    
    print(f"Original Array: {data_to_sort}")

    with multiprocessing.Manager() as manager:
        shared_results = manager.dict()
        
        workers = []
        for _ in range(3):
            p = multiprocessing.Process(target=worker_loop, args=(GROUP_NAME, shared_results))
            p.daemon = True
            p.start()
            workers.append(p)

        time.sleep(0.1)

        ROOT_TASK_ID = "root"
        merge_sort_parallel(data_to_sort, ROOT_TASK_ID, shared_results, GROUP_NAME)
        
        sorted_data = shared_results[ROOT_TASK_ID]
        print(f"Sorted Array:   {sorted_data}")

