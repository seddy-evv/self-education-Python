# The concurrent.futures module in Python provides a high-level interface for asynchronous and parallel execution of
# tasks using threads or processes. Below are the main functionalities and components of concurrent.futures:

# The module provides two types of executors to manage a pool of workers:
#
# ThreadPoolExecutor: For parallel tasks using threads (for I/O-bound operations).
# ProcessPoolExecutor: For parallel tasks using processes (for CPU-bound operations).

# 1. Executor.submit(fn, *args, **kwargs)
# Submits a function fn to the pool for execution and returns a Future object.
# A Future represents a "promise" to fetch results once the task completes.

# Example:
from concurrent.futures import ThreadPoolExecutor

def task(x):
    return x * x

with ThreadPoolExecutor() as executor:
    future = executor.submit(task, 4)
    print(f"Result: {future.result()}")  # Output: Result: 16

# 2. Executor.map(fn, *iterables)
# A convenient method to apply a function fn to every item in one or more iterables (like map, but parallelized).
# Collects results in the order of input items.

# Example:
from concurrent.futures import ProcessPoolExecutor

def task(x):
    return x * x

inputs = [1, 2, 3, 4, 5]
with ProcessPoolExecutor() as executor:
    results = executor.map(task, inputs)
    print(list(results))  # Output: [1, 4, 9, 16, 25]

# 3. Future.result(timeout=None)
# Waits for the computation to complete and retrieves the result of the submitted task.
# If the task raises an exception, result() re-raises it.
# You can set a timeout in seconds if needed.

# Example:
from concurrent.futures import ThreadPoolExecutor
import time

def task(x):
    time.sleep(1)
    return x * 2

with ThreadPoolExecutor() as executor:
    future = executor.submit(task, 10)
    print(future.result())  # Blocks until the task is done, output: 20

# 4. Future.done()
# Returns True if the task has completed (successfully or with an exception).
# Useful to check task completion without blocking.

# Example:
from concurrent.futures import ThreadPoolExecutor
import time

def task(x):
    time.sleep(2)
    return x * x

with ThreadPoolExecutor() as executor:
    future = executor.submit(task, 5)
    print(future.done())  # Output: False (not completed yet)
    time.sleep(2)         # Wait for completion
    print(future.done())  # Output: True (now completed)

# 5. Future.add_done_callback(fn)
# Adds a callback function that is called when the Future completes.
# Useful for handling results as soon as they are ready without blocking.

# Example:
from concurrent.futures import ThreadPoolExecutor

def task(x):
    return x * x

def callback(future):
    print(f"Task completed with result: {future.result()}")

with ThreadPoolExecutor() as executor:
    future = executor.submit(task, 6)
    future.add_done_callback(callback)

# 6. as_completed(futures, timeout=None)
# Takes an iterable of Future objects and yields them as they complete.
# Useful when you want to process results in the order of completion (not in the order of submission).

# Example:
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def task(x):
    time.sleep(x)
    return x

with ThreadPoolExecutor() as executor:
    futures = [executor.submit(task, i) for i in [3, 1, 2]]
    for future in as_completed(futures):
        print(f"Completed with result: {future.result()}")
# Output:
# Completed with result: 1
# Completed with result: 2
# Completed with result: 3

# 7. wait(futures, timeout=None, return_when=ALL_COMPLETED)
# Blocks until a group of Future objects meet a specific condition:
# ALL_COMPLETED: Wait for all tasks to complete.
# FIRST_COMPLETED: Wait for the first task to complete.
# FIRST_EXCEPTION: Wait for the first exception to be raised (if any).
# Returns a tuple of completed and not-completed tasks.

# Example:
from concurrent.futures import ThreadPoolExecutor, wait

def task(x):
    return x * x

with ThreadPoolExecutor() as executor:
    futures = [executor.submit(task, i) for i in [1, 2, 3]]
    done, not_done = wait(futures)
    print("Done:", len(done))  # Output: Done: 3
    print("Not done:", len(not_done))  # Output: Not done: 0

# 8. Shutting Down Executors
# To clean up, the executor must shut down once tasks are complete. The following methods are used:
# Executor.shutdown(wait=True): Signals the executor to stop accepting new tasks and waits for
# pending tasks to complete (if wait=True).
