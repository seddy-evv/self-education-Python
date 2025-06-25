# The easiest way to parallelize tasks in Python is by using the concurrent.futures module, which provides a clean
# and high-level interface for asynchronously executing tasks using threads or processes.
#
# The two main classes are:
# ThreadPoolExecutor for parallelizing tasks with threading.
# ProcessPoolExecutor for parallelizing tasks with process-based parallelism.

# ThreadPoolExecutor:
from concurrent.futures import ThreadPoolExecutor


# A sample function to demonstrate parallel tasks
def task(n):
    print(f"Task {n} is starting...")
    return f"Task {n} result"


# List of tasks to run in parallel
tasks = [1, 2, 3, 4, 5]

# Create a ThreadPoolExecutor to manage a pool of threads
with ThreadPoolExecutor() as executor:
    # Submit tasks to the ThreadPoolExecutor
    # executor.map: Maps the function task across the provided tasks list, executing them in parallel.
    results_threads = list(executor.map(task, tasks))
    print("Results:", results_threads)


# ProcessPoolExecutor:
from concurrent.futures import ProcessPoolExecutor


# A CPU-intensive sample function
def cpu_task(n):
    print(f"Processing {n}...")
    return sum(i * i for i in range(n))


# List of input sizes for the CPU-heavy function
inputs = [100, 200, 300]

if __name__ == '__main__':
    # Create a ProcessPoolExecutor to manage parallel processes
    with ProcessPoolExecutor() as executor:
        # Submit tasks to the ProcessPoolExecutor
        results = list(executor.map(cpu_task, inputs))
        print("Results:", results)

# Key Differences Between Threads and Processes
# ThreadPoolExecutor: Best suited for I/O-bound tasks (e.g., web requests, file reading).
# Easier and faster to start, but limited by Python's GIL.
# ProcessPoolExecutor: Best suited for CPU-bound tasks (e.g., heavy computations),
# as it runs tasks in separate processes and avoids the GIL.
