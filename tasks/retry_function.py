# Create python high order retry function. it takes a function as an argument. if it fails 3 times it
# returns the fail and error. If it was successful it returns the result.
import random


def retry(func, attempts=3):
    """
    High-order retry function that retries the given function up to 3 times.
    If the function fails all 3 times, it returns the failure and the error.
    If it succeeds, it returns the result.
    """
    def wrapper(*args, **kwargs):
        for attempt in range(1, attempts + 1):
            try:
                # Try to execute the function
                result = func(*args, **kwargs)
                print(f"Attempt {attempt}: Success")
                return result  # Return result if successful
            except Exception as e:
                last_exception = e
        # If all attempts fail, return the error
        print("All attempts failed.")
        return {"success": False, "error": last_exception}

    return wrapper


def unreliable_function():
    """
    A sample function that may fail randomly
    """
    if random.random() < 0.7:  # 70% chance of failure
        raise ValueError("Something went wrong!")
    return "Success!"


# Example usage:
if __name__ == "__main__":

    # Wrap the unreliable function with the retry mechanism
    wrapped_function = retry(unreliable_function)

    # Call the wrapped function
    result = wrapped_function()

    print(result)
