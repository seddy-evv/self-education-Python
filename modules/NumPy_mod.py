# NumPy, a fundamental library for numerical computations in Python, offers an extensive array of functions to perform
# operations on arrays and matrices. Below is a description of some of the main NumPy functions.
import numpy as np


# Array Creation

# 1. np.array()
# Description: Creates an array from a Python list or tuple.

# Example:
array = np.array([1, 2, 3])
print(array)

# 2. np.zeros()
# Description: Creates an array filled with zeros.

# Example:
zeros = np.zeros((3, 4))
print(zeros)

# 3. np.ones()
# Description: Creates an array filled with ones.

# Example:
ones = np.ones((2, 3))
print(ones)

# 4. np.eye()
# Description: Creates an identity matrix.

# Example:
identity = np.eye(3)
print(identity)

# 5. np.full()
# Description: Creates an array filled with a specified value.

# Example:
full_array = np.full((2, 2), 7)
print(full_array)

# 6. np.arange()
# Description: Generates values in a specified range with a step.

# Example:
range_array = np.arange(0, 10, 2)
print(range_array)

# 7. np.linspace()
# Description: Generates values evenly spaced between a start and end.

# Example:
linspace_array = np.linspace(0, 1, 5)
print(linspace_array)

# 8. np.random.rand()
# Description: Generates random values uniformly distributed between 0 and 1.

# Example:
random_uniform = np.random.rand(2, 3)
print(random_uniform)

# 9. np.random.randint()
# Description: Generates random integers.

# Example:
random_int = np.random.randint(0, 10, size=(2, 3))
print(random_int)


# Array Manipulation

# 1. np.reshape()
# Description: Reshapes an array into the specified dimensions.

# Example:
reshaped = np.reshape(np.arange(6), (2, 3))
print(reshaped)

# 2. np.transpose()
# Description: Transposes an array (reverses axes).

# Example:
transposed = np.transpose([[1, 2], [3, 4]])
print(transposed)

# 3. np.concatenate()
# Description: Concatenates arrays along a specified axis.

# Example:
a = np.array([[1, 2], [3, 4]])
b = np.array([[5, 6]])
concatenated = np.concatenate((a, b), axis=0)
print(concatenated)

# 4. np.stack()
# Description: Stacks arrays along a new axis.

# Example:
c = np.array([1, 2])
d = np.array([3, 4])
stacked = np.stack((c, d), axis=0)
print(stacked)

# 5. np.split()
# Description: Splits an array into multiple sub-arrays.

# Example:
split_array = np.split(np.arange(6), 3)
print(split_array)


# Mathematical Operations
a = np.array([1, 2, 3])
b = np.array([4, 5, 6])

# 1. np.add()
# Description: Adds two arrays element-wise.

# Example:
sum_array = np.add(a, b)
print(sum_array)

# 2. np.subtract()
# Description: Subtracts one array from another element-wise.

# Example:
difference_array = np.subtract(b, a)
print(difference_array)

# 3. np.multiply()
# Description: Multiplies two arrays element-wise.

# Example:
product_array = np.multiply(a, b)
print(product_array)

# 4. np.divide()
# Description: Divides one array by another element-wise.

# Example:
quotient_array = np.divide(b, a)
print(quotient_array)

# 5. np.dot()
# Description: Computes the dot product of two arrays.

# Example:
dot_product = np.dot(a, b)
print(dot_product)

# 6. np.sqrt()
# Description: Computes the square root element-wise.

# Example:
sqrt_array = np.sqrt([1, 4, 9])
print(sqrt_array)

# 7. np.power()
# Description: Raises elements to a specified power.

# Example:
powered = np.power([1, 2, 3], 2)
print(powered)

# 8. np.sum()
# Description: Computes the sum of array elements along a given axis.

# Example:
summation = np.sum([[1, 2], [3, 4]])
print(summation)

# 9. np.mean()
# Description: Computes the mean of array elements along a given axis.

# Example:
mean_value = np.mean([1, 2, 3, 4])
print(mean_value)

# 10. np.std()
# Description: Computes the standard deviation of array elements.

# Example:
std_deviation = np.std([1, 2, 3, 4])
print(std_deviation)

# 11. np.max() / np.min()
# Description: Finds the maximum/minimum value in an array.

# Example:
array = np.array([3, 1, 4, 1, 5])
print(np.max(array))
print(np.min(array))


# Logical Operations
array = np.array([1, -1, 0, 3, -5])

# 1. np.where()
# Description: Returns indices where a condition is true.

# Example:
indexes = np.where(array > 0)
print(indexes)

# 2. np.all()
# Description: Tests if all elements in an array are true.

# Example:
all_positive = np.all(array > 0)
print(all_positive)

# 3. np.any()
# Description: Tests if any element in an array is true.

# Example:
any_positive = np.any(array > 0)
print(any_positive)


# Array Properties
array = np.array([[1, 2], [3, 4]])

# 1. np.shape()
# Description: Returns the shape of an array.

# Example:
print(np.shape(array))

# 2. np.size()
# Description: Returns the total number of elements in an array.

# Example:
print(np.size(array))

# 3. np.ndim()
# Description: Returns the number of dimensions in an array.

# Example:
print(np.ndim(array))

# 4. np.dtype()
# Description: Returns the data type of array elements.

# Example:
print(array.dtype)
