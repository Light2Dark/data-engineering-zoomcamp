# iterators and generators

x = [1,2,3,4,5,6,7,8,9,10]

# map returns an iterator, not an actual list.
# List only gets created when we call list() on it / iterate over it
y = map(lambda i: i**2, x)

next(y) # similar to y.__next__()
# for i in y:
#   print(i)
  
l = iter(range(1,10))
# print(next(l))

# yield will pause the function and return the value until the next() function is called on it again. 
# the state of the function (variables, mem) is saved during a yield
def gen(x: int):
  yield 1 
  print("after yield 1")
  yield 2
  print("after yield 2")
  yield 3
  
m = gen(5)
print(next(m))
print(next(m))
print(next(m))

# Use case: don't need to store all the values in memory, just iterate one item at a time
def csv_reader(file_path: str):
  for row in open(file_path, "r"):
    yield row
    
# generator comprehension
tty = (i for i in range(1,10))
print(tty)