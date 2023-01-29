# Decorators in Python
from functools import wraps
from time import perf_counter, sleep

def timer(func):
  """Times any function"""
  
  @wraps(func)
  def wrapper(*args, **kwargs):
    start_time = perf_counter()
    func(*args, **kwargs)
    end_time = perf_counter()
    print(f"Total time taken {end_time-start_time}")
  
  return wrapper

@timer
def say_hello() -> str:
  """Prints hello"""
  
  sleep(1)
  print("hello")
  
if __name__ == "__main__":
  say_hello()
  print(say_hello.__doc__)