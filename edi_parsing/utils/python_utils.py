import os
import json

from functools import wraps
from typing import get_args
from dataclasses import asdict


def get_subtype(field):
  args = get_args(field)
  if len(args) == 1:
    return args[0]
  return None



def write_json(data, filename):
  directory = os.path.dirname(filename)
  if not os.path.exists(directory):
    os.makedirs(directory)

  with open(filename, "w+") as outfile:
    json.dump(data, outfile, indent=2)



def as_dict(dataclass_list: list):
  return [asdict(d) for d in dataclass_list]


def progress(list_values):
  
  def inner(function):
    total = len(list_values)
    @wraps(function)
    def wrapper(*args, **kwargs):

      index = list_values.index(args[0])
      progress_percent = (index+1)/total*100
      element_string_ratio = f"{index+1}/{total}"

      print(f"{'':#<15} Progress : {progress_percent:.2f}% - processing element {element_string_ratio} {'':#>15}")

      return function(*args, **kwargs)
      
    return wrapper
  
  return inner