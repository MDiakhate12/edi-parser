import re
from typing import Union, List
from dataclasses import Field


from pydifact.segmentcollection import RawSegmentCollection


def read_edi_segments(segments_pattern: str, edi_file_path:str = None, edi_string:str = None) -> list:
  
  if edi_file_path is None and edi_string is None:
      raise ValueError("You must provide either edi_file_path or edi_string.")
  if edi_file_path is not None and edi_string is not None:
      raise ValueError("You can only provide either edi_file_path or edi_string, not both.")

  if edi_string:
     edi_content = edi_string
  else:
    with open(edi_file_path, "r") as f:
       edi_content = f.read()

  segments = re.compile(pattern=segments_pattern, flags=re.DOTALL).findall(edi_content)

  return segments


def find_last_segment(segment_group):

    segments = segment_group.strip().split("'")

    print("FIND LAST SEGMENTS:", segments)
    print("FIND LAST SEGMENTS REVERSE:", list(reversed(segments)))

    reversed_segments = [s for s in list(reversed(segments)) if s.strip() != ""]

    for segment in reversed_segments:
        if segment.strip():
            return segment.strip() + "'"

    return None


def escape_regex_chars(string: str) -> str:
  """Escapes all regex-escapable characters of a string."""
  escaped_string = re.sub(r"([{}\[\]|()])", r"\\\1", string, flags=re.DOTALL)
  return escaped_string.replace(" ", "\s").replace("+", "\+").replace(".", "\.")


def get_segment_pattern_from_field(field: Field) -> str:

  qualifier = field.metadata.get('qualifier', None)
  subqualifier = field.metadata.get('subqualifier', None)

  qualifier = f"(?:(?:{qualifier}))" if qualifier else ""
  subqualifier = f"(?:\+(?:{subqualifier}))" if subqualifier else ""

  segment_pattern = f"{field.name[:3]}\+{qualifier}{subqualifier}"

  return segment_pattern


def build_field_regex(field: Field) -> str:

  segment_pattern = get_segment_pattern_from_field(field)
  separator = "" if segment_pattern.endswith("+") else r"[+:']"

  print("FIELD REGEX = ", f"{segment_pattern}{separator}.*?'")

  return f"{segment_pattern}{separator}.*?'"


def get_segment_strings(field: Field, segments_string: str) -> list:
  
  regex_pattern = build_field_regex(field)

  segments = re.compile(pattern=regex_pattern, flags=re.DOTALL).findall(segments_string)

  return segments


def get_segment_groups_string(field: Field, segments_string: str):

  segment_pattern = get_segment_pattern_from_field(field)
  
  if 'EQD' in field.name:
    segment_bloc_pattern = f"({segment_pattern}.*?)(?=EQD\+|$)"
  else:
    segment_bloc_pattern = f'({segment_pattern}.*?)(?={segment_pattern}|$)'


  print("FIELD NAME:", field.name, "SEGMENT PATTERN", segment_pattern)
  print("SEGMENT BLOC PATTERN", segment_bloc_pattern)

  pattern = re.compile(segment_bloc_pattern, re.DOTALL)

  matches = list(re.finditer(pattern, segments_string))
  print(f"MATCHES : {matches}")

  segment_groups = []

  total = len(matches) - 1

  for index, match in enumerate(matches):

      print("\n", f"{index} : SEGMENT GROUP {index}/{total}")

      segment_group_string = match.group()

      last_segment = find_last_segment(segment_group_string)#.replace("+", "\+").replace(".", "\.")
      print(f"BEFORE ESCAPE {last_segment=}")

      last_segment = escape_regex_chars(last_segment)
      print(f"AFTER ESCAPE {last_segment=}")

      complementary_match = re.compile(f"{segment_pattern}.*?'", re.DOTALL).search(segment_group_string)
      if complementary_match:
        segment_pattern_full = escape_regex_chars(complementary_match.group())
      else:
        segment_pattern_full = segment_pattern

      segment_group_regex = f'{segment_pattern_full}.*?{last_segment}'

      segment_group_found = re.compile(segment_group_regex, re.DOTALL).findall(segment_group_string)

      print(f"SEGMENT GROUP REGEX {index} : ", segment_group_regex)
      print(f"LAST SEGMENT IN {field.name} GROUP {index}: {last_segment}")
      print(f"SEGMENT GROUP STRING {index} : ", segment_group_string.replace('\n', ''))
      print(f"SEGMENT GROUP FOUND {index} : ", segment_group_found)
      print()

      segment_groups.append(segment_group_found)

  return segment_groups


def parse_segment(regex_pattern: str, segments_string: str) -> list:

  segments = re.compile(pattern=regex_pattern, flags=re.DOTALL).findall(segments_string)

  segments_collection = [RawSegmentCollection.from_str(segment).segments for segment in segments]

  elements_collection = [segments[0].elements for segments in segments_collection]

  return elements_collection
