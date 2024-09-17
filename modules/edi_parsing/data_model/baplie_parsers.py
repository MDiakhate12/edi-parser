import traceback

from dataclasses import dataclass, fields, is_dataclass

from modules.edi_parsing.utils import python_utils
from modules.edi_parsing.utils import parsing_utils

from pydifact.segmentcollection import RawSegmentCollection

@dataclass
class SegmentParser:

  @classmethod
  def get_field(cls, field_name: str):
    for field in fields(cls):
      if field.name == field_name:
        return field


  @classmethod
  def from_elements(cls, elements: list):

    initializable_fields = [field for field in fields(cls) if field.init]

    required_initializable_fields = [field for field in initializable_fields if field.default is field.default_factory]

    required_initializable_fields_names = [field.name for field in required_initializable_fields]

    if len(elements) == 0:
      return cls()

    if len(required_initializable_fields) <= len(elements):

      parsed_elements = {}

      for element, field in zip(elements, initializable_fields):

        if is_dataclass(field.type):
          if isinstance(element, list):
            subelements = field.type.from_elements(element)
            parsed_elements[field.name] = subelements
          else:
            parsed_elements[field.name] = field.type.from_elements([element])
        else:
          parsed_elements[field.name] = element

      # print(cls.__name__, parsed_elements)

      return cls(**parsed_elements)
    else:      
      raise ValueError(f"""Number elements to unpack into instance does not match with number of required attribute. {elements=}
      You can't have less elements than required attributes.
      {cls.__name__} contains {len(required_initializable_fields)} required initializable attributes
      {required_initializable_fields_names},
      but initalization got {len(elements)} elements
      {elements}""")
    
    
  @classmethod
  def from_segment_string(cls, segment_string: str):

    segments = RawSegmentCollection.from_str(segment_string).segments

    elements = [segment.elements for segment in segments]

    if len(elements) == 0:
      return cls()

    try:
      if len(elements) == 1:
        return cls.from_elements(elements[0])
      
      else:
        return cls.from_elements(elements)   
    except Exception as e:
      print(f"Cannot parse {cls} for elements {elements} {e}")
      traceback.print_exc()
  


@dataclass
class SegmentGroupParser:

  @classmethod
  def as_dict(self, dataclass_list: list):
    return python_utils.as_dict(dataclass_list)


  @classmethod
  def get_field(cls, field_name: str):
    for field in fields(cls):
      if field.name == field_name:
        return field
          

  @classmethod
  def from_segment_string(cls, segment_string: str = None):

    if segment_string:
      collected_fields = {}

      print(f"######### Parsing Segment Group : {cls.__name__} ########")
      print(f"SEGMENTS_STRING = BEGIN_SEGMENT<< {segment_string} >>END_SEGMENT", end="\n\n")
      
      initializable_fields = [field for field in fields(cls) if field.init]

      for field in fields(cls):

        print(f"######### Parsing subfield : {field.name} ########")

        subtype = python_utils.get_subtype(field.type)

        # print(f"FIELD REGEX = {field_regex}")
        print(f"FIELD TYPE = {field.type}")
        print(f"FIELD SUBTYPE = {subtype}")

        field_is_segment = is_dataclass(field.type) and issubclass(field.type, SegmentParser)
        field_is_segment_group = is_dataclass(field.type) and issubclass(field.type, SegmentGroupParser)

        subtype_is_segment = subtype and issubclass(subtype, SegmentParser)
        subtype_is_segment_group = subtype and issubclass(subtype, SegmentGroupParser)

        print(f"FIELD IS SEGMENT = {field_is_segment}")
        print(f"FIELD IS SEGMENT GROUP = {field_is_segment_group}")

        print(f"FIELD SUBTYPE IS SEGMENT = {subtype_is_segment}")
        print(f"FIELD SUBTYPE IS SEGMENT GROUP = {subtype_is_segment_group}")
        # print(f"FIELD ELEMENTS FOUND = {elements_collection}")
        # print(f"FIELD ELEMENTS SIZE = {len(elements_collection)}")

        if field.name == "edi_string":
          collected_fields[field.name] = segment_string

        # Case 1 : Field is of type SegmentGroupParser
        if field_is_segment_group:
          field_segment_groups_strings = parsing_utils.get_segment_groups_string(field=field, segments_string=segment_string)
          print(f"{field.name} SEGMENT GROUPS STRINGS = {field_segment_groups_strings}")
          
          if len(field_segment_groups_strings) == 0:
            collected_fields[field.name] = field.default_factory()
          elif len(field_segment_groups_strings) == 1:
            try:
              parsed_segment_group = field.type.from_segment_string(*field_segment_groups_strings[0])
              print(f"PARSED SEGMENT GROUP {parsed_segment_group=}")
              collected_fields[field.name] = parsed_segment_group
            except Exception as e:
              print(f"Error on segment group type parsing for {field.name=} {field.type=} {field_segment_groups_strings=} error_message={e}")
              traceback.print_exc()
          else:
            collected_fields[field.name] = field.default_factory()
            raise ValueError(f"""{field.name} is not a list type so cannot have multiple '{field.name}' matches in segment group string. 
              Either changes this field to List[{field.type.__name__}] or verify your segment group string to match only one value.
              Matched segment groups strings {field_segment_groups_strings}
              Please check the type of {field.name} in {cls.__name__} and ensure this segment group only matches one value or is of type List""")


        # Case 2 : Field is of type List[SegmentGroupParser]
        if subtype_is_segment_group:
          subtype_segment_groups_strings = parsing_utils.get_segment_groups_string(field=field, segments_string=segment_string)
          print(f"{field.name} SUBTYPE SEGMENT GROUPS STRINGS = {subtype_segment_groups_strings}")
          
          parsed_segment_groups = []
          
          if len(subtype_segment_groups_strings) >= 1:

            for s in subtype_segment_groups_strings:

              if len(s) >= 1:
                  try:
                    parsed_segment_group = subtype.from_segment_string(*s)
                    parsed_segment_groups.append(parsed_segment_group)
                  except Exception as e:
                    print(f"Error on segment group subtype parsing for {field.name=} {field.type=} {field_segment_strings=} error_message={e}")
                    traceback.print_exc()


          print(f"PARSED SEGMENT GROUPS {parsed_segment_groups=}")
          collected_fields[field.name] = parsed_segment_groups


        # Case 3 : Field is of type SegmentParser
        if field_is_segment:
          field_segment_strings = parsing_utils.get_segment_strings(field=field, segments_string=segment_string)
          print(f"{field.name} SEGMENT STRINGS = {field_segment_strings}")
          if len(field_segment_strings) == 0:
            collected_fields[field.name] = field.default_factory()
          elif len(field_segment_strings) == 1:
            try:
              parsed_segment = field.type.from_segment_string(*field_segment_strings)
              print(f"PARSED SEGMENT {parsed_segment=}")
              collected_fields[field.name] = parsed_segment
            except Exception as e:
              collected_fields[field.name] = field.default_factory()
              print(f"Error on segment type parsing for {field.name=} {field.type=} {field_segment_strings=} error_message={e}")
              traceback.print_exc()

          else:
            raise ValueError(f"""{field.name} is not a list type so cannot have multiple '{field.name}' matches in segment string. 
              Either changes this field to List[{field.type.__name__}] or verify your segment string to match only one value.
              Matched segments string {field_segment_strings}
              Please check the type of {field.name} in {cls.__name__} and ensure this segment only matches one value or is of type List""")


        # Case 4 : Field is of type List[SegmentParser]
        if subtype_is_segment:
          subtype_segment_strings = parsing_utils.get_segment_strings(field=field, segments_string=segment_string)
          print(f"{field.name} SUBTYPE SEGMENT STRINGS = {subtype_segment_strings}")
          
          parsed_segments = []

          if len(subtype_segment_strings) >= 1:
            for s in subtype_segment_strings:
              try:
                parsed_segment = subtype.from_segment_string(s)
                parsed_segments.append(parsed_segment)
              except Exception as e:
                print(f"Error on segment subtype parsing for {field.name=} {field.type=} {subtype_segment_strings=} error_message={e}")
                traceback.print_exc()

          print(f"PARSED SEGMENTS {parsed_segments=}")
          collected_fields[field.name] = parsed_segments
        
      return cls(**collected_fields)
    
    else:
      return cls()


    

  @classmethod
  def parse_segments_groups(self, segments_groups:list):
    
    @python_utils.progress(segments_groups)
    def parse_segment_group(segment_group: str, cls):

        segment_group = self.from_segment_string(segment_group)
        return segment_group
      
    segment_groups = []

    for s in segments_groups:
      
      segment_group = parse_segment_group(s, self)

      if segment_group:
        segment_groups.append(segment_group)

    return segment_groups