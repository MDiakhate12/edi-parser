from dataclasses import dataclass, field
from modules.edi_parsing.data_model.baplie_parsers import SegmentParser


@dataclass
class LocationIdentification(SegmentParser):
  location_identifier: str = field(default=None)
  code_list_identification_code: str = field(default=None)
  code_list_responsible_agency_code: str = field(default=None)


@dataclass
class Location(SegmentParser):
  location_function_code_qualifier: str = field(default=None)
  location_identification: LocationIdentification = field(default_factory=LocationIdentification)
  segment_name: str = field(init=False, default="LOC")


@dataclass
class EquipmentIdentification(SegmentParser):
  equipment_identifier: str = field(default=None)
  code_list_identification_code: str = field(default=None)
  code_list_responsible_agency_code: str = field(default=None)
  # country_identifier:str = field(default=None)

@dataclass
class EquipmentSizeAndType(SegmentParser):
  equipment_size_and_type_description_code: str = field(default=None)
  code_list_identification_code: str = field(default=None)
  code_list_responsible_agency_code: str = field(default=None)
  # equipment_size_and_type_description:str = field(default=None)

@dataclass
class EquipmentDetails(SegmentParser):
  equipement_type_code_qualifier: str = field(default=None)
  equipment_identification: EquipmentIdentification = field(default_factory=EquipmentIdentification)
  equipment_size_and_type: EquipmentSizeAndType = field(default_factory=EquipmentSizeAndType)
  equipment_supplier_code: str = field(default=None)
  equipment_status_code: str = field(default=None)
  full_or_empty_indication_code: str = field(default=None)
  segment_name: str = field(init=False, default="EQD")

@dataclass
class EquipmentAttachment(SegmentParser):
  equipement_type_code_qualifier: str = field(default=None)
  equipment_identification: EquipmentIdentification = field(default_factory=EquipmentIdentification)
  segment_name: str = field(init=False, default="EQA")

@dataclass
class MeasurementDetails(SegmentParser):
    measured_attribute_code: str = field(default=None)
    measurement_significance_code: str = field(default=None)

@dataclass
class ValueRange(SegmentParser):
    measurement_unit_code: str = field(default=None)
    measure: float = field(default=None)
    range_minimum_quantity: float = field(default=None)
    range_maximum_quantity: float = field(default=None)
    significant_digits_quantity: int = field(default=None)

@dataclass
class Measurement(SegmentParser):
    measurement_purpose_code_qualifier: str = field(default=None)
    measurement_details: MeasurementDetails = field(default_factory=MeasurementDetails)
    value_range: ValueRange = field(default_factory=ValueRange)
    segment_name: str = field(init=False, default="MEA")

@dataclass
class HandlingInstruction(SegmentParser):
    handling_instruction_description_code: str
    code_list_identification_code: str = field(default=None)
    code_list_responsible_agency_code: str = field(default=None)
    handling_instruction_description: str = field(default=None)

   # HAN+ZZZ:HANDLING:306:Stow/Stowed on deck protected'
@dataclass
class HazardousMaterialCategory(SegmentParser):
    hazardous_material_category_name_code: str = field(default=None)
    code_list_identification_code: str = field(default=None)
    code_list_responsible_agency_code: str = field(default=None)
    hazardous_material_category_name: str = field(default=None)

@dataclass
class Handling(SegmentParser):
    handling_instruction: HandlingInstruction = field(default_factory=HandlingInstruction)
    # hazardous_material_category: HazardousMaterialCategory = field(default=None)
    segment_name: str = field(init=False, default="HAN")

@dataclass
class Control(SegmentParser):
    control_total_type_code_qualifier: str = field(default=None)
    control_total_quantity: int = field(default=None)

@dataclass
class ControlTotal(SegmentParser):
  control: Control = field(default_factory=Control)
  segment_name: str = field(init=False, default="CNT")

@dataclass
class PartyIdentificationDetails(SegmentParser):
    party_identifier: str = field(default=None)
    code_list_identification_code: str = field(default=None)
    code_list_responsible_agency_code: str = field(default=None)

@dataclass
class NameAndAddress(SegmentParser):
    party_function_code_qualifier: str = field(default=None)
    party_identification_details: PartyIdentificationDetails = field(default_factory=PartyIdentificationDetails)
    segment_name: str = field(init=False, default="NAD")

@dataclass
class ReferenceIdentification(SegmentParser):
    reference_code_qualifier: str = field(default=None)
    reference_identifier: str = field(default=None)

@dataclass
class Reference(SegmentParser):
    reference: ReferenceIdentification = field(default_factory=ReferenceIdentification)
    segment_name: str = field(init=False, default="RFF")

@dataclass
class DimensionsSpecification(SegmentParser):
  measurement_unit_code: str = field(default=None)
  length_measure: float = field(default=None)
  width_measure: float = field(default=None)
  height_measure: float = field(default=None)

@dataclass
class Dimensions(SegmentParser):
  dimension_type_code_qualifier: int = field(default=None)
  dimension_specification: DimensionsSpecification = field(default_factory=DimensionsSpecification)
  segment_name: str = field(init=False, default="DIM")

@dataclass
class TemperatureSetting(SegmentParser):
    temperature_degree: float = field(default=None)
    measurement_unit_code: str = field(default=None)

@dataclass
class Temperature(SegmentParser):
    temperature_type_code_qualifier: str = field(default=None)
    temperature_setting: TemperatureSetting = field(default_factory=TemperatureSetting)
    segment_name: str = field(init=False, default="TMP")

@dataclass
class RangeSetting(SegmentParser):
    measurement_unit_code: str = field(default=None)
    range_minimum_quantity: float = field(default=None)
    range_maximum_quantity: float = field(default=None)

@dataclass
class Range(SegmentParser):
    type_code_qualifier: str = field(default=None)
    range_specification: RangeSetting = field(default_factory=RangeSetting)
    segment_name: str = field(init=False, default="RNG")

@dataclass
class HazardCode(SegmentParser):
    hazard_identification_code: str = field(default=None)
    additional_hazard_classification_identifier: str = field(default=None)
    hazard_code_version_identifier: str = field(default=None)


@dataclass
class DangerousGoodsShipmentFlashpoint(SegmentParser):
    shipment_flashpoint_value: str = field(default=None)
    measurement_unit_code: str = field(default=None)


@dataclass
class DangerousGoodsLabel(SegmentParser):
    marking_identifier_1: str = field(default=None)
    marking_identifier_2: str = field(default=None)
    marking_identifier_3: str = field(default=None)


@dataclass
class HazardIdentificationPlacardDetails(SegmentParser):
    orange_hazard_placard_upper_part_identifier: str = field(default=None)
    orange_hazard_placard_lower_part_identifier: str =  field(default=None)


@dataclass
class DangerousGoods(SegmentParser):
    dangerous_goods_regulations_code_qualifier: str = field(default=None)
    hazard_code: HazardCode = field(default_factory=HazardCode)
    undg_information: str = field(default=None)
    dangerous_goods_shipment_flashpoint: DangerousGoodsShipmentFlashpoint = field(default_factory=DangerousGoodsShipmentFlashpoint)
    packaging_danger_level_code: str = field(default=None)
    emergency_procedure_for_ships: str = field(default=None)
    hazard_medical_first_aid_guide: str = field(default=None)
    hazard_identification_placard_details: HazardIdentificationPlacardDetails = field(default_factory=HazardIdentificationPlacardDetails)
    transport_emergency_card_identifier: str = field(default=None)
    dangerous_goods_label: DangerousGoodsLabel = field(default_factory=DangerousGoodsLabel)
    segment_name: str = field(init=False, default="DGS")

@dataclass
class AttributeType(SegmentParser):
    attribute_type_description_code: str = field(default=None)
    code_list_identification_code: str = field(default=None)
    code_list_responsible_agency_code: str = field(default=None)
    attribute_type_description: str = field(default=None)


@dataclass
class AttributeDetails(SegmentParser):
    attribute_description_code: str = field(default=None)
    code_list_identification_code: str = field(default=None)
    code_list_responsible_agency_code: str = field(default=None)
    attribute_description: str = field(default=None)


@dataclass
class Attribute(SegmentParser):
    attribute_function_code_qualifier: str = field(default=None)
    attribute_type: AttributeType = field(default_factory=AttributeType)
    attribute_details: AttributeDetails = field(default_factory=AttributeDetails)
    segment_name: str = field(init=False, default="ATT")

@dataclass
class TextReference(SegmentParser):
    free_text_description_code: str = field(default=None)
    code_list_identification_code: str = field(default=None)
    code_list_responsible_agency_code: str = field(default=None)



@dataclass
class FreeText(SegmentParser):
    text_subject_code_qualifier: str = field(default=None)
    text_function_code: str = field(default=None)
    text_reference: TextReference = field(default_factory=TextReference)
    text_literal: str = field(default=None)
    segment_name: str = "FTX"

@dataclass
class NatureOfCargo(SegmentParser):
    cargo_type_classification_code: str = field(default=None)
    code_list_identification_code: str = field(default=None)
    code_list_responsible_agency_code: str = field(default=None)


@dataclass
class GoodsDetails(SegmentParser):
  nature_of_cargo: NatureOfCargo = field(default_factory=NatureOfCargo)
  segment_name: str = field(init=False, default="GDS")

@dataclass
class DateTimeOrPeriod(SegmentParser):
    function_code_qualifier: str = field(default=None)
    text: str = field(default=None)
    format_code: str = field(default=None)

@dataclass
class DateTime(SegmentParser):
    datetime_or_period: DateTimeOrPeriod = field(default_factory=DateTimeOrPeriod)
    segment_name: str = field(init=False, default="DTM")