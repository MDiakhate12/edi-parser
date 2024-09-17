from typing import List
from dataclasses import dataclass, field

from modules.edi_parsing.data_model.baplie_parsers import SegmentGroupParser

from modules.edi_parsing.data_model.baplie_segments import (
    Location,
    EquipmentDetails,
    EquipmentAttachment,
    Measurement,
    Handling,
    ControlTotal,
    NameAndAddress,
    Reference,
    Dimensions,
    Temperature,
    Range,
    DangerousGoods,
    Attribute,
    FreeText,
    GoodsDetails,
    DateTime,
)


@dataclass
class EquipmentAttachmentSegmentGroup(SegmentGroupParser):
    EQA: EquipmentAttachment = field(default_factory=EquipmentAttachment, metadata={"is_main_segment": True})
    NAD: NameAndAddress = field(default_factory=NameAndAddress)


@dataclass
class TemperatureSegmentGroup(SegmentGroupParser):
    TMP: Temperature = field(default_factory=Temperature, metadata={"is_main_segment": True})
    RNG: Range = field(default_factory=Range)
    DTM: DateTime = field(default_factory=DateTime)


@dataclass
class DangerousGoodsSegmentGroup(SegmentGroupParser):
    DGS: DangerousGoods = field(default_factory=DangerousGoods, metadata={"is_main_segment": True})
    ATT: List[Attribute] = field(default_factory=list)
    MEA_AAA_NET_WEIGHT: Measurement = field(default_factory=Measurement, metadata={"qualifier": "AAE", "subqualifier": "AAA"})
    MEA_AAB_GOODS_ITEM_GROSS_WEIGHT: Measurement = field(default_factory=Measurement, metadata={"qualifier": "AAE", "subqualifier": "AAB"})
    MEA_AAF_NET_NET_WEIGHT: Measurement = field(default_factory=Measurement, metadata={"qualifier": "AAE", "subqualifier": "AAF"})
    MEA_AEN_RADIOACTIVE_INDEX_OF_TRANSPORT: Measurement = field(default_factory=Measurement, metadata={"qualifier": "AAE", "subqualifier": "AEN"})
    MEA_AEO_RADIOACTIVITY: Measurement = field(default_factory=Measurement, metadata={"qualifier": "AAE", "subqualifier": "AEO"})
    MEA_AFN_NET_EXPLOSIVE_WEIGHT: Measurement = field(default_factory=Measurement, metadata={"qualifier": "AAE", "subqualifier": "AFN"})
    MEA_AFO_RADIOACTIVE_CRITICALITY_SAFETY_INDEX: Measurement = field(default_factory=Measurement, metadata={"qualifier": "AAE", "subqualifier": "AFO"})
    FTX_AAC: FreeText = field(default_factory=FreeText, metadata={"qualifier": "AAC"})
    FTX_AAD: FreeText = field(default_factory=FreeText, metadata={"qualifier": "AAD"})


@dataclass
class EquipmentDetailsSegmentGroup(SegmentGroupParser):
    EQD: EquipmentDetails = field(default_factory=EquipmentDetails, metadata={"is_main_segment": True})

    NAD: List[NameAndAddress] = field(default_factory=list)
    MEA: List[Measurement] = field(default_factory=list, metadata={"qualifier": "AAE", "subqualifier": "AAO|AAS|BRJ|BRK|BRL|T|ZO"})
    MEA_CONTAINER_WEIGHT: List[Measurement] = field(default_factory=list, metadata={"qualifier": "AAE", "subqualifier": "VGM|AET"})
    HAN: List[Handling] = field(default_factory=list)
    GDS: List[GoodsDetails] = field(default_factory=list)
    FTX: List[FreeText] = field(default_factory=list, metadata={"qualifier": "AAY|AGK|AAA"})

    LOC_9_PORT_OF_LOADING: Location = field(default_factory=Location, metadata={"qualifier": "9"})
    LOC_11_PORT_OF_DISCHARGE: Location = field(default_factory=Location, metadata={"qualifier": "11"})
    LOC_76_ORIGINAL_PORT_OF_LOADING: Location = field(default_factory=Location, metadata={"qualifier": "76"})
    LOC_83_PORT_OF_DELIVERY: Location = field(default_factory=Location, metadata={"qualifier": "83"})
    LOC_65_FINAL_PORT_OF_DISCHARGE: Location = field(default_factory=Location, metadata={"qualifier": "65"})

    LOC_64_FIRST_OPTIONAL_PLACE_OF_DISCHARGE: Location = field(default_factory=Location, metadata={"qualifier": "64"})
    LOC_68_SECOND_OPTIONAL_PLACE_OF_DISCHARGE: Location = field(default_factory=Location, metadata={"qualifier": "68"})
    LOC_70_THIRD_OPTIONAL_PLACE_OF_DISCHARGE: Location = field(default_factory=Location, metadata={"qualifier": "70"})
    LOC_97_OPTIONAL_PLACE_OF_DISCHARGE: Location = field(default_factory=Location, metadata={"qualifier": "97"})
    LOC_198_ORIGINAL_LOCATION: Location = field(default_factory=Location, metadata={"qualifier": "198"})

    TMP_SG: TemperatureSegmentGroup = field(default_factory=TemperatureSegmentGroup)
    EQA: List[EquipmentAttachmentSegmentGroup] = field(default_factory=list)
    DGS: List[DangerousGoodsSegmentGroup] = field(default_factory=list)

    DIM_5_OOG_FRONT_LENGTH_MEASURE: Dimensions = field(default_factory=Dimensions, metadata={"qualifier": "5"}) # OOG front
    DIM_6_OOG_BACK_LENGTH_MEASURE: Dimensions = field(default_factory=Dimensions, metadata={"qualifier": "6"}) # OOG back
    DIM_7_OOG_RIGHT_WIDTH_MEASURE: Dimensions = field(default_factory=Dimensions, metadata={"qualifier": "7"}) # OOG right
    DIM_8_OOG_LEFT_WIDTH_MEASURE: Dimensions = field(default_factory=Dimensions, metadata={"qualifier": "8"}) # OOG left
    DIM_13_OOG_TOP_HEIGHT_MEASURE: Dimensions = field(default_factory=Dimensions, metadata={"qualifier": "13"}) # OOG height
    DIM_10_EXTERNAL_EQUIPMENT_DIMENSION: Dimensions = field(default_factory=Dimensions, metadata={"qualifier": "10"}) # External equipment dimension
    DIM_1_BREAKBULK_HEIGHT_MEASURE: Dimensions = field(default_factory=Dimensions, metadata={"qualifier": "1"}) # BreakBulk height
    DIM_17_BUNDLE_FLAT_RACK_HEIGHT_MEASURE: Dimensions = field(default_factory=Dimensions, metadata={"qualifier": "17"}) # bundle of flat rack height
    DIM_18_NON_STANDARD_HEIGHT_MEASURE: Dimensions = field(default_factory=Dimensions, metadata={"qualifier": "18"}) # Fix non standard height
    DIM_19_COLLAPSED_FLAT_RACK_HEIGHT_MEASURE: Dimensions = field(default_factory=Dimensions, metadata={"qualifier": "19"}) # collapsed flat rack height
    DIM_19_ACTUAL_FLAT_RACK_HEIGHT_MEASURE: Dimensions = field(default_factory=Dimensions, metadata={"qualifier": "20"}) # actual height of flat rack with expandable end walls
    DIM_21_FLOOR_HEIGHT_MEASURE: Dimensions = field(default_factory=Dimensions, metadata={"qualifier": "21"}) # supporting equipment floor height
    DIM_22_CONTAINER_CORNER_POST_OFF_WIDTH: Dimensions = field(default_factory=Dimensions, metadata={"qualifier": "22"}) # Container off-standard dimension width at corner posts
    DIM_23_CONTAINER_BODY_OFF_WIDTH: Dimensions = field(default_factory=Dimensions, metadata={"qualifier": "23"}) # Container off-standard dimension width of body


@dataclass
class LocationSegmentGroup(SegmentGroupParser):
    LOC_147: Location = field(metadata={"is_main_segment": True, "qualifier": "147"})
    FTX_AGW: FreeText = field(default_factory=FreeText, metadata={"qualifier": "AGW"})
    RFF_EQ: List[Reference] = field(default_factory=list, metadata={"qualifier": "EQ"})
    EQD_CN: List[EquipmentDetailsSegmentGroup] = field(default_factory=list, metadata={"qualifier": "CN"})
    EQD_BB: List[EquipmentDetailsSegmentGroup] = field(default_factory=list, metadata={"qualifier": "BB"})
    CNT: ControlTotal = field(default_factory=ControlTotal)
    edi_string: str = field(default="")


@dataclass
class TankSegmentGroup(SegmentGroupParser):
  LOC_ZZZ: Location = field(default_factory=Location, metadata={"qualifier": "ZZZ"})
  MEA_WT: Measurement = field(default_factory=Measurement, metadata={"qualifier": "WT"})
  MEA_DEN: Measurement = field(default_factory=Measurement, metadata={"qualifier": "DEN"})
  MEA_VOL: Measurement = field(default_factory=Measurement, metadata={"qualifier": "VOL"})
  MEA_ACA: Measurement = field(default_factory=Measurement, metadata={"qualifier": "ACA"})
  DIM_1: Dimensions = field(default_factory=Dimensions, metadata={"qualifier": "1"})
  FTX_AAI: FreeText = field(default_factory=FreeText, metadata={"qualifier": "AAI"})