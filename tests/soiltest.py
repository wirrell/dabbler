"""
Tests for dssat_soil
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
from dabbler.soil import SoilGenerator 
from shapely.geometry import Polygon

# Load in test fields
fields = {}
with open('test/field.geojson', 'r') as field_file:
    field_json = json.load(field_file)
field = Polygon(field_json['features'][0]['geometry']['coordinates'][0])
fields['TXST000001'] = field
with open('test/field2.geojson', 'r') as field_file:
    field_json = json.load(field_file)
field = Polygon(field_json['features'][0]['geometry']['coordinates'][0])
fields['TEST000002'] = field

# Test dssat_soil
SG = SoilGenerator(save_location='./test/')

SG.generate_soils(fields, 'test', 'TT.SOL', 'test/')
