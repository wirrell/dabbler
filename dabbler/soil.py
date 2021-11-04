"""
Generate DSSAT soil files for anywhere in the world.

Soil data taken from ISRIC SoilGrids - https://www.isric.org/explore/soilgrids

Methodology from: https://doi.org/10.1016/j.envsoft.2019.05.012

Author: G. Worrall
Date: October 19th 2021
"""
import pandas as pd
import numpy as np
import random
import pyproj
import rasterio
import rasterio.mask
import io
from . import PTF
from .headers import get_header
from pathlib import Path
from shapely.geometry import LineString, box
from shapely.ops import split, unary_union, transform
from itertools import cycle
from rasterio.plot import show
from rasterio.io import MemoryFile
from soiltexture import getTexture

# List maps.isric.org services avaiable
services = {'wrb': 'https://maps.isric.org/mapserv?map=/map/wrb.map',
            'bdod': 'https://maps.isric.org/mapserv?map=/map/bdod.map',
            'cec': 'https://maps.isric.org/mapserv?map=/map/cec.map',
            'cfvo': 'https://maps.isric.org/mapserv?map=/map/cfvo.map',
            'clay': 'https://maps.isric.org/mapserv?map=/map/clay.map',
            'nitrogen': 'https://maps.isric.org/mapserv?map=/map/nitrogen.map',
            'phh2o': 'https://maps.isric.org/mapserv?map=/map/phh2o.map',
            'sand': 'https://maps.isric.org/mapserv?map=/map/sand.map',
            'silt': 'https://maps.isric.org/mapserv?map=/map/silt.map',
            'soc': 'https://maps.isric.org/mapserv?map=/map/soc.map',
            'ocs': 'https://maps.isric.org/mapserv?map=/map/osc.map',
            'ocd': 'https://maps.isric.org/mapserv?map=/map/ocd.map'}

ROI_CRS = 'EPSG:4326'

class SoilGenerator():
    """
    Produces soil tables for DSSAT for given WGS84 coordinates.
    """
    # NOTE: we include sand in initial extract for use in PTFs
    soilgrids_properties = ['bdod', 'soc', 'clay', 'silt', 'phh2o', 'cec', 'sand']
    soil_grids_dssat_labels = ['SBDM', 'SLOC', 'SLCL', 'SLSI', 'SLHW', 'SCEC', 'SAND']
    # define layer names for a SoilGrids service and their equivalent depths
    soilgrid_layers = {'{soil_property}_0-5cm_mean': 5,
                       '{soil_property}_5-15cm_mean': 15,
                       '{soil_property}_15-30cm_mean': 30,
                       '{soil_property}_30-60cm_mean': 60,
                       '{soil_property}_60-100cm_mean': 100,
                       '{soil_property}_100-200cm_mean': 200}

    def __init__(
        self,
        save_location,
        soilgridsdata='/home/george/Documents/data/soil/SoilGrids/Iowa',
        HC27data=Path(__file__).parent / "../data/HC27"
    ):
        self.soilgridsdata = Path(soilgridsdata)
        self.HC27data = Path(HC27data)
        self.load_soildata()

    def load_soildata(self):
        """
        Load SoilGrids dataset reference objects and HC27 data tables.
        """
        self.soillayersrefs = {}
        for soil_property in self.soilgrids_properties:
            for layer in self.soilgrid_layers:
                layer = layer.format(soil_property=soil_property)
                self.soillayersrefs[layer] = self.load_soillayer(soil_property, layer)

        self.HC27_soils = {}
        with open(self.HC27data / 'HC.SOL', 'r') as HC_f:
            HC_string = HC_f.read()

        HC27_tables = [table for table in HC_string.split('*') if table.startswith('H')]

        def interpolate_HC27(depth_table):
            # Interpolate HC27 table to methodology paper soil depths.
            # See Table 5 in https://doi.org/10.1016/j.envsoft.2019.05.012
            del depth_table['SLMH']  # remove master horizon so we can mulitply
            depth_table.loc[5, :] = depth_table.loc[10, :]
            depth_table.loc[15, :] = 0.5 * depth_table.loc[10, :] + \
                    0.5 * depth_table.loc[30, :]
            depth_table.loc[100, :] = 0
            depth_table.loc[200, :] = 0
            if 90 in depth_table.index:
                depth_table.loc[100, :] = 0.75 * depth_table.loc[90, :]
            if 120 in depth_table.index:
                depth_table.loc[100, :] += 0.25 * depth_table.loc[120, :]
                depth_table.loc[200, :] = 0.2 * depth_table.loc[120, :]
            if 150 in depth_table.index:
                depth_table.loc[200, :] += 0.3 * depth_table.loc[150, :]
            if 180 in depth_table.index:
                depth_table.loc[200, :] += 0.5 * depth_table.loc[180, :]

            return depth_table

        for table in HC27_tables:
            code, depth_table, properties = self._format_HC27_table(table)
            depth_table = interpolate_HC27(depth_table)
            self.HC27_soils[code] = (depth_table, properties)


    def _format_HC27_table(self, table):
        """Format HC27 table by loading it into pandas and reading info."""
        code = table.split(' ')[0]
        sections =  table.split('@')
        properties = pd.read_csv(io.StringIO(sections[2]), sep='\s+')
        depth_table = pd.read_csv(io.StringIO(sections[3]), sep='\s+',
                                  index_col='SLB')
        return code, depth_table, properties


    def load_soillayer(self, soil_property, layer):
        """Load in rasterio dataset reference object for soil property layer."""
        return rasterio.open(
            self.soilgridsdata / soil_property / (layer + '.tif')
        )

    def generate_soils(self, ROIs, loc_name, filename, append=False):
        """Generate DSSAT soil tables for a list of ROIs.

        Parameters
        ----------
        ROIs : dict of shapely.geometry.Polygon in WGS84 coodinates
                keyed by 10 character reference codes
        filename : str
            e.g. 'US.SOL'. NOTE: prefix must be 2 characters long
        append : bool
            If true, will append to a soil file if name already exists

        Returns
        -------
        str
            Absolute path to newly generated file.
        """

        # Reproject ROIs from WGS84 to SoilGrids projection:
        #   Interupted Goode Homolosine

        ROIs_WGS84 = ROIs
        ROIs = self._reproject_ROIs(ROIs)

        ROI_tables = ROIs.copy()

        if len(filename.split('.')[0]) > 2:
            raise ValueError(f'Filename {filename} must be of format XX.SOL')

        for ROI_key in ROIs:
            if len(ROI_key) != 10:
                raise ValueError('Keys to ROI must fit DSSAT format: '
                                 'XXNNNNNNNN')
            ROI_stub = ROI_key[:2]
            if ROI_stub != filename.split('.')[0]:
                raise ValueError(f'First two letters of ROI key {ROI_key} do not match'
                                 f' the filename {filename}. They must match as DSSAT '
                                 'uses the XX in XXNNNNNNNN key to search for the XX.'
                                 'SOL file.')
            ROI_tables[ROI_key] = self._form_soil_DFs()

        # get bulk density, soil organic carbon conc, clay, silt, pH in water,
        # carbon exchange capacity from SoilGrids
        for soil_property, key in zip(
            self.soilgrids_properties,
            self.soil_grids_dssat_labels
        ):
            self._add_depth_information(soil_property, key, ROIs, ROI_tables)

        # Calculate soil hydraulic properties from pedotransfer functions
        self._calculate_hydraulic_properties(ROI_tables)

        # Convert tables from SoilGrid units to DSSAT units
        for ROI_key in ROIs:
            depth_table = ROI_tables[ROI_key]
            self._soilgrid_to_DSSAT_conversion(depth_table)

        # Find HC27 soil for each ROI and assign HC27 values
        ROI_properties, HC_codes = self._assign_HC27_properties(ROI_tables)

        self.write_to_file(ROIs_WGS84,
                           ROI_tables,
                           ROI_properties,
                           HC_codes,
                           filename,
                           append)


    def write_to_file(self, ROIs, ROI_tables, ROI_properties, HC_codes,
                      filename, append):
        """Write values to DSSAT soil file."""

        # Set write mode
        write_mode = 'w'
        if append: write_mode = 'a'

        # set DP rounding number
        column_rounds = {'SLLL': 3,
                         'SDUL': 3,
                         'SSAT': 3,
                         'SRGF': 2,
                         'SSKS': 2,
                         'SBDM': 2,
                         'SLOC': 2,
                         'SLCL': 2,
                         'SLSI': 2,
                         'SLCF': 1,
                         'SLNI': 2,
                         'SLHW': 2,
                         'SLHB': 1,
                         'SCEC': 1,
                         'SADC': 1}

        # Format tables into one string per ROI
        full_write = ''

        with open(filename, write_mode) as f:
            for ROI_code, HC_code in zip(ROIs, HC_codes):
                depth_table = ROI_tables[ROI_code]
                properties = ROI_properties[ROI_code]

                # Remove SAND, we no longer need it
                del depth_table['SAND']

                # Round values
                for col in column_rounds:
                    depth_table[col] = np.round(depth_table[col].astype(float), column_rounds[col])

                # Pad master horizon values

                # First, build header
                LON = ROIs[ROI_code].centroid.xy[0][0]
                LAT = ROIs[ROI_code].centroid.xy[1][0]
                header = get_header('soil').format(
                    ROI_code=ROI_code,
                    LONG=LON,
                    LAT=LAT,
                    family=HC_code,
                    depth=depth_table.index[-1]
                )

                # Next build properties single row table
                properties = properties.to_string(index=False,
                                                  na_rep='   ').split('\n')
                # pad the start of each line
                properties[0] = '@ ' + properties[0]
                properties[1] = '  ' + properties[1]
                properties_string = '\n'.join(properties)

                # Now build depth table string
                # Have to split strip to get formatting exactly right for DSSAT
                strings = depth_table.to_string(index=False,
                                                na_rep='   ').split('\n')
                # Pad the start of each line for DSSAT formatting
                formatted_strings = ['@ ' + strings[0]] + [
                    '  ' + x for x in strings[1:]
                ]
                
                depth_table_string = '\n'.join(formatted_strings)

                # Write formatted strings to file
                f.write(header)
                f.write('\n')
                f.write(properties_string)
                f.write('\n')
                f.write(depth_table_string)
                f.write('\n\n')


    def _calculate_HC27_soils(self, ROI_tables):
        """Find HC27 generic soils closest to target soils based on HarvestChoice
        decision tree."""
        HC_codes = []

        # NOTE: we sum up to an HC27 code based on soil properties.
        # See paper in docstring for HC27 decision tree
        for ROI, depth_table in ROI_tables.items():
            HC_code = 1
            soc = depth_table.loc[5, 'SLOC']
            sand = depth_table.loc[5, 'SAND']
            clay = depth_table.loc[5, 'SLCL']
            texture = getTexture(sand, clay, 'USDA').split(' ')[-1]
            # available water storage capacity of 1m depth of the soil
            awc = (1000 * (depth_table.loc[:100, 'SDUL']
                          - depth_table.loc[:100, 'SLLL'])).mean()

            # Assign depth based on awc
            if awc <= 75: HC_code = HC_code + 2
            elif 75 < awc <= 100:
                if texture == 'sand':
                    HC_code = HC_code + 1
                else:
                    HC_code = HC_code + 2
            elif 100 < awc <= 125:
                if texture == 'clay':
                    HC_code = HC_code + 2
                else:
                    HC_code = HC_code + 1
            elif 125 < awc <= 150:
                if texture == 'sand':
                    pass
                else:
                    HC_code = HC_code + 1

            # Now compute HC27 code
            if texture == 'loam':
                HC_code = HC_code + 9
            elif texture == 'sand':
                HC_code = HC_code + 18
            if 0.7 <= soc < 1.2:
                HC_code = HC_code + 3
            elif soc < 0.7:
                HC_code = HC_code + 6

            HC_codes.append(f'HC_GEN00{HC_code}')

        return HC_codes

    def _assign_HC27_properties(self, ROI_tables):
        """Assign other properties based on matching HC27 generic soils."""
        HC_codes = self._calculate_HC27_soils(ROI_tables)

        soil_properties = {}

        for HC_code, (ROI, depth_table) in zip(HC_codes, ROI_tables.items()):
            HC_depth_table, HC_properties = self.HC27_soils[HC_code]
            # Do properties
            properties = HC_properties.copy()
            soil_properties[ROI] = properties
            # Do soil root growth factor and nitrogen concentration
            depth_table['SRGF'] = HC_depth_table.loc[depth_table.index, 'SRGF']
            depth_table['SLNI'] = HC_depth_table.loc[depth_table.index, 'SLNI']

        return soil_properties, HC_codes

    def _calculate_hydraulic_properties(self, ROI_tables):
        """
        Calculate soil hydraulic properties from texture and organic carbon
        content.
        """
        for ROI_key in ROI_tables:
            depth_table = ROI_tables[ROI_key]

            # convert to % weight from SoilGrids units
            # see: https://www.isric.org/explore/soilgrids/faq-soilgrids
            # NOTE: PTFs need them as fractions, see PTF docstring methodology
            # paper.
            sand_w = depth_table['SAND'] / (10 * 100)
            clay_w = depth_table['SLCL'] / (10 * 100)
            soc_w = depth_table['SLOC'] / (10 * 100)


            # do drained upper limit
            depth_table.loc[:, 'SDUL'] = PTF.drained_upper_limit(
                sand_w,
                clay_w,
                soc_w
            )
            # do wilting point
            depth_table.loc[:, 'SLLL'] = PTF.wilting_point(
                sand_w,
                clay_w,
                soc_w
            )
            # do saturated upper limit
            depth_table.loc[:, 'SSAT'] = PTF.saturated_upper_limit(
                sand_w,
                clay_w,
                soc_w
            )
            # do saturated hydraulic conductivity
            depth_table.loc[:, 'SSKS'] = PTF.saturated_hydraulic_conductivity(
                sand_w,
                clay_w,
                soc_w
            )

    def _reproject_ROIs(self, ROIs):
        """Reproject dict of ROI polygons to SoilGrids data projection."""
        # Get soil layer CRS
        SoilGrids_crs = pyproj.crs.CRS(
            list(self.soillayersrefs.values())[0].crs
        )
        WGS84 = pyproj.crs.CRS('EPSG:4326')

        transformer = pyproj.Transformer.from_crs(WGS84, SoilGrids_crs,
                                                  always_xy=True).transform

        ROIs_transform = ROIs.copy()

        for ROI_key in ROIs:
            ROIs_transform[ROI_key] = transform(transformer, ROIs[ROI_key])

        return ROIs_transform

    def _add_depth_information(self, soil_property, key, ROIs, ROI_tables):
        """Get soil depth information and add to DF.""" 

        # Go through all layers for property and extract data
        for layer_name, depth in self.soilgrid_layers.items():
            layer_name = layer_name.format(soil_property=soil_property)
            layer = self.soillayersrefs[layer_name]

            for ROI_key in ROIs:
                ROI_shape = ROIs[ROI_key]
                layer_data, layer_transform = rasterio.mask.mask(
                    layer, [ROI_shape], crop=True
                )
                layer_data = layer_data.astype(float)
                # Mask the nodata values and compute mean
                layer_data[layer_data == -32768] = np.nan
                layer_data_mean = np.nanmean(layer_data)
                # Update ROI soil depth table
                depth_table = ROI_tables[ROI_key]
                depth_table.loc[depth, key] = layer_data_mean


    def _soilgrid_to_DSSAT_conversion(self, depth_table):
        """Convert from soil grid values to DSSAT required values:
            ref:https://www.isric.org/explore/soilgrids/faq-soilgrids
        """

        # NOTE: here we assume the particle density of all soil particles is
        # similar and around 2.65 g/cm^3 
        # see: https://www.sciencedirect.com/topics/engineering/particle-density

        # NOTE: we assume soil organic compound density is 1.3 g/cm3
        # also see: https://www.sciencedirect.com/topics/engineering/particle-density

        # NOTE: the * 100 at the end is to get values to percentage

        # Convert bulk density
        # Comes in cg/cm^3, we need in grams per cm^3
        depth_table['SBDM'] = depth_table['SBDM'] / 100

        # Calculate weight / weight to cm^3 / cm^3 conversion
        conversion = depth_table['SBDM'] / 2.65

        # convert clay and silt
        depth_table['SLCL'] = (depth_table['SLCL'] / 1000) * conversion * 100
        depth_table['SLSI'] = (depth_table['SLSI'] / 1000) * conversion * 100

        # NOTE: do sand as we need it for PTF calculations
        depth_table['SAND'] = (depth_table['SAND'] / 1000) * conversion * 100

        # soil organic compound concentration, convert using soil bulk density
        # and above value for SOC particle density. value comes in dg/kg
        dg_kg = depth_table['SLOC']
        g_g = dg_kg / 10000
        depth_table['SLOC'] = g_g * (depth_table['SBDM'] / 1.3) * 100

        # Do soil pH
        # Comes in pH * 10, we need in pH
        depth_table['SLHW'] = depth_table['SLHW'] / 10

        # do cation exchange capacity
        # Comes in mmol / kg, need in cmol / kg
        depth_table['SCEC'] = depth_table['SCEC'] / 10


    def _form_soil_DFs(self):
        """Form a pandas dataframe to hold soil data."""

        soil_depth_cols = ['SLB', 'SLMH', 'SLLL', 'SDUL', 'SSAT', 'SRGF',
                           'SSKS', 'SBDM', 'SLOC', 'SLCL', 'SLSI', 'SLCF',
                           'SLNI', 'SLHW', 'SLHB', 'SCEC', 'SADC']    

        # format soil depth dataframe
        soil_depth = pd.DataFrame(columns=soil_depth_cols)
        soil_depth['SLB'] = [5, 15, 30, 60, 100, 200]
        soil_depth.index = soil_depth['SLB']

        # Set soil master horizon per https://doi.org/10.1016/j.envsoft.2019.05.012
        # NOTE: padding values here so they print easily in DSSAT formatting
        soil_depth['SLMH'] = ['A   ', 'A   ', 'AB  ', 'BA  ', 'B   ', 'BC  ']

        # following paper cited in docstring, SLCF and SLHB are both set to -99
        soil_depth['SLCF'] = -99
        soil_depth['SLHB'] = -99
        soil_depth['SADC'] = -99  # SADC not mentioned but it is also -99 in files

        return soil_depth
