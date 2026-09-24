"""
Configuration constants extracted from the existing SMD calculation logic.
All values are preserved exactly to guarantee numerical parity.
"""

# ---------------------------------------------------------------------------
# Vehicle classifications (from TrafficVolume)
# ---------------------------------------------------------------------------
LIGHT_VEHICLES = [
    'NUM_VEH1', 'NUM_VEH2', 'NUM_VEH3', 'NUM_VEH4', 'NUM_VEH5A'
]
HEAVY_VEHICLES = [
    'NUM_VEH5B', 'NUM_VEH6A', 'NUM_VEH6B',
    'NUM_VEH7A', 'NUM_VEH7B', 'NUM_VEH7C'
]
ALL_VEHICLE_COLS = LIGHT_VEHICLES + HEAVY_VEHICLES

# Columns excluded from the AADT sum (_add_aadt_col / excluded_veh_cols)
AADT_SUM_EXCLUDE = ['NUM_VEH8', 'NUM_VEH1']

# Prefix used to identify vehicle columns in RTC / AADT tables
VEHICLE_COL_PREFIX = 'NUM_VEH'

# ---------------------------------------------------------------------------
# CESA / VCR constants (from TrafficSummary)
# ---------------------------------------------------------------------------
R_VALUE = 50.54               # CESA multiplier
CESA_MULTIPLIER = 365.0       # days per year
CESA_SCALE = 1_000_000.0      # divisor for CESA formula

# ---------------------------------------------------------------------------
# PCE constants (from TrafficVolume)
# ---------------------------------------------------------------------------
PCE_TABLE = 'PCE_COEFFICIENT'
FLOWBAND_TABLE = 'FLOWBAND'
PCE_COL_PREFIX = 'PCE'
PCEH_COL = 'PCEH'

# Width adjustment factors for PCE_01
WIDTH_LT_6_FACTOR = 1.33
WIDTH_GT_8_FACTOR = 0.67
WIDTH_THRESHOLD_LOW = 6.0
WIDTH_THRESHOLD_HIGH = 8.0

# ---------------------------------------------------------------------------
# Capacity constants (from RoadCapacity)
# ---------------------------------------------------------------------------
CAPACITY_COEFF_TABLE = 'CAPACITY_COEFF'
AADT_TABLE_DEFAULT = 'SMD.AADT'
VEH8_COL_DEFAULT = 'NUM_VEH8'

# ---------------------------------------------------------------------------
# Terrain mapping (from TrafficVolume & RoadCapacity)
# ---------------------------------------------------------------------------
TERRAIN_MAP = {'F': 1, 'R': 2, 'H': 3}

# ---------------------------------------------------------------------------
# RNI / RTC table naming
# ---------------------------------------------------------------------------
RTC_TABLE_FMT = 'RTC_{year}'
RNI_TABLE_FMT = 'RNI_{semester}_{year}'
TEMP_ROUTE_TABLE = 'TEMP_REKAP_ROUTE_SELECTION'
TEMP_RTC_TABLE = 'TEMP_RTC'

# ---------------------------------------------------------------------------
# Post-description tables
# ---------------------------------------------------------------------------
POSTDESC_TABLE = 'POSTDESC'
POSTCLASS_TABLE = 'POST_CLASS'
ROAD_TYPE_GROUP_TABLE = 'ROAD_TYPE_GROUP'  # implicit via RNISummary

# ---------------------------------------------------------------------------
# Standard column names (from SMDConfigs.table_fields)
# These are fallbacks; the pipeline prefers live values from SMDConfigs.
# ---------------------------------------------------------------------------
ROUTEID_COL = 'LINKID'
DATE_COL = 'SURVEY_DATE'
HOUR_COL = 'SURVEY_HOURS'
MINUTE_COL = 'SURVEY_MINUTE'
SURVEY_DIR_COL = 'SURVEY_DIREC'
CLASS_COL = 'TRAFPOST'

# RNI fields
RNI_ROUTEID = 'LINKID'
RNI_FROM_M = 'FROM_STA'
RNI_TO_M = 'TO_STA'
RNI_LANE_CODE = 'LANE_CODE'
RNI_ROAD_TYPE = 'ROAD_TYPE'
RNI_LANE_WIDTH = 'LANE_WIDTH'
RNI_LEFT_TERR = 'LEFT_TERRAIN'
RNI_RIGHT_TERR = 'RIGHT_TERRAIN'
RNI_SEGMENT_LEN = 'SEGMENT_LENGTH'
RNI_LI_SH_W = 'LEFT_INNER_SH_W'
RNI_LO_SH_W = 'LEFT_OUTER_SH_W'
RNI_RI_SH_W = 'RIGHT_INNER_SH_W'
RNI_RO_SH_W = 'RIGHT_OUTER_SH_W'

# ---------------------------------------------------------------------------
# Output column ordering (from VCR.calculate_aadt_vcr)
# ---------------------------------------------------------------------------
OUTPUT_BASE_COLS = ['LINKID', 'VCR', 'VOLUME', 'CAPACITY']
OUTPUT_SUFFIX_COLS = ['CESA', 'AADT']
