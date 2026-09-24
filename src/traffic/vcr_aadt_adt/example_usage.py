"""
Example: how to run the refactored AADT calculator.

This script is standalone and does **not** modify any SMD_Package files.
"""

from aadt_recalc import AADTPipeline

# ---------------------------------------------------------------------------
# Single route
# ---------------------------------------------------------------------------
pipe = AADTPipeline()
result = pipe.calculate(routes=['11010001'], year=2021)
print(result.head())

# ---------------------------------------------------------------------------
# Multiple routes
# ---------------------------------------------------------------------------
result = pipe.calculate(
    routes=['11010001', '11010002', '11010003'],
    year=2021,
    semester=None,   # auto-discovers latest RNI table
)
print(result.head())

# ---------------------------------------------------------------------------
# All routes (use with caution – may be large)
# ---------------------------------------------------------------------------
# result = pipe.calculate(routes='ALL', year=2021)
