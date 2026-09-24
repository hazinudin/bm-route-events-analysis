"""
aadt_recalc – Standalone AADT/VCR/Capacity Recalculation
========================================================

This package is a **drop-in replacement** for the AADT calculation logic in
``SMD_Package.event_table.kemantapan.service`` (``data_type='AADT'``).

Why it exists
-------------
The original AADT calculation was deeply entangled inside a monolithic
``KemantapanService`` class that mixes concerns:

* ArcPy / Oracle side effects (``os.chdir``, temp-table creation, cursor
  commits) inside ``__init__`` methods
* Hidden circular dependencies (``VCR`` → ``TrafficVolume`` + ``RoadCapacity``
  → each creates its own ``TrafficSummary`` → duplicate temp-table work)
* SQL built via ``.format(**self.__dict__)`` – impossible to unit test without
  a live database
* No separation between data access, business logic, and orchestration

This package **refactors the same formulas** into small, explicit, testable
modules while keeping the numerical output identical.

Old vs New mapping
------------------

==============================================  =============================================
Legacy (``SMD_Package``)                        New (``aadt_recalc``)
==============================================  =============================================
``KemantapanService``                           ``AADTPipeline``
``VCR``                                         ``AADTPipeline._calculate_volume``
``TrafficVolume``                               ``AADTPipeline._calculate_volume``
``RoadCapacity``                                ``AADTPipeline._calculate_capacity``
``TrafficSummary.post_class_aadt_sql()``        ``queries.post_class_aadt_sql()``
``TrafficVolume.calculate_flow()``              In-memory flow computation in pipeline
``TrafficVolume._find_pce()``                   ``calculators.interpolate_pce()``
``TrafficVolume._calculate_pceh()``             ``calculators.calculate_pceh_row()``
``TrafficSummary._add_cesa_col()``              ``calculators.add_cesa()``
``TrafficSummary._add_aadt_col()``              ``calculators.add_aadt_sum()``
``VCR.calculate_aadt_vcr()``                    ``calculators.merge_vcr()``
``SMDConfigs``                                  ``smd_config.SMDConfigs``
``SMD_Package.db_conn.smd_connection``          ``db_conn.smd_connection``
``FindLatest.LatestTable``                      ``latest_table.LatestTable``
``RNISummary.road_type_group_df``               ``rni_summary.road_type_group_df``
==============================================  =============================================

How to use
----------

Basic example
^^^^^^^^^^^^^

.. code-block:: python

    from aadt_recalc import AADTPipeline

    pipe = AADTPipeline()
    result = pipe.calculate(
        routes=['11010001', '11010002'],
        year=2021,
        semester=None,   # None = auto-discover latest RNI table
    )

    # result columns:
    # LINKID, VCR, VOLUME, CAPACITY,
    # NUM_VEH1 .. NUM_VEH7C,
    # CESA, AADT

All routes
^^^^^^^^^^

.. code-block:: python

    result = pipe.calculate(routes='ALL', year=2021)

With a custom connection
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    import oracledb as cx_Oracle
    from aadt_recalc import AADTPipeline
    from aadt_recalc.db_context import DBContext

    conn = cx_Oracle.connect(user='...', password='...', dsn='...')
    pipe = AADTPipeline(connection=conn)
    result = pipe.calculate(routes=['11010001'], year=2021)

Architecture
------------

``AADTPipeline`` (``pipeline.py``)
    The only class most users need to touch.  It wires everything together:

    1. Resolve the correct RNI table name (``latest_table.py``)
    2. Populate ``TEMP_REKAP_ROUTE_SELECTION`` (``db_context.py``)
    3. Fetch post-class AADT via SQL (``queries.py``)
    4. Compute CESA & AADT sum (``calculators.py``)
    5. Compute VOLUME = max(PCEH) per LINKID (``calculators.py``)
    6. Compute CAPACITY = weighted average per LINKID (``queries.py``)
    7. Merge to final VCR output (``calculators.py``)

``queries.py``
    Every SQL string returned here is designed to be byte-for-byte identical
    to the query produced by the legacy ``SMD_Package`` classes.  This is the
    primary defence against numerical drift.

``calculators.py``
    Pure functions that operate on plain ``pandas.DataFrame`` objects.  They
    contain **no database side effects** and can be unit tested with a 5-row
    fixture.

``db_context.py``
    Thin wrapper around the Oracle connection.  Handles temp-table inserts
    and query execution.

``db_conn.py``
    Creates the global ``smd_connection`` exactly the same way the original
    ``SMD_Package.db_conn`` does (reads ``SMD_Package/.env``).

``smd_config.py``
    Loads ``smd_config.json`` from the project root and exposes every top-level
    key as an attribute – identical behaviour to ``SMD_Package.load_config.SMDConfigs``.

Numerical parity
----------------

The following formulas are preserved **exactly** from the legacy code:

* **CESA** = ``sum(AADT_veh × VDF_veh) × 365 × 50.54 / 1,000,000``
* **AADT** = ``sum(all NUM_VEH except NUM_VEH1 & NUM_VEH8)``
* **PCE interpolation** = linear interpolation between two closest FLOW rows
* **Width adjustment** = ``< 6 m → PCE_01 × 1.33``; ``> 8 m → PCE_01 × 0.67``
* **PCEH** = ``Σ(AADT_veh × PCE_veh × HDA_veh)`` per band, then ``max()`` per LINKID
* **Capacity** = ``BASE × FCW × FCSP × FCSF``, weighted by segment length
* **VCR** = ``PCEH / WEIGHTED_CAP``

If you need to verify parity, query ``SMD.AADT`` for a representative route
set, save the result as ``tests/fixtures/golden_aadt.csv``, and compare
column-by-column.

Package layout
--------------

::

    aadt_recalc/
    ├── __init__.py            # exports AADTPipeline
    ├── pipeline.py            # Main orchestrator
    ├── queries.py             # SQL builders (legacy-compatible)
    ├── calculators.py         # Pure math functions
    ├── db_context.py          # Temp-table + query runner
    ├── db_conn.py             # Oracle connection factory
    ├── smd_config.py          # Config loader
    ├── latest_table.py        # RNI table resolver
    ├── rni_summary.py         # Road-type-group helper
    ├── config.py              # Constants (vehicle classes, thresholds, etc.)
    ├── example_usage.py       # Copy-paste examples
    └── data/
        ├── vdf.json           # Vehicle Damage Factors
        └── roadtype_group.json # Road-type group mapping

Dependencies
------------

* ``pandas``
* ``numpy``
* ``oracledb`` (or ``cx_Oracle``)
* ``python-dotenv`` (for reading ``SMD_Package/.env``)

No ``SMD_Package`` import statements remain in production code (only comments
and docstrings mention the legacy module for traceability).
"""

from .pipeline import AADTPipeline

__all__ = ['AADTPipeline']
