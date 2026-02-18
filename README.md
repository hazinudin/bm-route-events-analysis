# Binamarga Road Events Analysis

dbt project for analyzing road condition data (RNI — *Roughness & Network Inventory*) from the Binamarga national road database.  
Produces **Kemantapan** (road steadiness/condition index) reports by route, segmented by IRI and PCI grading.

## Project Structure

```
models/
├── staging/
│   └── stg_rni_combined.sql    # Incremental staging model combining RNI data (2022–2025)
└── marts/
    ├── kemantapan_lkm_iri.sql          # Kemantapan by total length (IRI)
    ├── kemantapan_lkm_iri_pok.sql      # Kemantapan by total length (IRI POK)
    ├── kemantapan_mean_iri.sql         # Kemantapan by mean IRI
    ├── kemantapan_mean_iri_pok.sql     # Kemantapan by mean IRI POK
    ├── kemantapan_mean_iri_sk.sql      # Kemantapan by mean IRI (SK weighted)
    ├── kemantapan_mean_iri_pok_sk.sql  # Kemantapan by mean IRI POK (SK weighted)
    ├── kemantapan_mean_pci.sql         # Kemantapan by mean PCI
    └── kemantapan_mean_pci_sk.sql      # Kemantapan by mean PCI (SK weighted)

macros/
├── kemantapan.sql          # km_sum macro for condition length calculation
├── kemantapan_columns.sql  # Reusable column definitions (KM & percentage)
├── rni_iri_join.sql        # RNI ↔ Roughness join macro
└── rni_pci_join.sql        # RNI ↔ PCI join macro
```

## Data Sources

| Source       | Schema | Tables                                               |
|-------------|--------|------------------------------------------------------|
| `binamarga` | `SMD`  | `RNI_<semester>_<year>` (e.g., `RNI_1_2022` – `RNI_2_2025`) |
| `lrs`       | `ALRS` | `road_network_nat`                                   |

## Usage

### Prerequisites

- Python with `dbt-oracle` adapter installed
- Oracle database connectivity to the Binamarga server
- Database credentials configured in `~/.dbt/profiles.yml`

### Running Models

**Full refresh** (loads all RNI data from 2022–2025):
```bash
dbt run --select stg_rni_combined --full-refresh
```

**Incremental run** — update a specific semester (auto-detects changed routes via `UPDATE_DATE`):
```bash
dbt run --select stg_rni_combined --vars '{"year": 2025, "semester": 2}'
```

**Incremental run** — update specific routes only:
```bash
dbt run --select stg_rni_combined --vars '{"year": 2025, "semester": 2, "routes": ["01001", "01002"]}'
```

**Run kemantapan marts** (requires `year`, `semester` variables):
```bash
dbt run --select kemantapan_lkm_iri --vars '{"year": 2025, "semester": 2}'
```

### Variables

| Variable   | Type       | Required                    | Description                                      |
|-----------|------------|-----------------------------|--------------------------------------------------|
| `year`     | `integer`  | Yes (incremental runs)      | Target year (e.g., `2025`)                       |
| `semester` | `integer`  | Yes (incremental runs)      | Target semester (`1` or `2`)                     |
| `routes`   | `list`     | No                          | List of `LINKID` values to filter; if omitted, updates all changed routes |

### Key Concepts

- **Kemantapan**: Road condition index classifying road segments into Good, Fair, Poor, and Bad categories based on IRI or PCI thresholds.
- **IRI** (*International Roughness Index*): A measure of road surface roughness.
- **PCI** (*Pavement Condition Index*): A composite index of pavement distress.
- **Paved vs Unpaved**: Different threshold ranges apply depending on surface type (`SURF_TYPE`).
