# Binamarga Road Events Analysis

dbt project for analyzing road condition data (RNI — *Roughness & Network Inventory*) from the Binamarga national road database.  
Produces **Kemantapan** (road steadiness/condition index) reports by route, segmented by IRI and PCI grading.

## Project Structure

```
models/
├── staging/
│   ├── stg_rni_combined.sql    # Incremental staging model combining RNI data (2022–2025)
│   └── active_lrs.sql           # Active LRS road network data
└── marts/
    ├── kemantapan/
    │   ├── kemantapan_lkm_iri.sql          # Kemantapan by total length (IRI)
    │   ├── kemantapan_lkm_iri_pok.sql      # Kemantapan by total length (IRI POK)
    │   ├── kemantapan_mean_iri.sql         # Kemantapan by mean IRI
    │   ├── kemantapan_mean_iri_pok.sql     # Kemantapan by mean IRI POK
    │   ├── kemantapan_mean_iri_sk.sql      # Kemantapan by mean IRI (SK weighted)
    │   ├── kemantapan_mean_iri_pok_sk.sql  # Kemantapan by mean IRI POK (SK weighted)
    │   ├── kemantapan_max_iri.sql         # Kemantapan by max IRI
    │   ├── kemantapan_max_iri_sk.sql      # Kemantapan by max IRI (SK weighted)
    │   ├── kemantapan_mean_pci.sql         # Kemantapan by mean PCI
    │   ├── kemantapan_mean_pci_sk.sql      # Kemantapan by mean PCI (SK weighted)
    │   ├── kemantapan_max_pci.sql         # Kemantapan by max PCI
    │   └── kemantapan_max_pci_sk.sql      # Kemantapan by max PCI (SK weighted)
    └── rni/
        ├── rekap_lebar_rni.sql              # RNI surface width summary
        ├── rekap_lebar_rni_sk.sql           # RNI surface width summary (SK weighted)
        ├── rekap_tipe_jalan.sql             # RNI road type summary
        ├── rekap_tipe_jalan_sk.sql          # RNI road type summary (SK weighted)
        ├── rekap_tipe_perkerasan.sql        # RNI pavement type summary
        ├── rekap_tipe_perkerasan_lkm.sql    # RNI pavement type summary by length
        └── rekap_tipe_perkerasan_sk.sql     # RNI pavement type summary (SK weighted)

macros/
├── kemantapan.sql          # km_sum & km_sum_inclusive macros for condition length calculation
├── kemantapan_columns.sql  # Reusable column definitions (KM & percentage)
├── kemantapan_columns_sk.sql  # Reusable SK-weighted column definitions
├── get_prov_name.sql       # Province name join and query macro
├── join_satker_balai.sql   # Satker and Balai mapping join macro
├── rni_iri_join.sql        # RNI ↔ Roughness join macro
└── rni_pci_join.sql        # RNI ↔ PCI join macro
```

## Data Sources

| Source       | Schema | Tables                                               |
|-------------|--------|------------------------------------------------------|
| `binamarga` | `SMD`  | `RNI_<semester>_<year>` (e.g., `RNI_1_2022` – `RNI_2_2025`), `SATKER_BALAI_MAP`, `PROVINCE_MAP` |
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

## Recent Features

### Kemantapan Models
- **Max IRI/PCI models**: Added `kemantapan_max_iri` and `kemantapan_max_pci` for condition analysis based on maximum values (including SK-weighted variants)
- **IRI marginal calculations**: Enhanced with `IRI_MARGINAL_UP` and `IRI_MARGINAL_P` columns for marginal condition tracking

### RNI Summary Reports
- **Surface width summaries**: `rekap_lebar_rni` models for road width analysis (with `STD_WIDTH` and `SUB_STD_WIDTH` columns)
- **Road type summaries**: `rekap_tipe_jalan` for classification by road type
- **Pavement type summaries**: `rekap_tipe_perkerasan` for pavement surface analysis (with LKM and SK variants)

### Macros & Enhancements
- **Province mapping**: `get_prov_name` macro for province name resolution with duplicate prevention
- **Satker/Balai joins**: `join_satker_balai` macro for organizational unit mapping
- **SK columns**: `kemantapan_columns_sk` for SK-weighted column definitions
- **Inclusive calculations**: `km_sum_inclusive` macro for inclusive length calculations

### Data Enhancements
- Added `BM_PROV_ID` columns to RNI summary models for province identification
- Province mapping via `PROVINCE_MAP` source table
- Satker/Balai mapping via `SATKER_BALAI_MAP` source table

### Key Concepts

- **Kemantapan**: Road condition index classifying road segments into Good, Fair, Poor, and Bad categories based on IRI or PCI thresholds.
- **IRI** (*International Roughness Index*): A measure of road surface roughness.
- **PCI** (*Pavement Condition Index*): A composite index of pavement distress.
- **POK** (*Pupukan Operasional Kegiatan*): Road segments eligible for operational budget allocation.
- **SK** (*Surat Keputusan*): Weighted calculations based on official decrees/authorizations.
- **Paved vs Unpaved**: Different threshold ranges apply depending on surface type (`SURF_TYPE`).
- **Satker & Balai**: Satuan Kerja (work unit) and Balai (regional office) organizational mappings.
