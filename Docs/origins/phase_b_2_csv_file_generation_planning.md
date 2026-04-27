# HorizonScale – Personal Portfolio Project  
**Phase B: Simulation & Portfolio System Creation**  
**Sub-Task 2: CSV File Generation – Naming Convention Correction**  

**Author:** Sean L Girgis  
**Date:** December 20, 2025  

Thank you for the correction – yes, it's **p95** (95th percentile utilization), not "f95". This aligns perfectly with the original system's resource metrics (P95 CPU/memory/storage utilization). The file naming uses the `p95` convention to reflect percentile data.

### Corrected Input File List & Naming
Updated table with accurate naming (e.g., `cpu_p95_OCT2024.csv`):

| # | File Type | Example File Name | Description & Key Columns | Notes |
|---|-----------|-------------------|---------------------------|-------|
| 1 | CPU Metrics | `cpu_p95_OCT2024.csv` | Daily P95 CPU utilization for physical servers | node_name, date, cpu_p95, max_cores, yearmonth, region |
| 2 | vCPU Metrics | `vcpu_p95_OCT2024.csv` | Daily P95 vCPU utilization for virtual servers | node_name, date, vcpu_p95, max_cores, yearmonth, region |
| 3 | Memory Metrics | `mem_p95_OCT2024.csv` | Daily P95 memory utilization | node_name, date, p95resmem_util, yearmonth, region |
| 4 | Storage Metrics | `sto_p95_OCT2024.csv` | Daily P95 storage utilization | node_resource/node_name, date, P95_Storage_Util_Pct, Storage_Capacity_mb, yearmonth, region |
| 5 | Server Config | `server_config_OCT2024.csv` | Physical server configuration & hierarchy | CI_NAME (= node_name), GOC, LE_ID, app_manager_*, business_owner_* |
| 6 | ESX Guest Config | `esx_guest_config_OCT2024.csv` | Virtual server (VM) configuration & hierarchy | Same as server_config, filtered for virtual |
| 7 | CSI Basic | `csi_basic_102024.csv` | Business ownership and functionality | Hierarchy columns (app_manager_*, business_owner_*) |
| 8 | CSI Roles | `csi_roles_102024.csv` | Role and group email mappings | csi_id, role, group_email |

### Naming Convention Summary (Corrected)
- Resource files: `{resource}_p95_{MMMYYYY}.csv` (e.g., `cpu_p95_OCT2024.csv`)  
- Config files: `{type}_config_{MMMYYYY}.csv`  
- CSI files: `csi_{type}_{MMYYYY}.csv`

### Updated Config Module Snippet
Add to `src/horizonscale/config.py` (for future CSV generation):
```python
# Resource file prefixes (p95 for percentile)
RESOURCE_PREFIXES = {
    "cpu": "cpu_p95",
    "vcpu": "vcpu_p95",
    "mem": "mem_p95",
    "sto": "sto_p95"
}
```

### Suggested Doc Update
**File Name**: `docs/phase_b_2_csv_file_generation_planning.md` (v2)  
**Content**: Paste this corrected table/response.

Commit any updates:
```powershell
git add docs/phase_b_2_csv_file_generation_planning.md src/horizonscale/config.py
git commit -m "Correct p95 naming convention for resource metric files"
git push
```

This ensures 100% alignment with percentile-based metrics. Ready to build the CSV exporter script (`generate_csv_inputs.py`)? Let me know. 🚀