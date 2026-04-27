# HorizonScale – Personal Portfolio Project  
**Phase B: Simulation & Portfolio System Creation**  
**Sub-Task 1: Synthetic Data Generator Discussion & Planning**  

### Agreement on Your Idea  
Yes, I completely agree – starting with a synthetic data generator is the ideal first step for Phase B. Without access to real input files from the original system, we need to create realistic, controllable datasets to bootstrap the entire pipeline. This ensures we can test end-to-end functionality, simulate various scenarios (e.g., normal growth, sudden bursts, seasonal patterns, anomalies, or edge cases like low utilization), and validate our models without relying on proprietary data.  

Key benefits:  
- **Realism**: We can mimic the structure, scale (1,000–5,000 hosts as per charter), and variability of the original Trenda inputs.  
- **Testability**: Generate diverse scenarios (e.g., steady trends, exponential growth, failures) to stress-test preprocessing, forecasting, and alerting.  
- **Portability**: Outputs as CSVs in the same format as original files, stored in `/data/synthetic/` for easy loading.  
- **Modularity**: Build the generator as a standalone script (`src/horizonscale/generate_synthetic_data.py`) with configurable parameters (e.g., num_hosts, time_range, scenario_type).  

We'll implement it in Python using libraries like NumPy (for trends/noise), Pandas (for DataFrames/CSVs), and Faker (for realistic node names). No external tools needed yet – we can prototype in a Jupyter notebook (`notebooks/synthetic_data_exploration.ipynb`) for quick iterations. Once ready, commit it with tests (`tests/test_synthetic_generator.py`).  

Next: After listing the files below, we can outline the generator code skeleton in our next exchange.  

### List of Input Files from Original System  
Based on the Phase A Discovery Report, the original Trenda system ingests several types of CSV files via the loader module. These are primarily monthly feeds for resource metrics, configurations, and support data. File names often encode dates (e.g., "MMMYYYY" like "OCT2019") or qualifiers (e.g., "csi_MMYYYY.csv"). Below, I list the key input file types, inferred/example names (since exact names vary by cycle), and their typical fields (schemas derived from code excerpts and annotations).  

Note: Fields are based on Hive table schemas and processing logic. All files are CSVs; resource files are region-agnostic but processed regionally. We'll generate synthetic versions matching these structures.  

1. **CPU Resource Metrics (F95 File)**  
   - **Description**: Monthly CSV with P95 CPU utilization data for physical servers. Used for forecasting CPU needs.  
   - **Example File Name**: `cpu_f95_MMMYYYY.csv` (e.g., `cpu_f95_OCT2019.csv`).  
   - **Fields**:  
     - `node_name`: STRING – Unique server/host identifier (e.g., "server-abc123").  
     - `date`: DATE – Timestamp of the measurement (e.g., "2019-10-01").  
     - `cpu_p95`: FLOAT – 95th percentile CPU utilization (e.g., 75.5).  
     - `max_engines`: INT – Maximum capacity/engines (e.g., 16).  
     - `yearmonth`: STRING – Partition key (e.g., "201910").  
     - `region`: STRING – Geographic region (e.g., "NA").  

2. **vCPU Resource Metrics (F95 File)**  
   - **Description**: Monthly CSV for virtual CPU (VM) utilization, similar to CPU but for VMs.  
   - **Example File Name**: `vcpu_f95_MMMYYYY.csv` (e.g., `vcpu_f95_OCT2019.csv`).  
   - **Fields**:  
     - `node_name`: STRING – VM/host identifier.  
     - `date`: DATE – Measurement date.  
     - `vcpu_p95`: FLOAT – 95th percentile vCPU utilization.  
     - `max_engines`: INT – VM capacity.  
     - `yearmonth`: STRING – Partition key.  
     - `region`: STRING – Region.  

3. **Memory Resource Metrics (F95 File)**  
   - **Description**: Monthly CSV for memory (MEM) utilization.  
   - **Example File Name**: `mem_f95_MMMYYYY.csv` (e.g., `mem_f95_OCT2019.csv`).  
   - **Fields**:  
     - `node_name`: STRING – Host identifier.  
     - `date`: DATE – Measurement date.  
     - `p95resmem_util`: FLOAT – 95th percentile memory utilization percentage.  
     - `yearmonth`: STRING – Partition key.  
     - `region`: STRING – Region.  

4. **Storage Resource Metrics (F95 File)**  
   - **Description**: Monthly CSV for storage (STO) utilization, often larger and split for NA region.  
   - **Example File Name**: `sto_f95_MMMYYYY.csv` (e.g., `sto_f95_OCT2019.csv`).  
   - **Fields**:  
     - `node_resource`: STRING – Storage resource/cluster identifier.  
     - `node_name`: STRING – Host/node identifier.  
     - `region`: STRING – Region.  
     - `date`: DATE – Measurement date.  
     - `P95_Storage_Util_Pct`: FLOAT – 95th percentile storage utilization percentage.  
     - `Storage_Capacity_mb`: FLOAT – Capacity in MB.  
     - `yearmonth`: STRING – Partition key.  

5. **Server Configuration File (CON Pair – Part 1)**  
   - **Description**: CSV for server configs, paired with ESX Guest; used for device-level info.  
   - **Example File Name**: `server_config_MMMYYYY.csv` (e.g., `server_config_OCT2019.csv`).  
   - **Fields**:  
     - `CI_NAME`: STRING – Configuration item name (e.g., host ID).  
     - `GOC`: STRING – Global operations code.  
     - `LE_ID`: STRING – Legal entity ID.  
     - `app_manager_goc`: STRING – App manager GOC.  
     - `app_manager_le_id`: STRING – App manager legal entity ID.  
     - `app_manager_le_name`: STRING – App manager legal entity name.  
     - `app_manager_ms_id`: STRING – App manager market segment ID.  
     - `app_manager_ms_name`: STRING – App manager market segment name.  
     - `business_owner_goc`: STRING – Business owner GOC (added for hierarchies).  
     - `business_owner_le_id`: STRING – Business owner legal entity ID.  
     - `business_owner_le_name`: STRING – Business owner legal entity name.  
     - `business_owner_ms_id`: STRING – Business owner market segment ID.  
     - `business_owner_ms_name`: STRING – Business owner market segment name.  

6. **ESX Guest Configuration File (CON Pair – Part 2)**  
   - **Description**: CSV for ESX/VM guest configs (updated v2 format post-2024).  
   - **Example File Name**: `esx_guest_config_MMMYYYY.csv` (e.g., `esx_guest_config_OCT2019.csv`).  
   - **Fields**: Similar to server config, with VM-specific additions:  
     - `CI_NAME`: STRING – VM item name.  
     - `GOC`: STRING.  
     - `LE_ID`: STRING.  
     - (Same hierarchy fields as above: app_manager_*, business_owner_*).  

7. **CSI Basic Functionality File (Support/CMDB)**  
   - **Description**: CSV for app ownership and functionality from CMDB/CSI.  
   - **Example File Name**: `csi_basic_MMYYYY.csv` (e.g., `csi_basic_102019.csv`).  
   - **Fields**:  
     - `app_manager_goc`: STRING.  
     - `app_manager_le_id`: STRING.  
     - `app_manager_le_name`: STRING.  
     - `app_manager_ms_id`: STRING.  
     - `app_manager_ms_name`: STRING.  
     - `business_owner_goc`: STRING.  
     - `business_owner_le_id`: STRING.  
     - `business_owner_le_name`: STRING.  
     - `business_owner_ms_id`: STRING.  
     - `business_owner_ms_name`: STRING.  
     - Additional: Roles, emails, suppress flags.  

8. **CSI Roles File (Support/CMDB)**  
   - **Description**: CSV for CSI roles and groups.  
   - **Example File Name**: `csi_roles_MMYYYY.csv`.  
   - **Fields**:  
     - `csi_id`: STRING – CSI identifier.  
     - `role`: STRING – Role (e.g., app manager).  
     - `group_email`: STRING – Support group email.  

9. **Notification Rules File (Support)**  
   - **Description**: CSV for alerting/suppression rules (part of BUSDEV load).  
   - **Example File Name**: `notification_rules_MMYYYY.csv`.  
   - **Fields**:  
     - `app_group`: STRING – Application group.  
     - `suppress`: STRING – Suppression flag (e.g., "YES").  
     - `risk_threshold`: FLOAT – Risk levels for alerts.  

This covers all major inputs from the original system. In the generator, we'll create CSVs for each, with options for scenarios (e.g., `--scenario growth` or `--num_hosts 2000`). Thoughts on parameters or first code draft?