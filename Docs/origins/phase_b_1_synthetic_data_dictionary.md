# HorizonScale – Personal Portfolio Project  
**Phase B: Simulation & Portfolio System Creation**  
**Sub-Task 1: Synthetic Data Generator – Data Dictionary Document**  

**Author:** Sean L Girgis  
**Date:** December 20, 2025  

### Data Dictionary for Synthetic Database  
**Database:** horizonscale_synth.db (SQLite)  
**Purpose:** Centralized master data for synthetic generation of realistic infrastructure metrics (hosts, hierarchies, time periods). Used to ensure consistency across exported CSVs for CPU, vCPU, MEM, STO, CON, and CSI inputs.  

The database contains three tables: `hosts`, `business_hierarchy`, and `time_periods`. Below is a detailed data dictionary.

#### Table: hosts  
Stores core server/host entities with classification, type, and practical capacity specs.

| Column                  | Data Type | Constraints                  | Description                                                                 | Example Values                  |
|-------------------------|-----------|------------------------------|-----------------------------------------------------------------------------|---------------------------------|
| node_name              | TEXT     | PRIMARY KEY                 | Unique identifier for the host/server (used as foreign key in other tables) | server-abc12345                |
| classification         | TEXT     | NOT NULL                    | Type of host: physical (bare-metal) or virtual (VM)                        | physical, virtual              |
| server_type            | TEXT     | NOT NULL                    | Operating system/environment type                                          | Unix, Linux, Windows, Other    |
| region                 | TEXT     | NOT NULL                    | Geographic region (for regional forecasting)                               | NA, EMEA, LATAM, ASIAPAC       |
| max_cores              | INTEGER  |                             | Maximum CPU cores (power-of-2, higher for physical)                        | 16, 32, 64 (physical); 2, 4, 8, 16 (virtual) |
| memory_gb              | INTEGER  |                             | Total memory in GB (power-of-2, practical enterprise values)               | 4, 8, 16, 32, 64, 128, 256, 512 |
| storage_capacity_mb    | REAL     |                             | Total storage capacity in MB (500GB–5TB range)                             | 500000.0 to 5000000.0          |

#### Table: business_hierarchy  
Stores application and business ownership details linked to hosts via node_name. Fields are distinct for app_manager and business_owner.

| Column                      | Data Type | Constraints                  | Description                                                                 | Example Values                  |
|-----------------------------|-----------|------------------------------|-----------------------------------------------------------------------------|---------------------------------|
| node_name                  | TEXT     | NOT NULL, FOREIGN KEY to hosts | Links to host entity                                                        | server-abc12345                |
| app_manager_goc            | TEXT     |                             | Global Operations Code for app manager                                      | GOC-456                        |
| app_manager_le_id          | TEXT     |                             | Legal Entity ID for app manager                                             | LE-78901                       |
| app_manager_le_name        | TEXT     |                             | Legal Entity Name for app manager                                           | Citi Retail Services           |
| app_manager_ms_id          | TEXT     |                             | Market Segment ID for app manager                                           | MS-2345                        |
| app_manager_ms_name        | TEXT     |                             | Market Segment Name for app manager (business unit)                         | Retail Banking                 |
| business_owner_goc         | TEXT     |                             | Global Operations Code for business owner                                   | GOC-123                        |
| business_owner_le_id       | TEXT     |                             | Legal Entity ID for business owner                                          | LE-56789                       |
| business_owner_le_name     | TEXT     |                             | Legal Entity Name for business owner                                        | Citigroup North America        |
| business_owner_ms_id       | TEXT     |                             | Market Segment ID for business owner                                        | MS-6789                        |
| business_owner_ms_name     | TEXT     |                             | Market Segment Name for business owner                                      | Financial Services             |
| suppress_alerts            | TEXT     | DEFAULT 'NO'                | Flag to suppress notifications for this host                                | NO, YES                        |

#### Table: time_periods  
Stores daily timestamps for time-series generation (historical + future buffer).

| Column     | Data Type | Constraints                  | Description                                                                 | Example Values                  |
|------------|-----------|------------------------------|-----------------------------------------------------------------------------|---------------------------------|
| date       | DATE     | PRIMARY KEY                 | Full date (YYYY-MM-DD)                                                      | 2023-01-01                     |
| yearmonth  | TEXT     | NOT NULL                    | Partition key (YYYYMM)                                                      | 202301                         |

### Notes
- **Total Rows (approx.)**: ~20,000 hosts (distributed across combinations), ~1,066 time periods (2023-01-01 to 2025-12-01).  
- **Indexes**: Added for performance (region, node_name).  
- **Usage**: Query joins on node_name for enrichment; time_periods for generating daily metrics (e.g., P95 util).  
