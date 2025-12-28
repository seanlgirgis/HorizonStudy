-- 1. Hosts Table: The central inventory for the enterprise simulation.
-- node_name serves as the unique identifier (mimicking BMC TrueSight exports).
CREATE TABLE IF NOT EXISTS hosts (
    node_name VARCHAR PRIMARY KEY,
    classification VARCHAR,        -- e.g., Physical, Virtual
    server_type VARCHAR,           -- e.g., Windows, Linux, AIX
    region VARCHAR,                -- e.g., AMER, EMEA, APAC
    cpu_cores INTEGER,
    memory_gb INTEGER,
    storage_capacity_mb INTEGER,
    department VARCHAR,            -- Primary department ownership
    scenario VARCHAR,              -- e.g., STEADY_GROWTH, SEASONAL, BURST
    variant VARCHAR                -- e.g., normal, aggressive, imminent_breach
);

-- 2. Business Hierarchy Table: Maps hosts to the organizational structure.
-- host_id is a foreign key linked to the hosts table.
CREATE TABLE IF NOT EXISTS business_hierarchy (
    host_id VARCHAR PRIMARY KEY,
    department VARCHAR,
    sub_department VARCHAR,
    app_code VARCHAR,              -- Enterprise application identifier
    app_name VARCHAR,
    FOREIGN KEY (host_id) REFERENCES hosts(node_name)
);

-- 3. Time Periods Table: The time dimension for the 3-year simulation.
-- Seeded via Polars to ensure contiguous dates from 2023 to 2025.
CREATE TABLE IF NOT EXISTS time_periods (
    date DATE PRIMARY KEY,
    yearmonth VARCHAR              -- Format: YYYYMM (e.g., 202512) for efficient partitioning
);