# HorizonScale – Personal Portfolio Project  
**Official Project Charter & Execution Plan**  
Goal: Build a 100% original, production-grade, resume-defining capacity planning & forecasting system from scratch, inspired by (but never copying) an existing internal tool you have access to.

**Project Owner:** Sean L Girgis  

**Target Completion:** 3–4 weeks (fast but extremely high quality)  
**Outcome:** Public GitHub repository + 12–18 page technical paper + killer resume bullets + interview-ready talking points

### Phase B – Sub-task 1: Synthetic Data Generator (Script Breakdown in Pseudo-Code)

Awesome job installing `sqlite3` and verifying the DB run! Since it was successful (5,000 hosts seeded, hierarchies and time periods in place), we're locked and loaded for the next script.

Now, as requested: Here's a **super comprehensive, dummy-proof breakdown** of what `00_init_db.py` does – explained in **pseudo-code** (high-level, human-readable steps like a recipe or adventure story). I'll make it fun and engaging, like guiding a total newbie through a "database-building quest." Think of it as a step-by-step map with explanations for every twist and turn. No jargon overload – just clear, awesome insights so you can visualize exactly how the code "builds" your synthetic infrastructure world.

#### The Epic Quest: What `00_init_db.py` Does (Pseudo-Code Adventure)
Imagine you're the architect of a massive virtual data center (HorizonScale). This script is your "foundation builder" – it creates a lightweight SQLite database (like a digital blueprint book) to store all the fake-but-realistic server details. It's Stage 0: Setting up the skeleton before we add the "life" (time-series data) in the next script.

Here's the pseudo-code – broken into chapters (main sections), with plain-English explanations, why-it-matters notes, and what-happens-under-the-hood vibes. It's comprehensive, so grab a coffee!

**Chapter 0: Gather Your Tools and Set the Scene**  
(Imports and Setup – Like packing your backpack for the quest)  
```
IMPORT necessary magic spells: sqlite3 (for talking to databases), polars (fast data wrangling), faker (fake name/ID generator), datetime (date handling), argparse (command-line options), random (random numbers), logging (diary entries), Path (file paths)

FROM your project's config file, GRAB constants like:  
- DB_PATH (where the database file lives, e.g., 'data/synthetic/horizonscale_synth.db')  
- SQL_SCHEMA_DIR (folder with the table blueprints)  
- REGIONS (list: ['NA', 'EMEA', 'LATAM', 'ASIAPAC'])  
- CLASSIFICATIONS (['physical', 'virtual'])  
- SERVER_TYPES (['Unix', 'Linux', 'Windows', 'Other'])  
- DEPARTMENTS (business units like ['Retail Banking', 'Investment Banking'])  
- TIME_START ('2023-01-01'), TIME_END ('2025-12-01'), TIME_INTERVAL ('1d')  
- DEFAULT_NUM_HOSTS (2000, but you overrode to 5000)

SET UP a diary (logging) to record adventures:  
- Level: INFO (tell me important stuff)  
- Format: Timestamp - Level - Message  
GET a logger named after the script
```
**Why this matters:** This loads all the "ingredients" and settings. Without this, the script can't find paths or know what regions/departments to use. It's like prepping your kitchen before cooking – efficient and error-proof.

**Chapter 1: Craft Employee IDs Like a Spy Agency**  
(Function: generate_id – A mini-tool for making realistic IDs)  
```
DEFINE FUNCTION generate_id(name: string) -> string:  
    SPLIT the name into parts (e.g., "John Doe" -> ['John', 'Doe'])  
    IF fewer than 2 parts (rare fallback):  
        TAKE first and last letter of name, uppercase them + random 6-digit number  
    ELSE:  
        GRAB first initial (e.g., 'J'), last initial ('D'), uppercase  
        ADD random 6-digit number (e.g., 123456)  
    COMBINE into ID like 'JD123456'  
    RETURN the ID
```
**Why this matters (dummy edition):** In real companies, employee IDs are short and memorable. This function fakes them based on names (using Faker later) so your business hierarchy feels authentic – no generic "ID123". It's a small but cool detail for realism in your portfolio.

**Chapter 2: The Grand Database Build – Init the World!**  
(Function: init_db – The heart of the script, your "world creator")  
```
DEFINE FUNCTION init_db(db_path: Path, num_hosts: int) -> Nothing:  
    LOG: "Starting the build at {db_path}"  

    CONNECT to the database file (create if missing)  

    // Step 1: Lay the Foundations (Create Tables)  
    FIND schema file: SQL_SCHEMA_DIR + '01_create_tables.sql'  
    IF file missing: YELL ERROR and stop  
    READ the file (SQL commands like CREATE TABLE hosts(...))  
    EXECUTE the SQL script to build tables: hosts, business_hierarchy, time_periods  
    LOG: "Foundations built!"  

    CREATE a Faker wizard (US-style names for realism)  

    // Step 2: Spawn the Servers (Seed Hosts Table)  
    CALCULATE combos: num_classifications (2) * num_server_types (4) = 8  
    HOSTS_PER_COMBO = num_hosts // 8 (e.g., 5000 // 8 = 625 per combo)  

    DEFINE scenarios list: ['steady_growth', 'seasonal', 'burst', 'low_idle', 'capacity_breach', 'plateau_decline']  

    PREPARE empty list for host data  
    FOR each classification (physical/virtual):  
        FOR each server_type (Unix/Linux/etc.):  
            FOR 625 times (hosts per combo):  
                INVENT node_name: 'server-' + random UUID snippet (e.g., 'server-abc12345')  
                PICK random region from REGIONS  
                CHOOSE max_cores: If physical, bigger options [16,32,64]; else smaller [2,4,8,16]  
                CHOOSE memory_gb: Powers of 2 like [4,8,16,...,512]  
                CHOOSE storage_mb: Random between 500GB (500,000 MB) and 5TB (5,000,000 MB)  
                ASSIGN random scenario from list (for later patterns like growth or bursts)  
                ADD tuple to host_data list: (node_name, cls, stype, region, cores, mem, storage, scenario)  

    INSERT all hosts into 'hosts' table (OR IGNORE duplicates)  
    LOG: "Seeded {5000} hosts."  

    // Step 3: Assign Managers and Owners (Seed Business Hierarchy)  
    PREPARE empty list for hierarchy data  
    FOR each host in host_data:  
        // App Manager (one "person" per host)  
        INVENT app_name with Faker (e.g., 'Alice Johnson')  
        CREATE app_goc ID from name  
        CREATE app_le_id from name  
        SET app_le_name = app_name  
        CREATE app_ms_id from name  
        PICK random department for app_ms_name  

        // Business Owner (totally different "person")  
        INVENT business_name with Faker (e.g., 'Bob Smith')  
        CREATE business_goc, le_id, ms_id similarly  
        SET business_le_name = business_name  
        PICK random department for business_ms_name  

        ADD tuple: (node_name, app_goc, app_le_id, ..., business_ms_name, 'NO' for alerts)  

    INSERT all into 'business_hierarchy' (OR IGNORE duplicates)  
    LOG: "Seeded {5000} hierarchies."  

    // Step 4: Create the Calendar (Seed Time Periods)  
    PARSE start_date (2023-01-01) and end_date (2025-12-01)  
    GENERATE date list with Polars: Every 1 day from start to end (1066 dates)  
    FORMAT each: (YYYY-MM-DD, YYYYMM like '202301')  
    INSERT into 'time_periods' (OR IGNORE duplicates)  
    LOG: "Added {1066} time periods."  

    SAVE changes (commit) and CLOSE connection  

    LOG: "Build complete!"  
    PRINT friendly summary: DB path, hosts seeded, combos, time periods
```
**Why this matters (dummy edition):** This is where the magic happens! It builds 3 tables:  
- **hosts**: Your "server catalog" – 5,000 fake servers with types, sizes, regions, and "personality" (scenario for how their usage will behave later). Even distribution ensures balance (no bias toward one type).  
- **business_hierarchy**: Links servers to fake "people" and departments – makes it feel like a real company org chart.  
- **time_periods**: A calendar table for daily data (helps with forecasting dates).  
The "OR IGNORE" makes it safe to re-run without duplicates. Raises errors only if schema file is missing.

**Chapter 3: Launch the Quest**  
(Main Block – Like hitting "start" on your game)  
```
IF this is the main script (not imported):  
    CREATE argument parser with description  
    ADD option: --num_hosts (default 2000, help text)  
    PARSE command-line args  
    CALL init_db with DB_PATH and args.num_hosts
```
**Why this matters:** Lets you customize (e.g., --num_hosts 5000) without editing code. Professional touch!

#### Quest Complete: Overall Impact
This script turns an empty file into a populated DB (~50–100 MB) in seconds – your foundation for synthetic data. It's idempotent (re-run safe), logged (track issues), and realistic (thanks to Faker and thoughtful options). In the project, this DB will be queried in the next script to generate usage patterns over time (e.g., CPU spikes for "burst" scenarios).

If anything's unclear, hit me up – this is dummy-proof level 9000! 😎

### Next Steps
With the DB verified, we're ready for **`01_generate_synthetic_metrics.py`** – the script that breathes life into these hosts with daily utilization data (trends, seasons, bursts, etc.). Let me know if you want to add a `--reset` flag to auto-delete the DB, or if we're good to proceed with coding the metrics generator. 🚀