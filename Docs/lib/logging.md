docs/lib/logging.md

### **File Overview: `logging.py**`

The `logging.py` file provides a unified, high-visibility logging framework for the HorizonScale pipeline. It centralizes log formatting, destination management, and performance profiling tools to ensure all telemetry and system events are captured with filename and function context.

---

### **1. Core Logging Initialization**

The module establishes a standardized Root Logger through the `init_root_logging` function. This function ensures that all scripts within the pipeline follow a consistent reporting structure.

* **Dual-Channel Output**: The framework simultaneously writes to a persistent file in the `/logs` directory (at `DEBUG` level) and to the terminal (at `INFO` level).
* **Automated Formatting**: Injects structural metadata into every message, following the pattern: Time - [File:Function] - Level - Message.
* **Idempotency**: Includes logic to clear existing handlers to prevent duplicate log entries if initialization is called multiple times within the same execution context.

### **2. Performance Profiling (Execution Timer)**

A key feature of this module is the `execution_timer` context manager. This utility is designed for performance auditing and bottleneck identification.

* **Real-World Tracking**: Measures the exact duration of a code block using high-resolution performance counters.
* **Human-Readable Reporting**: Automatically calculates and logs the duration in minutes and seconds upon the completion of a task.
* **Usage in Pipeline**: This is used to profile critical tasks such as data generation and forecasting to measure throughput.

### **3. Noise Reduction and Maintenance**

To maintain a high signal-to-noise ratio, the module manages the verbosity of the entire environment.

* **Library Suppression**: Explicitly raises the log level for third-party libraries (like Faker) to suppress unnecessary background noise.
* **Directory Management**: Automatically creates the necessary log directories if they are missing before execution begins.
* **Standardized Log Names**: Uses the script name as the filename to ensure unique identification for post-mortem analysis.

### **4. System Objectives**

* **Traceability**: Ensures every log entry points back to a specific line of code and function.
* **Auditability**: Provides a full record of system behavior for both successful runs and error conditions.
* **Performance Visibility**: Centralizes the reporting of task durations to assist in optimization efforts across the 2,000-server environment.