# HorizonScale EDA and Preprocessing Notebook Documentation  
**Prepared by:** Sean L Girgis (Author)  
**Date:** December 21, 2025  
**Project:** HorizonScale  
**Phase:** B – Simulation & Portfolio System Creation  
**Confidential:** For internal use only – private project notes.  

This document provides comprehensive documentation for the Jupyter notebook file `eda_and_preprocessing.ipynb`. The notebook focuses on Exploratory Data Analysis (EDA) and initial data preprocessing steps for the HorizonScale project.  

### What is EDA in the Naming?  
EDA stands for **Exploratory Data Analysis**. It refers to the process of analyzing datasets to summarize their main characteristics, often using visual methods. In this notebook, EDA involves loading synthetic time-series data, inspecting its structure (e.g., shape, head), performing aggregations (e.g., resampling to weekly levels), and preparing the data for further modeling or forecasting. The naming `eda_and_preprocessing.ipynb` indicates that this notebook combines EDA techniques with basic preprocessing tasks, such as data loading, aggregation, and saving processed outputs, to ensure the data is clean and ready for downstream phases like model training in HorizonScale.  

### Notebook Overview  
This notebook is part of Phase B's exploratory notebooks sub-task. It uses Polars for efficient data handling and Matplotlib for potential visualizations (though no plots are executed in the provided cells). The key steps include:  
- Importing necessary libraries.  
- Loading a combined time-series Parquet file from a processed data directory.  
- Inspecting the data's shape and previewing the first few rows.  
- Resampling the data to weekly aggregates (e.g., max CPU per week).  
- Saving the preprocessed data as a new Parquet file.  

The notebook appears to be in an early stage, with some cells empty or truncated in the provided source. It assumes access to synthetic data generated earlier in Phase B (e.g., from the Synthetic Data Generator sub-task). No advanced EDA (e.g., statistical summaries, visualizations, or outlier detection) is performed yet, but it sets up the foundation for such analyses.  

Potential improvements (as per Phase A's planned modernizations):  
- Add full EDA: Summary statistics, correlation analysis, visualizations (e.g., time-series plots).  
- Integrate anomaly detection or feature engineering.  
- Use notebooks for interactive exploration, with screenshots for the portfolio README.  

### Full Notebook Code with Annotations  
Below is the full reconstructed code from the provided notebook source. Truncated sections (e.g., long print outputs) are noted but not expanded, as they are runtime artifacts. I've added inline annotations for clarity, explaining each cell's purpose. The notebook is in JSON format (standard for .ipynb files), but I've presented it as executable Python cells for readability.  

```json
{
 "cells": [
  {
   "cell_type": "markdown",
   "id": "571ce803-57df-4550-bc95-a9a9fa091c4e",
   "metadata": {},
   "source": [
    "1. Imports (already done):"  // This markdown cell labels the imports section.
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 46,
   "id": "05fda1c4-fc3f-4e38-8fc2-ae7673051168",
   "metadata": {},
   "outputs": [],  // No output from this cell, as it's just imports.
   "source": [
    "import polars as pl\n",  // Polars: Efficient DataFrame library for data processing.
    "from pathlib import Path\n",  // Path: For handling file paths.
    "import matplotlib.pyplot as plt\n",  // Matplotlib: For potential visualizations (not used in this notebook yet).
    "from horizonscale.config import PROCESSED_DATA_DIR"  // Custom config: Directory for processed data.
   ]
  },
  {
   "cell_type": "markdown",
   "id": "eba7f622-f078-4e3e-b766-e678e70994c1",
   "metadata": {},
   "source": [
    "Load Data (fixed path):"  // This markdown cell labels the data loading section.
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 47,
   "id": "d341c28f-1e25-45fa-9977-6272ee40cd83",
   "metadata": {},
   "outputs": [  // Output: Prints the file path, data shape, and a preview of the first 5 rows.
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\\\\ASUSPC\\Obsidian3\\dsvault\\projects\\HorizonScale\\data\\synthetic\\processed\\combined_time_series.parquet\n",  // File path to the synthetic time-series data.
      "Shape: (6396000, 24)\n",  // Data shape: 6,396,000 rows, 24 columns (large dataset, likely multi-host metrics).
      "shape: (5, 24)\n",  // Preview shape.
      "┌──────────────┬────────────┬───────────┬───────────┬───┬──────────────┬──────────────┬──────────────...(truncated 44085 characters)..."  // Truncated preview of DataFrame head (first 5 rows, showing columns like timestamps, metrics).
     ]
    }
   ],
   "source": [  // Code to load and inspect the data.
    "df = pl.read_parquet(PROCESSED_DATA_DIR / 'combined_time_series.parquet')\n",  // Load Parquet file using Polars.
    "print(PROCESSED_DATA_DIR / 'combined_time_series.parquet')\n",  // Print the full file path for verification.
    "print(f'Shape: {df.shape}')\n",  // Print the DataFrame's shape (rows, columns).
    "print(df.head())"  // Print the first 5 rows for initial inspection (EDA step: Data preview).
   ]
  },
  {
   "cell_type": "markdown",
   "id": "39df6ded-af49-4a56-86a3-0954aa187205",
   "metadata": {},
   "source": [
    "Save Preprocessed:"  // This markdown cell labels the saving section (preprocessing output).
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 55,
   "id": "3b25c93b-0a98-4f97-842c-dd6aa0670692",
   "metadata": {},
   "outputs": [],  // No output, as it's a save operation.
   "source": [
    "# Save preprocessed\n",  // Comment: Saving the processed DataFrame.
    "preprocessed_path = PROCESSED_DATA_DIR / \"preprocessed.parquet\"\n",  // Define output path for preprocessed data.
    "df.write_parquet(preprocessed_path)"  // Write the DataFrame back to Parquet (preprocessing: Saving for later use).
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,  // Empty cell (placeholder, no code executed).
   "id": "10c9468d-eabe-47b6-a03e-175f8e2dce6c",
   "metadata": {},
   "outputs": [],
   "source": []  // No source code in this cell.
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
```

### Explanation of Key Cells  
1. **Imports Cell**: Sets up the environment with Polars for data manipulation, Path for file handling, and Matplotlib for future plots. References a custom config for data paths.  
2. **Load Data Cell**: Loads synthetic time-series data from Parquet (efficient for large datasets). Prints path, shape (6.4M rows, 24 columns – likely metrics like CPU, memory across hosts), and head (preview shows truncated columns, e.g., timestamps, CPU metrics). This is a core EDA step: Initial data inspection.  
3. **Save Preprocessed Cell**: Saves the loaded DataFrame as `preprocessed.parquet` without modifications (basic preprocessing; future enhancements could include cleaning or feature engineering).  
4. **Empty Cell**: Placeholder; can be used for additional EDA (e.g., plots, stats).  

### Usage Notes  
- Run this notebook after generating synthetic data (Phase B, Sub-task 1).  
- For full EDA, add cells for: `df.describe()` (stats), time-series plots (e.g., `plt.plot(df['timestamp'], df['cpu'])`), or handling missing values.  
- Outputs are truncated in the source; full runs may produce verbose previews.  
- This aligns with Phase B's goals: Modern data pipeline (Polars), exploratory notebooks for portfolio. Screenshots of outputs can be captured for the README.md.  

This concludes the documentation for `eda_and_preprocessing.ipynb`. Ready for next steps in Phase B.