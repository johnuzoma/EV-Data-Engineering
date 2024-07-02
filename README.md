This project focuses on building an ETL pipeline and analysing data to identify potential locations for new EV charge points in South Dublin.

# ETL pipeline
<img width="607" alt="labelled_pipeline" src="https://github.com/johnuzoma/EV-Data-Engineering/assets/18267074/75b125b9-1e13-46a6-833a-6d6fe1a4a7ad">


## Data extraction and loading process
I used shell commands in a Databricks notebook to download the raw datasets into the **bronze** folder in Databricks filesystem.
Please view this notebook that contains the code [1. Extract EV and demographic data into bronze.ipynb](https://github.com/johnuzoma/EV-Data-Engineering/blob/main/notebooks/1.%20Extract%20EV%20and%20demographic%20data%20into%20bronze.ipynb).
## Data cleaning and preparation techniques
- For the geojson file (EV Charging Points), I used PySpark to read it into a dataframe, remove the first 5 rows (which was metadata), and flatten the dataframe so it looks like a proper table. I replaced missing values in string columns with "Unknown", extracted left-most word from **Town** using regular expressions, trimmed string columns, converted datatypes and saved the dataframe as a delta table in silver layer.
## Data transformations and analysis

