# 1. Executive Summary
Using Azure Data Factory, I built an ETL pipeline comprising of Databricks notebooks and I used Spark SQL to analyse data to identify potential locations for new EV charge points in South Dublin.

# 2. ETL pipeline
<img width="606" alt="labelled_pipeline" src="https://github.com/johnuzoma/EV-Data-Engineering/assets/18267074/2b54d74f-3801-4b0e-90af-5b8f2fa233eb">


## 2.1. Data extraction and loading process
I used shell commands in a Databricks notebook to download the raw datasets into the **bronze** folder in Databricks filesystem.
Please view this notebook that contains the code [1. Extract EV and demographic data into bronze.ipynb](https://github.com/johnuzoma/EV-Data-Engineering/blob/main/notebooks/1.%20Extract%20EV%20and%20demographic%20data%20into%20bronze.ipynb).
## 2.2. Data cleaning and preparation techniques
- For the geojson file (EV Charging Points), I used PySpark to read it into a dataframe, remove the first 5 rows (which was metadata), and flatten the dataframe so it looks like a proper table. I replaced missing values in string columns with "Unknown", extracted left-most word from **Town** using regular expressions, trimmed string columns, converted datatypes and saved the dataframe as a delta table in silver layer using append mode. Please view this notebook that contains the code [2. Cleanse EV charging point data for silver.ipynb](https://github.com/johnuzoma/EV-Data-Engineering/blob/main/notebooks/2.%20Cleanse%20EV%20charging%20point%20data%20for%20silver.ipynb).

- For the csv file (Vehicle Licensing), I used PySpark to define a schema and read it into a dataframe. I dropped redundant and meaningless columns based on their indices and also renamed some columns. I replaced missing values in string columns with "Unknown", extracted left-most word from **Town** using regular expressions, trimmed string columns, and saved the dataframe as a delta table in silver layer using append mode. Please view this notebook that contains the code [3. Cleanse vehicle licensing data for silver.ipynb](https://github.com/johnuzoma/EV-Data-Engineering/blob/main/notebooks/3.%20Cleanse%20vehicle%20licensing%20data%20for%20silver.ipynb).

- For the csv file (Demographic Data), I used PySpark to define a schema and read it into a dataframe. I dropped redundant columns based on their indices and also renamed some columns. I replaced missing values in string columns with "Unknown", trimmed string columns, and saved the dataframe as a delta table in silver layer. Please view this notebook that contains the code [4. Cleanse demographic data for silver.ipynb](https://github.com/johnuzoma/EV-Data-Engineering/blob/main/notebooks/4.%20Cleanse%20demographic%20data%20for%20silver.ipynb).
## 2.3. Data transformations and analysis
- Using Spark SQL, I aggregated (summed) **number_of_chargers** by **Town** from the charging points delta table. I also aggregated (summed) **No_of_licensed_vehicles** by **Town** for only electric vehicles from the vehicle licenses delta table. Then I used PySpark to perform an inner join between both aggregated dataframes to form a fact dataframe.

- For the demogrphic table, I used Spark SQL to filter **Electoral_divisions** that contain 'south dublin' and **Statistic_label** = 'Population density (persons per sq km)'. Using PySpark, I extracted the left-most word from **Electoral_divisions** using regular expressions. I pivoted **Statistic_label**, taking the SUM of **Value** as its value and I renamed some columns. The result was a dimension dataframe.

- I saved both dataframes as delta tables in the gold layer using append mode. Please view this notebook that contains the code [5. Generate fact and dimension tables for gold.ipynb](https://github.com/johnuzoma/EV-Data-Engineering/blob/main/notebooks/5.%20Generate%20fact%20and%20dimension%20tables%20for%20gold.ipynb).

- For the analysis, I used SQL to write a CTE to calculate the ratio of electric vehicles to chargers to identify areas with high EV concentration and limited charging infrastructure. Then I performed an inner join between the CTE and demographic data to identify potential areas for new charge points, sorted in descending order by both EV-to-charger ratio and population density. The report is shown below:
<img width="920" alt="report" src="https://github.com/johnuzoma/EV-Data-Engineering/assets/18267074/bec98bc7-2c70-47ff-ac4d-d76a7b695ca4">

Please view this notebook that contains the code [6. Generate report.ipynb](https://github.com/johnuzoma/EV-Data-Engineering/blob/main/notebooks/6.%20Generate%20report.ipynb).



