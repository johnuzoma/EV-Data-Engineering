# 1. Executive summary
Using Azure Data Factory, I built an ETL pipeline comprising of Databricks notebooks. In those notebooks, I wrote shell commands to extract data, PySpark code to cleanse the data and Spark SQL code to analyse the data to identify potential locations for new EV charge points in South Dublin.

# 2. How to use CI/CD pipelines to migrate code from one environment to another (development, test, production)
Azure Data Factory (ADF) supports CI/CD by integrating with Azure DevOps and Azure Resource Manager templates. Below is an overview of how I would implement a CI/CD in an Azure Data Factory environment configured with Azure Repos Git.

## 2.1. Tools I would use
1. **Azure Data Factory**: For creating and managing data pipelines.
2. **Azure DevOps**: For version control, build automation, and release management.
3. **Azure Resource Manager (ARM) Templates**: For defining the infrastructure and configurations of ADF entities.

## 2.2. CI/CD lifecycle steps
1. **Development Environment**
    - Setup: Configure a development data factory with Azure Repos Git.
    - Branching: Developers create feature branches to work on new features or bug fixes.
    - Debugging: Use iterative development and debugging techniques to ensure changes work as expected.
    - Pull Requests: Once changes are ready, create a pull request from the feature branch to the main branch for peer review.
2. **Testing Environment**
    - Publishing: After pull requests are approved and merged, changes are published to the development data factory.
    - Deployment to Testing: Use Azure Pipelines to deploy the desired version of the development factory to a User Acceptance Testing (UAT) factory. This involves:
      - Configuring ARM template parameters to apply the appropriate settings.
      - Running pre-deployment scripts to stop triggers and perform necessary cleanup.
      - Deploying the ARM templates using Azure DevOps tasks.
      - Running post-deployment scripts to restart triggers and verify deployment.
3. **Production Environment**
    - Verification: After changes are validated in the testing environment, deploy to the production factory using Azure Pipelines.
    - Consistency: Ensure that the production factory mirrors the tested configuration by applying the same ARM templates and parameters.

## 2.2. Best practices for CI/CD in data engineering
1. **Git Integration**: Only configure the development data factory with Git. Deploy changes to testing and production environments using CI/CD pipelines without Git integration.
2. **Pull Requests**: Give pull requests the names that are descriptive of what they do. It’s also good practice to leave comments about the pull requests for reviewers.
3. **Pre- and Post-Deployment Scripts**: Use PowerShell scripts to manage triggers and perform cleanup tasks before and after deployments. Ensure you use the latest versions of PowerShell and the ADF module to avoid errors.
4. **Integration Runtimes**: Maintain the same name, type, and sub-type of integration runtimes across all environments to avoid configuration issues. Consider using a shared factory for self-hosted integration runtimes.
5. **Key Vault Management**: Use separate Azure Key Vaults for different environments with the same secret names to simplify parameterization.
6. **Resource Naming**: Avoid spaces in resource names to prevent ARM template deployment issues. Use underscores (_) or hyphens (-) instead.
7. **Feature Flags**: Use global parameters and the **if condition** activity to manage feature flags, allowing you to control which features are active in different environments.
8. **Avoid Manual Alterations**: Do not manually add or alter files in the ADF Git repository, as this can cause loading errors.

# 3. My development ETL pipeline for the project
<img width="606" alt="labelled_pipeline" src="https://github.com/johnuzoma/EV-Data-Engineering/assets/18267074/2b54d74f-3801-4b0e-90af-5b8f2fa233eb">

## 3.1. Data extraction and loading process
I used shell commands in a Databricks notebook to download the raw datasets into the **bronze** folder in Databricks filesystem.
Please view this notebook that contains the code [1. Extract EV and demographic data into bronze.ipynb](https://github.com/johnuzoma/EV-Data-Engineering/blob/main/notebooks/1.%20Extract%20EV%20and%20demographic%20data%20into%20bronze.ipynb).
## 3.2. Data cleaning and preparation techniques
- For the geojson file (EV Charging Points), I used PySpark to read it into a dataframe, removed the first 5 rows (which was metadata), and flattened the dataframe so it looks like a proper table. I replaced missing values in string columns with "Unknown", extracted the left-most word from **Town** using regular expressions, trimmed string columns, converted datatypes and saved the dataframe as a delta table in silver layer using append mode. Please view this notebook that contains the code [2. Cleanse EV charging point data for silver.ipynb](https://github.com/johnuzoma/EV-Data-Engineering/blob/main/notebooks/2.%20Cleanse%20EV%20charging%20point%20data%20for%20silver.ipynb).

- For the csv file (Vehicle Licensing), I used PySpark to define a schema and read it into a dataframe. I dropped redundant and meaningless columns based on their indices and also renamed some columns. I replaced missing values in string columns with "Unknown", extracted the left-most word from **Town** using regular expressions, trimmed string columns, and saved the dataframe as a delta table in silver layer using append mode. Please view this notebook that contains the code [3. Cleanse vehicle licensing data for silver.ipynb](https://github.com/johnuzoma/EV-Data-Engineering/blob/main/notebooks/3.%20Cleanse%20vehicle%20licensing%20data%20for%20silver.ipynb).

- For the csv file (Demographic Data), I used PySpark to define a schema and read it into a dataframe. I dropped redundant columns based on their indices and also renamed some columns. I replaced missing values in string columns with "Unknown", trimmed string columns, and saved the dataframe as a delta table in silver layer using append mode. Please view this notebook that contains the code [4. Cleanse demographic data for silver.ipynb](https://github.com/johnuzoma/EV-Data-Engineering/blob/main/notebooks/4.%20Cleanse%20demographic%20data%20for%20silver.ipynb).
## 3.3. Data transformations and analysis
- Using Spark SQL, I aggregated (summed) **Number_of_chargers** by **Town** from the charging points delta table. I also aggregated (summed) **No_of_licensed_vehicles** by **Town** for only electric vehicles from the vehicle licenses delta table. Then I used PySpark to perform an inner join between both aggregated dataframes to form a fact dataframe.

- For the demogrphic table, I used Spark SQL to filter **Electoral_divisions** that contain 'south dublin' and **Statistic_label** = 'Population density (persons per sq km)'. Using PySpark, I extracted the left-most word from **Electoral_divisions** using regular expressions. I pivoted **Statistic_label**, taking the SUM of **Value** as its value and I renamed some columns. The result was a dimension dataframe.

- I saved both dataframes as delta tables in the gold layer using append mode. Please view this notebook that contains the code [5. Generate fact and dimension tables for gold.ipynb](https://github.com/johnuzoma/EV-Data-Engineering/blob/main/notebooks/5.%20Generate%20fact%20and%20dimension%20tables%20for%20gold.ipynb).

- For the analysis, I used SQL to write a CTE (Common Table Expression) to calculate the ratio of electric vehicles to chargers to identify areas with high EV concentration and limited charging infrastructure. Then I performed an inner join between the CTE and demographic table to identify potential areas for new charge points, sorted in descending order by both EV-to-charger ratio and population density. The report is shown below:
<img width="918" alt="report" src="https://github.com/johnuzoma/EV-Data-Engineering/assets/18267074/2b828357-5a60-435b-a54a-3dc7d018b4ff">

Please view this notebook that contains the code [6. Generate report.ipynb](https://github.com/johnuzoma/EV-Data-Engineering/blob/main/notebooks/6.%20Generate%20report.ipynb).



