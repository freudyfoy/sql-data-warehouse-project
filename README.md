# sql-data-warehouse-project
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights.


## Data Architecture

The data architecture for this project follows **Medallion Architecture** Bronze, Silver, and Gold layers:


<img width="1544" height="912" alt="data_architecture" src="https://github.com/user-attachments/assets/2f85c927-e993-4c3f-90f1-db5ea7b6f539" />

**Bronze Layer:** Stores _raw data_ as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.

**Silver Layer:** This layer includes _data cleansing, standardization, and normalization_ processes to prepare data for analysis.

**Gold Layer:** Houses business-ready _data modeled into a star schema_ required for reporting and analytics.
