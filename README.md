# Sharepoint_Shortcut_data_Ms_Fabric
📘 End-to-End Data Engineering Pipeline using Microsoft Fabric & SharePoint
This project demonstrates a complete end-to-end data engineering workflow built using Microsoft Fabric, including data ingestion, transformations, Lakehouse storage layers (Bronze/Silver), semantic modeling, Power BI reporting, and automated orchestration through pipelines.

# 🚀 Project Overview
This repository contains the code, notebooks, and resources used to build a scalable data pipeline that:

Ingests files from SharePoint using OneLake Shortcut
Stores raw data in Lakehouse (Bronze Layer)
Performs data cleaning and transformation using Fabric Notebooks
Saves curated data to Silver Layer
Builds a semantic model on top of Silver data
Creates a Power BI report
Automates the entire workflow using Fabric Data Pipelines
Supports incremental refresh whenever new files are added in SharePoint


# 🏗️ Architecture Workflow
### 1. Data Ingestion (Bronze Layer)

Created a OneLake Shortcut to connect Fabric Lakehouse with a SharePoint document library.
Shortcut ensures auto-sync when new files are added.
Raw data is ingested directly into:
sharepoint_lakehouse > sharepointData_Bronze schema




### 2. Data Transformation (Silver Layer)


Developed a Fabric Notebook to clean and enrich the Bronze data.


### Cleaning logic includes:

Removing unwanted/duplicate rows
Handling null values:

Categorical columns → filled using Mode

Numeric columns → filled using Median


Standardizing column formats
Applying business rules



Final cleaned dataset saved to:
sharepoint_silver schema




### 3. Semantic Modeling

Created a Semantic Model on top of the Silver tables.
Implemented:

Relationships
Measures
Data formatting
Business-friendly naming conventions




## 4. Power BI Reporting

Built a Power BI report using the semantic model.
Visuals automatically reflect changes when new data flows through the pipeline.


## 5. Data Pipeline Orchestration


# Created a Fabric Data Pipeline to orchestrate:

Bronze ingestion
Transformation notebook execution
Silver updates
Semantic model refresh
Power BI dataset refresh



# Pipeline scheduled for automated execution.



## 6. Automatic Refresh When New SharePoint Files Arrive

#### When new files are added to the SharePoint folder:

Shortcut syncs automatically
Pipeline refresh triggers
Notebook cleans & processes new rows
Silver table updates
Semantic model refreshes
Power BI report shows new data instantly



This ensures a fully automated, scalable, and maintenance-free workflow.

# 📁 Folder Structure
├── notebooks/

│   └── data_cleaning_notebook.ipynb

├── pipelines/

│   └── sharepoint_end_to_end_pipeline.json

├── reports/

│   └── power_bi_report.pbix

└── README.md


# 🛠️ Technologies Used

Microsoft Fabric
OneLake Shortcut
Lakehouse (Delta Tables)
PySpark / Python Notebooks
Power BI
Semantic Models
Fabric Pipelines
SharePoint Online


# 🌟 Key Features

#### 🔄 Fully automated ETL/ELT workflow
#### 🗂️ Multi-layer architecture: Bronze → Silver
#### ⚙️ Notebook-driven data transformations
#### 🔗 Live SharePoint integration via OneLake shortcuts
#### 📊 Dynamic Power BI reporting
#### ⏱️ Scheduled + event-driven refresh
#### 📈 Supports incremental new-row ingestion
