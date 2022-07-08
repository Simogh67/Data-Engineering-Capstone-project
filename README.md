# Traffic Events Dataset
### Data Engineering Capstone Project

#### Project Summary
In this project, we implement an ETL process for a traffic events dataset via creating a star schema. Our schema enables data scientists to use the dataset stored in 
the data warehouse for purposes such as traffic analysis and prediction, impact prediction, accident prediction, routing engine optimization, travel time estimation.   
The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

** Please download the dataset used in this project from https://smoosavi.org/datasets/lstw.

### Step 1: Scope the Project and Gather Data

This project builds an optimized star schema via 
leveraging Apache Spark. Our data consists of three 
different csv files. 
The first and second files are countrywide traffic 
and weather events, which can be found here https://smoosavi.org/datasets/lstw.

The third file is Airport codes provided by Udacity. 
Based on these three files, we build our data warehouse 
via creating a star schema. Since our data is a big data,
 we use Spark to manage it. Our designed schema
  helps potential users to leverage the dataset for 
 purposes such as traffic analysis and prediction, 
impact prediction, accident prediction, routing engine optimization, 
travel time estimation, and many other research applications.

#### Data Description

**Traffic Events**: this is a countrywide traffic events dataset 
collected from August 2016 to the end of Dec 2020, and covers 49 
states of the United States.

**Weather Events**: this is a countrywide weather events dataset 
collected from August 2016 to the end of Dec 2020, and covers 49 
states of the United States.

**Airport codes**: this worldwide dataset includes airport codes
 and corresponding cities.