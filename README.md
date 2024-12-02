# ETL and Data Pipelines with Shell, Airflow, and Kafka

## Final Assessment Overview

### Objectives:
This project focuses on building an ETL (Extract, Transform, Load) pipeline using Shell, Apache Airflow, and Kafka. The main tasks include:

- **Extracting** data from various file formats: CSV, TSV, and fixed-width files.
- **Transforming** the data into the desired format.
- **Loading** the transformed data into a staging area for further processing.

### Exercise Breakdown:

#### **Exercise 1: Create Imports, DAG Arguments, and Definition**
- **Task 1.1**: Define the DAG arguments (2 pts)  
  Set up the necessary arguments for the DAG (e.g., start date, retries, email notifications).

- **Task 1.2**: Define the DAG (2 pts)  
  Create a DAG with appropriate scheduling and task dependencies.

#### **Exercise 2: Create Tasks Using BashOperator**
- **Task 2.1**: Create a task to unzip data (2 pts)  
  Use the `BashOperator` to unzip the data file into the designated location.

- **Task 2.2**: Create a task to extract data from a CSV file (2 pts)  
  Extract specific fields from a CSV file using Bash commands and save them to a new file.

- **Task 2.3**: Create a task to extract data from a TSV file (2 pts)  
  Use the `awk` command to extract necessary fields from a TSV file and save them as CSV.

- **Task 2.4**: Create a task to extract data from a fixed-width file (2 pts)  
  Use `awk` to extract data from a fixed-width file and format it into CSV.

- **Task 2.5**: Create a task to consolidate data extracted from previous tasks (2 pts)  
  Merge the extracted data into a single CSV file.

- **Task 2.6**: Transform the data (2 pts)  
  Transform the `vehicle_type` field into uppercase and save the transformed data.

- **Task 2.7**: Define the task pipeline (1 pt)  
  Set up the correct task dependencies to ensure the tasks are executed in the proper order.

#### **Exercise 3: Getting the DAG Operational**
- **Task 3.1**: Submit the DAG (1 pt)  
  Submit the DAG for execution in Airflow.

- **Task 3.2**: Unpause and trigger the DAG (3 pts)  
  Unpause the DAG and trigger it manually to start the process.

- **Task 3.3**: List the DAG tasks (2 pts)  
  Use Airflow's CLI or Web UI to list all tasks within the DAG.

- **Task 3.4**: Monitor the DAG (2 pts)  
  Monitor the execution of the DAG and troubleshoot if necessary.


