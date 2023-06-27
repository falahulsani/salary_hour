**This script is used to insert data from CSV files into PostgreSQL tables and execute a SQL query on the database. **

**Here is an explanation of the folder structure in the script:**
1. The **core** folder this folder contains the files serves to store modules related to CSV file processing and PostgreSQL connection.
2. The **query** folder contains files that contain query manipulations for calculating "salary_per_hour". These files are used to perform the necessary data manipulations.
3. The **source** folder contains the source data that will be used in the analysis or processing. This folder is used to store the data files that will be imported or processed by the script.

**Setup:**
1. Create your virtual environment:
   ````
   conda create -n salary_hour
   conda activate salary_hour
   ````
2. Install the required library in your terminal:
   ````
   pip install -r requirements.txt
   ````
3. Execute the script by running it with Python:
   ````
   python main.py
   ````
4. After the python code has finished, you can use the following query into BI Tools:
   ````
   SELECT year, month, branch_id, salary_per_hour FROM salary_hour
   ````
