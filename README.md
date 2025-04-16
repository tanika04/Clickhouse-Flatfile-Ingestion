This web application facilitates bidirectional data transfer between ClickHouse databases and flat files (CSV). It supports column selection, JWT authentication, and provides progress monitoring and reporting.  

**Features**  

-Bidirectional Data Flow:  

  ClickHouse → Flat File export  
  Flat File → ClickHouse ingestion  


-ClickHouse Integration: 

  JWT token authentication  
  Table and column discovery  
  Data type inference  


-Multi-Table Join:  

  Join multiple tables from ClickHouse before export  
  Custom join condition configuration  



-User Interface:  
  
  Data preview before ingestion  
  Column selection  
  Progress monitoring  
  Completion reporting  


**Setup and Installation** 

**Prerequisites**  
Python 3.7+  
ClickHouse Cloud  
Web browser  

**Installation**  

1. Clone this repository:  
git clone https://github.com/yourusername/Clickhouse-Flatfile-Ingestion.git  
cd Click house-Flatfile-Ingestion  

2. Create and activate a virtual environment (optional but recommended):  
python -m venv venv  
source venv/bin/activate  # On Windows: venv\Scripts\activate  

3. Install the required packages:  
pip install flask clickhouse-connect pandas werkzeug numpy  


4. Create uploads folder (if not using git clone):  
mkdir uploads  

Configuration  
No additional configuration is required beyond what is provided in the UI. The application uses a local folder named 'uploads' to store files temporarily. 

**Running the Application** 

1. Start the application:  
python app.py  


2. Open your web browser and navigate to:  
http://localhost:5000

**Usage Instructions**  

**Flat File → ClickHouse**  

1. Select "Flat File → ClickHouse" from the dropdown  
2. Upload a CSV file and specify the delimiter  
3. Review the data preview and select columns to ingest  
4. Enter ClickHouse connection details and target table name  
5. Click "Ingest to ClickHouse" to start the process  
6. Monitor progress and view completion status  


**ClickHouse → Flat File**  


1. Select "ClickHouse → Flat File" from the dropdown  
2. Enter ClickHouse connection details and click "Connect & Fetch Tables"  
3. Select a table and click "Load Table Columns"  
4. Select columns to export  
5. Click "Export to CSV" to download the data  

**ClickHouse Multi-Table Join → Flat File**  


1. Select "ClickHouse Multi-Table Join → Flat File" from the dropdown    
2. Enter ClickHouse connection details and click "Connect & Fetch Tables"  
3. Add tables to the join  
4. Configure join conditions between tables    
5. Select columns to include in the export  
6. Click "Execute Join & Export to CSV" to download the data
   
