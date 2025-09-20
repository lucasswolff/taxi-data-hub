import os
import requests
from dotenv import load_dotenv
import snowflake.connector
import glob
from datetime import datetime

def download_data(url, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    response = requests.get(url)

    if response.status_code == 200:
        
        with open(file_path, 'wb') as f:
            f.write(response.content)
            
        print(f'File downloaded to {file_path}')
    
    elif response.status_code == 403:
        print(f'Download failed. Error code: {response.status_code}. The file might not exist.')

    else:
        print(f'Download failed. Error code: {response.status_code}')

    return response.status_code


def get_year_list():
    current_year = datetime.now().year
    current_month = datetime.now().month

    if current_month <= 2:
        year = current_year - 1
    else: 
        year = current_year

    year_list = [str(x) for x in range(2020, year + 1)]

    return year_list

def upload_via_stage(file_path, table_name, stage_name, cursor):

    try:
        # Create file format for parquet
        cursor.execute("""
            CREATE OR REPLACE FILE FORMAT parquet_format
            TYPE = 'PARQUET'
        """)

        cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")
        
        cursor.execute(f"PUT file://{file_path} @{stage_name}/ AUTO_COMPRESS=TRUE")
        print(f'File {file_path} put to {stage_name}')

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                data VARIANT,
                source_file_name STRING,
                load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
        
        load_time = datetime.now().timestamp()

        cursor.execute(f"""
            COPY INTO {table_name}
            FROM (
                SELECT TO_VARIANT($1), METADATA$FILENAME, {load_time}::timestamp
                FROM @{stage_name}
            )
            FILE_FORMAT = (TYPE = PARQUET)
            """)
        
        print(f'Data copied to Table {table_name}')
        
    except Exception as e:
        print(f"Error: {e}")
        raise        


def main():
    
    #### Snowflake Connection
    print('Connecting to Snowflake...')

    load_dotenv()
    connection_params = {
        'user': os.getenv("SNOWFLAKE_USER"),
        'password': os.getenv("SNOWFLAKE_PASSWORD"),
        'account': os.getenv("SNOWFLAKE_ACCOUNT"),
        'role': os.getenv("SNOWFLAKE_ROLE"),
        'warehouse': os.getenv("SNOWFLAKE_WAREHOUSE"),
        'database': os.getenv("SNOWFLAKE_DATABASE"),
        'schema': 'raw'
    }

    conn = snowflake.connector.connect(**connection_params)
    cursor = conn.cursor()

    #### Download and upload to Snowflake
    print('Starting download and upload to Snowflake')
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
    local_path = '/tmp/'
    
    #vehicles = ['yellow', 'green', 'fhv', 'fhvhv']
    vehicles = ['yellow', 'green']

    #years = get_year_list()
    years = ['2024', '2025']
    
    months = [f'{i:02}' for i in range(1, 13)] # 01, 02, ... 12
    # months = ['01']

    for vehicle in vehicles:

        table_name = vehicle + '_taxi_raw'
        stage_name = vehicle + '_taxi_stage'

        # check for files already in stage 
        try:
            cursor.execute(f"LIST @{stage_name}")
            staged_files = {row[0].split("/")[-1] for row in cursor.fetchall()}
        except:
            # stage doesn't exist - treat as empty
            staged_files = set()

        for year in years:
            for month in months:
               
                url = base_url + vehicle + '_tripdata_' + year + '-' + month + '.parquet'
                
                file_name = vehicle +  '_tripdata_' + year + '_' + month + '.parquet'
                file_path = local_path + file_name

                print(f'Starting process for {file_name}')

                # Check if already in stage. If not, downloads and uploads
                if file_name in staged_files:
                    print(f"{file_name} already in Stage - skipping")
                else:
                    print(f"{file_name} not in Stage - uploading...")

                    download_status_code = download_data(url, file_path)

                    if download_status_code == 200:
                        upload_via_stage(file_path, table_name, stage_name, cursor)
      
                        os.remove(file_path)

    conn.close()
        
if __name__ == '__main__':
    main()

