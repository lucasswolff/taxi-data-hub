import sys
import os

running_on = 'AWS EMR' if 'HADOOP_CONF_DIR' in os.environ else 'local'

if running_on == 'local':
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))


from curated.utils.create_spark import create_spark_session
from curated.transformations.taxi_transformer import TransformerBase, TransformerYellowGreen
from curated.tests.test_curated_taxi import run_tests
from curated.utils.read_lockup_tables import ReadLockup
from curated.transformations.transform_run_mode import TransformMode

def main(run_mode, months, trans_mode, raw_folder_path, lockup_folder_path, curated_folder_path):
    #### CREATE SPARK
    spark = create_spark_session(app_name="curated_layer")
    
    #### READ FILES
    print('Reading files...')
    
    taxi = 'yellow'
    file_path = trans_mode.get_run_mode_local_files(run_mode, months, raw_folder_path, taxi, running_on) # get file path based on run parameters
    df_yellow_raw = spark.read.option("mergeSchema", "true").parquet(*file_path) 
    
    lockup_reader = ReadLockup()
    df_payment_type, df_ratecode, df_trip_type, df_taxi_zone = lockup_reader.read_lockup_tables(spark, lockup_folder_path)
    
    #### TRANSFORM
    print('Initiating dataframe transformation...')
    
    transformer_yellow = TransformerYellowGreen()
    df_yellow = transformer_yellow.make_yellow_consistent(df_yellow_raw) # add columns with defaut values to be consistent with green taxi

    transformer = TransformerBase()
    df_yellow = transformer.transform_df(df_yellow, df_payment_type, df_ratecode, df_trip_type, df_taxi_zone) 

    #### TEST 
    run_tests(df_yellow)
    print('All tests passed!')
    
    #### SAVE FILES
    print('Saving files...')

    df_yellow.coalesce(1).write \
            .partitionBy('file_year', 'file_month') \
            .mode('overwrite') \
            .parquet(curated_folder_path)
    
        
    print('Successfully wrote files to Curated')
    
if __name__ == '__main__':

    trans_mode = TransformMode()
    run_mode, months = trans_mode.parse_args()


    if running_on == 'local':
        raw_folder_path = "sample_data/raw/yellow/"
        lockup_folder_path = "lockup_tables/"
        curated_folder_path = "sample_data/curated/yellow"
        
    else:
        # mudar o path para secret
        raw_folder_path = "s3://taxi-data-hub/dev/raw/yellow/"
        lockup_folder_path = "s3://taxi-data-hub/dev/lockup_tables/"
        curated_folder_path = "s3://taxi-data-hub/dev/curated/yellow/"

    main(run_mode, months, trans_mode, raw_folder_path, lockup_folder_path, curated_folder_path)


