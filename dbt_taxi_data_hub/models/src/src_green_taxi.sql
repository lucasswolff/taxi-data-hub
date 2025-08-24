select *
from read_parquet('../raw_data/green/*.parquet', union_by_name = true, filename=true)

