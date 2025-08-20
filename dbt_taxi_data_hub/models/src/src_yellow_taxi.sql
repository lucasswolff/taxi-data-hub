select *
from read_parquet('../raw_data/yellow/*.parquet', union_by_name = true, filename=true)
