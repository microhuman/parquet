import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# 定义 Schema
schema = pa.schema([
    ('id', pa.int32()),
    ('name', pa.string()),
    ('age', pa.int32())
])

# prepare data
ids = pa.array([1, 2], type = pa.int32())
names = pa.array(['a', 'b'], pa.string())
ages = pa.array([20, 21], type = pa.int32())

# create Parquet data
batch = pa.RecordBatch.from_arrays(
    [ids, names, ages],
    schema = schema
)
table = pa.Table.from_batches([batch])

# write Parquet 
pq.write_table(table, 'plain.parquet')

# log 
schema = pq.read_schema('plain.parquet')
print(schema)
 
df = pd.read_parquet('plain.parquet')
print(df.to_json())

# show schema
# java -jar parquet-tools-1.6.0rc3.jar schema -d plain.parquet

# show data
# java -jar parquet-tools-1.6.0rc3.jar  cat  plain.parquet


