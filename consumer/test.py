import pyarrow.parquet as pq
from rich import print
import pandas as pd



table = pq.read_table("E:\Prajwal\DataEngineeringProjects\BankingDataRealTimeAnalytics\consumer\customers_20260704_195030_a228c4e9.parquet")
print(table.to_pandas().head())


# Load the file
df = pd.read_parquet('E:\Prajwal\DataEngineeringProjects\BankingDataRealTimeAnalytics\consumer\customers_20260704_195030_a228c4e9.parquet')

# Option A: Print directly to your terminal as a clean JSON string
print(df.to_json(orient='records', indent=4))