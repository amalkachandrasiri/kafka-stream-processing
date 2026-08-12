# For CSV files
import pandas as pd

# Load only the first 15,000 rows
df = pd.read_csv('data/Camera_Traffic_Counts_20260811.csv', nrows=15000)

# Save to a new trimmed file
df.to_csv('data/Camera_Traffic_Counts_20260811_trimmed.csv', index=False)
print('Done! Trimmed file saved as trimmed_output.csv')