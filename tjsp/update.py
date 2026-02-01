"""
ddddd
"""

from paths import data_path
from datetime import datetime
import tjsp_update as tjsp


# Only to change
date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
with open(data_path / 'date.txt', 'w') as f:
    f.write(f'Data: {date_time}')


# Create Dataframe
df = tjsp.get_table()


# Save "tabela_debitos_judiciais"
df.to_csv(
    data_path / 'tabela_debitos_judiciais.csv',
    index=False,
    decimal=',',
)
