"""
ddddd
"""

from datetime import datetime

from paths import data_path

import tjsp.table as tjsp


# Only to change
date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
with open(data_path / 'date.txt', 'w') as f:
    f.write(f'Data: {date_time}')


# Create Dataframe
df = tjsp.TJSP()


# Save "tabela_debitos_judiciais"
df.to_csv(
    data_path / 'tabela_debitos_judiciais.csv',
    index=False,
    decimal=',',
)
