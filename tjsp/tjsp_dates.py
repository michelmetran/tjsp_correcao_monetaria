#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from pathlib import Path
from datetime import datetime
from .tjsp_update import get_table


def get_local_table():
    # Paths
    project_path = Path(__file__).parents[1].resolve()
    data_path = project_path / 'tjsp' / 'data'
    data_path.mkdir(exist_ok=True)

    # Read Data
    df = pd.read_csv(
        data_path / 'tabela_debitos_judiciais.csv',
        parse_dates=['data'],
        decimal=','
    )
    df.loc[:, 'data_ref'] = pd.to_datetime(df['data']).dt.to_period('M')
    return df


def get_tjsp_from_date(date, update_table=False):
    # Ajust Date
    if isinstance(date, str):
        date_fix = datetime.strptime(date, '%Y-%m-%d')
    elif isinstance(date, datetime):
        date_fix = date

    # Get Dataframe
    if not update_table:
        df = get_local_table()
    else:
        df = get_table()

    # Json
    mask = (df['mes'] == date_fix.month) & (df['ano'] >= date_fix.year)
    return df.loc[mask].to_dict('records')[0]


if __name__ == '__main__':
    # Com dia (string)
    dados = get_tjsp_from_date(date='2020-11-15')
    print(dados['taxa'])

    # Com dia (datetime)
    date_fix = datetime.strptime('2021-11-15', '%Y-%m-%d')
    dados = get_tjsp_from_date(date=date_fix)
    print(dados['taxa'])
