"""
s
"""

from datetime import datetime
from io import BytesIO

import pandas as pd
import requests_cache
import tabula
from tabula.io import read_pdf


def fix_table(df):
    """
    Pega uma tabela que não tem cabeçalho e foi colocado errado a primeira linha como cabeçalho
    e ajusta, criando a primeira linha com os valores do cabeçalho

    :param df: _description_
    :type df: _type_
    :return: _description_
    :rtype: _type_
    """
    # 1. Transformamos o cabeçalho em uma linha e resetamos o índice das colunas
    df_cabecalho = df.columns.to_frame().T
    df_cabecalho.columns = range(len(df.columns))

    # 2. Resetamos os nomes das colunas do DataFrame original para números
    df.columns = range(len(df.columns))

    # 3. Juntamos a "linha do cabeçalho" com os dados e resetamos o índice
    df = pd.concat([df_cabecalho, df], ignore_index=True)
    return df


class TJSP:
    def __init__(self) -> None:
        self.url = 'https://api.tjsp.jus.br/Handlers/Handler/FileFetch.ashx?codigo=177683'
        # self.dfs = None
        # self.df = None
        self.extract()
        self.create_list_small_tables()
        self.merge_tables()
        self.adjust_data()
        self.adjust_date()
        self.filter()
        self.clean()

    def extract(self):
        # Requests
        session = requests_cache.CachedSession('tjsp_cache')
        r = session.get(
            url=self.url,
            # allow_redirects=True,
        )

        # Read PDF
        self.dfs = tabula.read_pdf(
            input_path=BytesIO(r.content),
            pages='all',
            stream=True,
        )

    def create_list_small_tables(self):

        # ddd
        list_dfs = []

        for n, _ in enumerate(self.dfs):

            # Define a tabela "da vez" e próxima
            df = self.dfs[n]
            try:
                df_1 = self.dfs[n + 1]

            except:
                df_1 = pd.DataFrame([{'mes': 0}])
                # print('não te proxima!')

            # Rename Coluns
            df = df.rename(
                columns={'Unnamed: 0': 'mes'},
                inplace=False,
                errors='ignore',
            )
            df_1 = df_1.rename(
                columns={'Unnamed: 0': 'mes'},
                inplace=False,
                errors='ignore',
            )

            # Se tem 12 registros, passa
            if len(df) == 12:
                df = df.set_index('mes', inplace=False)
                list_dfs.append(df)

            # Se a tabela da vez e a próxima tem menos que 12 linhas
            elif len(df) < 12 and len(df_1) < 12:
                # display(df.head(2))
                # display(df_1.head(2))

                primeira_coluna_df = list(df.columns)[0]
                primeira_coluna_df_1 = list(df_1.columns)[0]
                if (
                    primeira_coluna_df == 'mes'
                    and primeira_coluna_df_1 != 'mes'
                ):
                    df_1 = fix_table(df=df_1)
                    df_1.columns = list(df.columns)[0 : len(df_1.columns)]
                    df = pd.concat([df, df_1], ignore_index=True)
                    if len(df) == 12:
                        # display(df)
                        df = df.set_index('mes', inplace=False)
                        list_dfs.append(df)

                    else:
                        raise Exception('Não tem 12 linhas')

            # Aqui é a tabela "adicionada" anteriormente
            elif len(df) < 12 and len(df_1) == 12:
                pass

            else:
                # display(df.head(2))
                # display(df_1.head(2))
                raise Exception('Que condição é essa?')

            # print('-' * 100)
            # if n == 7:
            #     # break
            #     pass

        # dddd
        self.list_dfs = list_dfs

    def merge_tables(self):
        self.df = pd.concat(self.list_dfs, axis=1)
        self.df = self.df.stack()
        self.df = pd.DataFrame(self.df)
        self.df = self.df.reset_index()

        # Rename Columns
        self.df = self.df.rename(
            columns={'level_1': 'ano', 0: 'taxa'},
            inplace=False,
            errors='ignore',
        )

    def adjust_data(self):

        # Rename Values
        dict_mes = {
            'JAN': 1,
            'FEV': 2,
            'MAR': 3,
            'ABR': 4,
            'MAI': 5,
            'JUN': 6,
            'JUL': 7,
            'AGO': 8,
            'SET': 9,
            'OUT': 10,
            'NOV': 11,
            'DEZ': 12,
        }

        # Ajusta Mês
        df = self.df.replace({'mes': dict_mes})
        df['mes'] = df['mes'].astype(int)

        # Ajusta Ano
        df['ano'] = df['ano'].str.replace(' ', '')
        df['ano'] = df['ano'].astype(int)

        # Ajusta Datas
        df['year'] = df['ano']
        df['month'] = df['mes']
        df['day'] = 1

        df['data'] = pd.to_datetime(df[['year', 'month', 'day']])
        df['data_ref'] = df['data'].dt.strftime('%Y-%m')

        # Drop
        df.drop(['year', 'month', 'day'], axis=1, inplace=True, errors='ignore')

        # Sortear
        df.sort_values('data', inplace=True)

        # Results
        self.df = df

    def adjust_date(self):
        df = self.df
        # Ajusta Taxa
        self.df['taxa_string'] = self.df['taxa']
        self.df['taxa'] = self.df['taxa'].str.replace('-', '', regex=True)
        self.df['taxa'] = self.df['taxa'].str.replace(r'\.', '', regex=True)
        self.df['taxa'] = self.df['taxa'].str.replace(',', '.', regex=True)

        self.df = self.df[self.df['taxa'] != '']
        self.df['taxa'] = self.df['taxa'].astype(float).copy()

    def filter(self):
        # Filtra apenas os registros que estão até o mês atual
        mask = self.df['data'] <= pd.Timestamp(
            datetime.today().year, datetime.today().month, 10
        )
        self.df = self.df[mask]

        # 
        mask = self.df['taxa'].isnull()
        self.df = self.df[~mask]

    def clean(self):
        self.df = self.df.reindex(
            columns=[
                'data',
                'data_ref',
                'ano',
                'mes',
                #'taxa_string',
                'taxa',
            ],
            # copy=True,
        ).copy()

        self.df = self.df.reset_index(
            drop=True,
            inplace=False,
        )
