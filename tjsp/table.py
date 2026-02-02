"""
Módulo para extração, tratamento e processamento da tabela de débitos judiciais do TJSP.

Este módulo automatiza o download e processamento da tabela de débitos judiciais
do Tribunal de Justiça de São Paulo (TJSP), incluindo limpeza de dados, normalização
de datas e filtragem de registros.
"""

from datetime import datetime
from io import BytesIO

import pandas as pd
import requests_cache
import tabula
from tabula.io import read_pdf


def fix_table(df):
    """
    Corrige DataFrames que tiveram o cabeçalho lido como linha de dados.

    Transforma os nomes das colunas em uma linha de dados e renumera as colunas
    para garantir consistência com outras tabelas.

    Args:
        df (pd.DataFrame): DataFrame extraído do PDF com cabeçalho incorretamente interpretado.

    Returns:
        pd.DataFrame: DataFrame corrigido com cabeçalho convertido em primeira linha de dados.
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
    """
    Classe para extração e tratamento da tabela de débitos judiciais do TJSP.

    Realiza o download do PDF da tabela de débitos judiciais do Tribunal de Justiça
    de São Paulo, extrai as tabelas, normaliza os dados (datas, meses, anos e taxas)
    e filtra registros até o mês atual.

    Attributes:
        url (str): URL da API do TJSP para download da tabela.
        dfs (list): Lista de DataFrames extraídos do PDF.
        list_dfs (list): Lista de DataFrames após processamento inicial.
        df (pd.DataFrame): DataFrame final após todos os processamentos.
    """

    def __init__(self) -> None:
        """
        Inicializa a classe TJSP e executa todo o pipeline de processamento.

        Chama sequencialmente os métodos para:
        1. Extrair dados do PDF
        2. Processar tabelas pequenas
        3. Mesclar tabelas
        4. Normalizar datas
        5. Limpar valores de taxa
        6. Filtrar registros
        7. Limpar estrutura final
        """
        self.url = 'https://api.tjsp.jus.br/Handlers/Handler/FileFetch.ashx?codigo=177683'
        self._extract()
        self._create_list_small_tables()
        self._merge_tables()
        self._adjust_data()
        self._adjust_taxa()
        self._filter()
        self._clean()

    def _extract(self):
        """
        Baixa o PDF da tabela de débitos judiciais do TJSP e extrai as tabelas.

        Utiliza cache de requisições para evitar downloads repetidos.
        Extrai todas as tabelas do PDF usando a biblioteca tabula.

        Resultado:
            self.dfs (list): Lista de DataFrames extraídos do PDF.
        """
        # Requests
        session = requests_cache.CachedSession('tjsp_cache')
        r = session.get(
            url=self.url,
        )

        # Read PDF
        self.dfs = read_pdf(
            input_path=BytesIO(r.content),
            pages='all',
            stream=True,
        )

    def _create_list_small_tables(self):
        """
        Processa as tabelas extraídas, corrigindo cabeçalhos e mesclando tabelas pequenas.

        Itera sobre as tabelas extraídas e:
        - Renomeia a coluna 'Unnamed: 0' para 'mes'
        - Mantém tabelas com exatamente 12 linhas (12 meses)
        - Mescla tabelas pequenas (< 12 linhas) com a próxima para completar 12 linhas
        - Usa fix_table() para corrigir cabeçalhos incorretos

        Resultado:
            self.list_dfs (list): Lista de DataFrames processados, cada um com 12 linhas.
        """

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

    def _merge_tables(self):
        """
        Mescla todas as tabelas processadas em um único DataFrame.

        Concatena as tabelas ao longo do eixo das colunas, faz stack (transposição),
        reseta índices e renomeia colunas para 'ano' e 'taxa'.

        Resultado:
            self.df (pd.DataFrame): DataFrame mesclado e reestruturado.
        """
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

    def _adjust_data(self):
        """
        Normaliza os valores de mês e ano, cria colunas de data.

        Realiza as seguintes transformações:
        - Converte mês de texto (JAN, FEV, etc.) para número (1-12)
        - Remove espaços do ano e converte para inteiro
        - Cria coluna 'data' com datetime a partir de ano/mês/dia
        - Cria coluna 'data_ref' no formato YYYY-MM
        - Ordena DataFrame por data

        Resultado:
            self.df (pd.DataFrame): DataFrame com datas normalizadas.
        """

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
        df.drop(
            ['year', 'month', 'day'],
            axis=1,
            inplace=True,
            errors='ignore',
        )

        # Sortear
        df.sort_values('data', inplace=True)

        # Results
        self.df = df

    def _adjust_taxa(self):
        """
        Limpa e converte os valores da coluna 'taxa' para float.

        Realiza limpeza de caracteres especiais:
        - Remove hífens e pontos
        - Converte vírgulas em pontos (formato brasileiro para internacional)
        - Remove linhas com taxa vazia
        - Converte valores para float
        - Preserva valores originais em 'taxa_string'

        Resultado:
            self.df (pd.DataFrame): DataFrame com taxa normalizada.
        """
        df = self.df
        # Ajusta Taxa
        self.df['taxa_string'] = self.df['taxa']
        self.df['taxa'] = self.df['taxa'].str.replace('-', '', regex=True)
        self.df['taxa'] = self.df['taxa'].str.replace(r'\.', '', regex=True)
        self.df['taxa'] = self.df['taxa'].str.replace(',', '.', regex=True)

        self.df = self.df[self.df['taxa'] != '']
        self.df['taxa'] = self.df['taxa'].astype(float).copy()

    def _filter(self):
        """
        Filtra o DataFrame para manter apenas registros válidos.

        Aplica dois filtros:
        1. Mantém apenas registros com data até o dia 10 do mês atual
        2. Remove linhas com taxa nula

        Resultado:
            self.df (pd.DataFrame): DataFrame filtrado.
        """
        # Filtra apenas os registros que estão até o mês atual
        mask = self.df['data'] <= pd.Timestamp(
            datetime.today().year, datetime.today().month, 10
        )
        self.df = self.df[mask]

        #
        mask = self.df['taxa'].isnull()
        self.df = self.df[~mask]

    def _clean(self):
        """
        Reordena e prepara o DataFrame final para exportação.

        Seleciona apenas as colunas relevantes na ordem:
        - data
        - data_ref
        - ano
        - mes
        - taxa

        Reseta o índice para garantir numeração sequencial.

        Resultado:
            self.df (pd.DataFrame): DataFrame final limpo e formatado.
        """
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

    def get_value_from_date(self, date):
        # Ajust Date
        if isinstance(date, str):
            date_fix = datetime.strptime(date, '%Y-%m-%d')
        elif isinstance(date, datetime):
            date_fix = date
        else:
            raise Exception('Precisa ser string ou date')
        

        # Json
        mask = (self.df['mes'] == date_fix.month) & (
            self.df['ano'] >= date_fix.year
        )
        return self.df.loc[mask].to_dict('records')[0]
