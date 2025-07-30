import os
import json
import pandas as pd
import re
from unidecode import unidecode

class PorDataProcess:
    folderPathPorData = '..\\..\\Bases\\PorData\\'
    folderPathInePt = '..\\..\\Bases\\InePt\\'
    filePathBaseAgregada = '..\\..\\Bases\\PorData\\base_agregada\\estatisticas_agregadas.csv'
    dfs = []

    # Função para ler o arquivo JSON e retornar seu conteúdo como uma lista de dicionários
    @staticmethod
    def read_json_file(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                return json.load(file)
        except FileNotFoundError:
            print(f"Error: O arquivo {file_path} não foi encontrado.")
        except json.JSONDecodeError:
            print(f"Error: Falha ao decodificar JSON no arquivo {file_path}.")
        return []

    # Função para ler arquivos CSV a partir de uma pasta
    @staticmethod
    def read_csv_files_from_folders(folder_path):
        dataframes = []
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(root, file)
                    print(f'Processando arquivo: {file_path}')
                    try:
                        df = pd.read_csv(file_path, sep=',', encoding='utf-8', low_memory=False)
                        dataframes.append({
                            'df_folder': os.path.basename(root),
                            'df_name': os.path.splitext(file)[0],
                            'df': df
                        })
                    except Exception as e:
                        print(f"Erro ao ler {file}: {e}")
        return dataframes

    # Função para tratar o DataFrame de cultura e cinemas
    @staticmethod
    def trata_df_cultura( df_cultura_cinemas):
        df_cultura_cinemas = df_cultura_cinemas[df_cultura_cinemas['03. Âmbito Geográfico'].notnull()]
        last_year = df_cultura_cinemas['01. Ano'].max()
        df_cultura_cinemas = df_cultura_cinemas[df_cultura_cinemas['01. Ano'] == last_year]

        df_cultura_cinemas = df_cultura_cinemas[['01. Ano', '02. Nome Região (Portugal)', '03. Âmbito Geográfico', '09. Valor']]
        df_cultura_cinemas = df_cultura_cinemas.rename(columns={
            '01. Ano': 'Ano',
            '02. Nome Região (Portugal)': 'Região',
            '03. Âmbito Geográfico': 'Âmbito Geográfico',
            '09. Valor': 'Valor'
        })
        df_cultura_cinemas['Ano'] = df_cultura_cinemas['Ano'].astype(int)
        df_cultura_cinemas['Valor'] = df_cultura_cinemas['Valor'].astype(int)
        return df_cultura_cinemas.reset_index(drop=True)

    # Função para encontrar um DataFrame pelo nome
    @staticmethod
    def find_df_by_name(dataframes, df_name):
        for df_info in dataframes:
            if df_info['df_name'] == df_name:
                return df_info['df']
        raise ValueError(f"DataFrame com o nome '{df_name}' não encontrado.")

    @staticmethod
    def adiciona_df_tratado(Categoria, Nome, df):
        self.dfs.append({
        'Categoria': Categoria,
        'df_name': Nome,
        'df': df
        })
    @staticmethod
    # Função para listar cidades com cinemas
    def lista_cidades_cinemas( df_cultura_cinemas):
        df_cultura_cinemas = df_cultura_cinemas.groupby(['Região', 'Âmbito Geográfico']).agg({'Valor': 'sum'}).reset_index()
        df_cultura_cinemas = df_cultura_cinemas.sort_values(by='Valor', ascending=False).reset_index(drop=True)
        print("Cidades com mais cinemas:")
        for _, row in df_cultura_cinemas.iterrows():
            print(f"{row['Região']}: {row['Valor']} cinemas")
    @staticmethod
    # Função para tratar o DataFrame de população residente
    def trata_df_populacao_residente(df_populaccao_residente):
        df_populaccao_residente = df_populaccao_residente[df_populaccao_residente['04. Âmbito Geográfico'].notnull()]
        df_populaccao_residente = df_populaccao_residente[
            (df_populaccao_residente['03. Nome Região (Portugal)'] != 'Portugal') &
            (df_populaccao_residente['03. Nome Região (Portugal)'].notnull()) &
            (df_populaccao_residente['03. Nome Região (Portugal)'].str.strip() != '')
        ]

        last_year = df_populaccao_residente['01. Ano'].max()
        df_populaccao_residente = df_populaccao_residente[df_populaccao_residente['01. Ano'] == last_year]
        df_populaccao_residente_sexo = df_populaccao_residente[df_populaccao_residente['06. Filtro 2'] == 'Total']

        df_sexo_pivot = df_populaccao_residente_sexo.pivot_table(
                index=['01. Ano', '03. Nome Região (Portugal)'],
                columns='05. Filtro 1',
                values='10. Valor',
                aggfunc='sum',
                fill_value=0
            )

        df_sexo_pivot.reset_index(inplace=True)

        df_populaccao_residente_faixa = df_populaccao_residente[df_populaccao_residente['05. Filtro 1'] == 'Total']
        df_faixa_pivot = df_populaccao_residente_faixa.pivot_table(
            index=['01. Ano', '03. Nome Região (Portugal)'],
            columns='06. Filtro 2',
            values='10. Valor',
            aggfunc='sum',
            fill_value=0
        )
        df_faixa_pivot.reset_index(inplace=True)

        df_populacao_final = pd.merge(df_sexo_pivot, df_faixa_pivot, on=['03. Nome Região (Portugal)'], how='left')
        df_populacao_final.reset_index(inplace=True)

        df_populacao_final = df_populacao_final.rename(columns={
            '01. Ano_x': 'Ano',
            '03. Nome Região (Portugal)': 'Região',
            'Homens': 'Homens',
            'Mulheres': 'Mulheres',
            '0 - 4 anos': '0 - 4 anos',
            '5 - 9 anos': '5 - 9 anos',
            '10 - 14 anos': '10 - 14 anos',
            '15 - 19 anos': '15 - 19 anos',
            '20 - 24 anos': '20 - 24 anos',
            '25 - 29 anos': '25 - 29 anos',
            '30 - 34 anos': '30 - 34 anos',
            '35 - 39 anos': '35 - 39 anos',
            '40 - 44 anos': '40 - 44 anos',
            '45 - 49 anos': '45 - 49 anos',
            '50 - 54 anos': '50 - 54 anos',
            '55 - 59 anos': '55 - 59 anos',
            '60 - 64 anos': '60 - 64 anos',
            '65 - 69 anos': '65 - 69 anos',
            '70 - 74 anos': '70 - 74 anos',
            '75 - 79 anos': '75 - 79 anos',
            '80 - 84 anos': '80 - 84 anos',
            '85 ou mais anos': '85 ou mais anos',
            'Total_y': 'Total População'
        })

        df_populacao_final = df_populacao_final.astype({
                'Ano': 'int',
                'Região': 'str',
                'Homens': 'int',
                'Mulheres': 'int',
                '0 - 4 anos': 'int',
                '5 - 9 anos': 'int',
                '10 - 14 anos': 'int',
                '15 - 19 anos': 'int',
                '20 - 24 anos': 'int',
                '25 - 29 anos': 'int',
                '30 - 34 anos': 'int',
                '35 - 39 anos': 'int',
                '40 - 44 anos': 'int',
                '45 - 49 anos': 'int',
                '50 - 54 anos': 'int',
                '55 - 59 anos': 'int',
                '60 - 64 anos': 'int',
                '65 - 69 anos': 'int',
                '70 - 74 anos': 'int',
                '75 - 79 anos': 'int',
                '80 - 84 anos': 'int',
                '85 ou mais anos': 'int',
                'Total População': 'int'
            })
        df_populacao_final.drop(columns=['index','Total_x','01. Ano_y'], inplace=True)

        # Calcular faixas etárias e proporções
        df_populacao_final['Total Crianças'] = df_populacao_final['0 - 4 anos'] + df_populacao_final['5 - 9 anos']
        df_populacao_final['Total Adolescentes'] = df_populacao_final['10 - 14 anos'] + df_populacao_final['15 - 19 anos']
        df_populacao_final['Total Jovens Adultos'] = df_populacao_final['20 - 24 anos']
        df_populacao_final['Total Adultos'] = df_populacao_final[['25 - 29 anos', '30 - 34 anos', '35 - 39 anos', '40 - 44 anos', 
                                                            '45 - 49 anos', '50 - 54 anos', '55 - 59 anos']].sum(axis=1)
        df_populacao_final['Total Idosos'] = df_populacao_final[['60 - 64 anos', '65 - 69 anos', '70 - 74 anos', '75 - 79 anos', 
                                                            '80 - 84 anos', '85 ou mais anos']].sum(axis=1)

        # Proporções de cada faixa etária
        df_populacao_final['Proporção Crianças'] = (df_populacao_final['Total Crianças'] / df_populacao_final['Total População']) * 100
        df_populacao_final['Proporção Adolescentes'] = (df_populacao_final['Total Adolescentes'] / df_populacao_final['Total População']) * 100
        df_populacao_final['Proporção Jovens Adultos'] = (df_populacao_final['Total Jovens Adultos'] / df_populacao_final['Total População']) * 100
        df_populacao_final['Proporção Adultos'] = (df_populacao_final['Total Adultos'] / df_populacao_final['Total População']) * 100
        df_populacao_final['Proporção Idosos'] = (df_populacao_final['Total Idosos'] / df_populacao_final['Total População']) * 100


        return df_populacao_final
    @staticmethod
    # Função para listar cidades por grupo etário
    def cidades_por_grupo(df, categoria='Todos', n=5):
        categorias = {
            'Crianças': 'Proporção Crianças',
            'Adolescentes': 'Proporção Adolescentes',
            'Jovens Adultos': 'Proporção Jovens Adultos',
            'Adultos': 'Proporção Adultos',
            'Idosos': 'Proporção Idosos',
            'Populosas': 'Total População'
        }
        
        if categoria not in categorias and categoria != 'Todos':
            print("Categoria inválida!")
            return
        
        if categoria == 'Todos':
            categorias_list = categorias.keys()
        else:
            categorias_list = [categoria]

        for cat in categorias_list:
            cidade_ordenada = df[['Ano', 'Região', categorias[cat]]].sort_values(by=categorias[cat], ascending=False).head(n)
            print(f"\nCidades com mais {cat} (%) - Top {n}:")
            for _, row in cidade_ordenada.iterrows():
                proporcao = row[categorias[cat]]
                print(f"{row['Região']}: {int(proporcao)}%")
    @staticmethod
    def trata_df_populacao_densidade(df_densidade_populacional):

        df_densidade_populacional = df_densidade_populacional.dropna(axis=1, how='all')

        df_densidade_populacional = df_densidade_populacional[df_densidade_populacional['03. Nome Região (Portugal)'].notnull()]

        last_year = df_densidade_populacional['01. Ano'].max()
        df_densidade_populacional = df_densidade_populacional[df_densidade_populacional['01. Ano'] == last_year]

        df_densidade_populacional = df_densidade_populacional[df_densidade_populacional['03. Nome Região (Portugal)'] != 'Portugal']

        df_densidade_populacional = df_densidade_populacional[['01. Ano', '03. Nome Região (Portugal)', '04. Âmbito Geográfico', '10. Valor']]

        df_densidade_populacional.rename(columns={
            '01. Ano': 'Ano',
            '03. Nome Região (Portugal)': 'Região',
            '04. Âmbito Geográfico': 'Âmbito Geográfico',
            '10. Valor': 'Valor'
        }, inplace=True)

        df_densidade_populacional = df_densidade_populacional.astype({
            'Ano': 'int',
            'Região': 'str',
            'Âmbito Geográfico': 'str',
            'Valor': 'float'
        })

        return df_densidade_populacional.reset_index(drop=True)
    @staticmethod
    def trata_crimes_catetoria(df_seguranca_crime_catetoria):
        
        df_seguranca_crime_catetoria = df_seguranca_crime_catetoria.dropna(axis=1, how='all')
        df_seguranca_crime_catetoria = df_seguranca_crime_catetoria[df_seguranca_crime_catetoria['02. Nome Região (Portugal)'] != 'Portugal']

        last_year = df_seguranca_crime_catetoria['01. Ano'].max()
        df_seguranca_crime_catetoria = df_seguranca_crime_catetoria[df_seguranca_crime_catetoria['01. Ano'] == last_year]

        df_seguranca_crime_catetoria = df_seguranca_crime_catetoria[['01. Ano', '02. Nome Região (Portugal)', '03. Âmbito Geográfico', '04. Filtro 1', '09. Valor']]

        df_seguranca_crime_catetoria.rename(columns={
            '01. Ano': 'Ano',
            '02. Nome Região (Portugal)': 'Região',
            '03. Âmbito Geográfico': 'Âmbito Geográfico',
            '04. Filtro 1': 'Categoria',
            '09. Valor': 'Valor'
        }, inplace=True)

        df_seguranca_crime_catetoria = df_seguranca_crime_catetoria.astype({
            'Ano': 'int',
            'Região': 'str',
            'Âmbito Geográfico': 'str',
            'Categoria': 'str',
            'Valor': 'float'
        })

        # filtering the dataframe to show only rows where 'Total' is not null
        df_seguranca_crime_catetoria = df_seguranca_crime_catetoria[df_seguranca_crime_catetoria['Valor'].notnull()]

        df_seg_categoria = df_seguranca_crime_catetoria.pivot_table(
                index=['Ano', 'Região'],
                columns='Categoria',
                values='Valor',
                aggfunc='sum',
                fill_value=0
        )

        df_seg_categoria.reset_index(inplace=True)

        return df_seg_categoria
    @staticmethod
    def trata_crimes_tipo(df_seg_crimes_tipo):
        df_seg_crimes_tipo = df_seg_crimes_tipo.dropna(axis=1, how='all')
        df_seg_crimes_tipo = df_seg_crimes_tipo[df_seg_crimes_tipo['03. Nome Região (Portugal)'] != 'Portugal']
        df_seg_crimes_tipo = df_seg_crimes_tipo[df_seg_crimes_tipo['03. Nome Região (Portugal)'].notna()]

        last_year = df_seg_crimes_tipo['01. Ano'].max()
        df_seg_crimes_tipo = df_seg_crimes_tipo[df_seg_crimes_tipo['01. Ano'] == last_year]

        df_seg_crimes_tipo = df_seg_crimes_tipo.drop( columns= ['09. Símbolo','08. Escala'], axis=1)

        df_seg_crimes_tipo = df_seg_crimes_tipo[['01. Ano', '03. Nome Região (Portugal)', '04. Âmbito Geográfico', '05. Filtro 1', '10. Valor']]

        df_seg_crimes_tipo.rename(columns={
            '01. Ano': 'Ano',
            '03. Nome Região (Portugal)': 'Região',
            '04. Âmbito Geográfico': 'Âmbito Geográfico',
            '05. Filtro 1': 'Tipo de Crime',
            '10. Valor': 'Valor'
        }, inplace=True)

        df_seg_crimes_tipo = df_seg_crimes_tipo.astype({
            'Ano': 'int',
            'Região': 'str',
            'Âmbito Geográfico': 'str',
            'Tipo de Crime': 'str',
            'Valor': 'float'
        })

        df_seg_crimes_tipo = df_seg_crimes_tipo.pivot_table(
                index=['Ano', 'Região'],
                columns='Tipo de Crime',
                values='Valor',
                aggfunc='sum',
                fill_value=0
        )

        return df_seg_crimes_tipo.reset_index(drop=False)
    @staticmethod
    def trata_hospitais(df_saude_hospitais):
        df_saude_hospitais = df_saude_hospitais.dropna(axis=1, how='all')
        df_saude_hospitais = df_saude_hospitais[df_saude_hospitais['02. Nome Região (Portugal)'] != 'Portugal']

        last_year = df_saude_hospitais['01. Ano'].max()
        df_saude_hospitais = df_saude_hospitais[df_saude_hospitais['01. Ano'] == last_year]

        df_saude_hospitais.drop(columns=['08. Símbolo'], inplace=True)

        df_saude_hospitais.rename(columns={
                '01. Ano': 'Ano',
                '02. Nome Região (Portugal)': 'Região',
                '03. Âmbito Geográfico': 'Âmbito Geográfico',
                '04. Filtro 1': 'Tipo hospital',
                '09. Valor': 'Valor'
            }, inplace=True)

        df_saude_hospitais = df_saude_hospitais.astype({
                'Ano': 'int',
                'Região': 'str',
                'Âmbito Geográfico': 'str',
                'Tipo hospital': 'str',
                'Valor': 'float'
            })

        df_saude_hospitais = df_saude_hospitais.pivot_table(
                    index=['Ano', 'Região'],
                    columns='Tipo hospital',
                    values='Valor',
                    aggfunc='sum',
                    fill_value=0
            )

        df_saude_hospitais.reset_index(inplace=True)

        return df_saude_hospitais
    @staticmethod
    def trata_farmacias(df_saude_farmacias):

        df_saude_farmacias = df_saude_farmacias.dropna(axis=1, how='all')
        df_saude_farmacias = df_saude_farmacias[df_saude_farmacias['02. Nome Região (Portugal)'] != 'Portugal']

        if df_saude_farmacias['08. Símbolo'].nunique() == 1:
            df_saude_farmacias = df_saude_farmacias.drop(columns=['08. Símbolo'])

        last_year = df_saude_farmacias['01. Ano'].max()
        df_saude_farmacias = df_saude_farmacias[df_saude_farmacias['01. Ano'] == last_year]

        df_saude_farmacias.rename(columns={
                '01. Ano': 'Ano',
                '02. Nome Região (Portugal)': 'Região',
                '03. Âmbito Geográfico': 'Âmbito Geográfico',
                '09. Valor': 'Valor'
            }, inplace=True)

        df_saude_farmacias = df_saude_farmacias.astype({
                'Ano': 'int',
                'Região': 'str',
                'Âmbito Geográfico': 'str',
                'Valor': 'float'
            })
        
        return df_saude_farmacias
    @staticmethod
    def trata_ensino_n_superior(df_edu_ensino_n_superior, last_year=None):
        df_edu_ensino_n_superior = df_edu_ensino_n_superior.dropna(axis=1, how='all')
        df_edu_ensino_n_superior = df_edu_ensino_n_superior[df_edu_ensino_n_superior['02. Nome Região (Portugal)'] != 'Portugal']

        df_edu_ensino_n_superior = df_edu_ensino_n_superior.drop(columns=['08. Símbolo'])
        if last_year == None:
            last_year = df_edu_ensino_n_superior['01. Ano'].max()
        df_edu_ensino_n_superior = df_edu_ensino_n_superior[df_edu_ensino_n_superior['01. Ano'] == last_year]

        df_edu_ensino_n_superior.rename(columns={
                '01. Ano': 'Ano',
                '02. Nome Região (Portugal)': 'Região',
                '03. Âmbito Geográfico': 'Âmbito Geográfico',
                '04. Filtro 1': 'Nível de Ensino',
                '05. Filtro 2': 'Tipo de Escola',
                '09. Valor': 'Valor'
            }, inplace=True)

        df_edu_ensino_n_superior = df_edu_ensino_n_superior.astype({
                'Ano': 'int',
                'Região': 'str',
                'Âmbito Geográfico': 'str',
                'Nível de Ensino': 'str',
                'Tipo de Escola': 'str',
                'Valor': 'float'
            })
        df_edu_nivel = df_edu_ensino_n_superior[df_edu_ensino_n_superior['Tipo de Escola']=='Total']
        df_edu_escola_nivel = df_edu_nivel.pivot_table(
                index=['Ano', 'Região'],
                columns='Nível de Ensino',
                values='Valor',
                aggfunc='sum',
                fill_value=0
            )

        df_edu_escola_nivel.reset_index(inplace=True)

        df_edu_tipo = df_edu_ensino_n_superior[df_edu_ensino_n_superior['Nível de Ensino']=='Total']

        df_edu_escola_tipo= df_edu_ensino_n_superior.pivot_table(
                index=['Ano', 'Região'],
                columns='Tipo de Escola',
                values='Valor',
                aggfunc='sum',
                fill_value=0
            )

        df_edu_escola_tipo.reset_index(inplace=True)

        df_edu_escola_final = pd.merge(df_edu_escola_tipo, df_edu_escola_nivel, on=['Ano', 'Região'], how='left')
        df_edu_escola_final.reset_index(inplace=True)

        df_edu_escola_final.drop(columns=['index','Total_x'], inplace=True)
        df_edu_escola_final.rename(columns={'Total_y': 'Total Estabelecimentos'}, inplace=True)

        return df_edu_escola_final
    @staticmethod
    def trata_ensino_superior(df_edu_ensino_superior):
        
        df_edu_ensino_superior = df_edu_ensino_superior.dropna(axis=1, how='all')
        df_edu_ensino_superior = df_edu_ensino_superior[df_edu_ensino_superior['02. Nome Região (Portugal)'] != 'Portugal']

        df_edu_ensino_superior = df_edu_ensino_superior.drop(columns=['08. Símbolo'])

        last_year = df_edu_ensino_superior['01. Ano'].max()
        df_edu_ensino_superior = df_edu_ensino_superior[df_edu_ensino_superior['01. Ano'] == last_year]

        df_edu_ensino_superior.rename(columns={
                '01. Ano': 'Ano',
                '02. Nome Região (Portugal)': 'Região',
                '03. Âmbito Geográfico': 'Âmbito Geográfico',
                '04. Filtro 1': 'Tipo de Escola',
                '09. Valor': 'Valor'
            }, inplace=True)

        df_edu_ensino_superior = df_edu_ensino_superior.astype({
                'Ano': 'int',
                'Região': 'str',
                'Âmbito Geográfico': 'str',
                'Tipo de Escola': 'str',
                'Valor': 'float'
            })

        df_edu_ensino_superior = df_edu_ensino_superior.pivot_table(
                index=['Ano', 'Região'],
                columns='Tipo de Escola',
                values='Valor',
                aggfunc='sum',
                fill_value=0
            )

        df_edu_ensino_superior.reset_index(inplace=True)

        return df_edu_ensino_superior
    @staticmethod
    def trata_econ_depositos(df_econ_depositos):
        df_econ_depositos = df_econ_depositos.dropna(axis=1, how='all')

        df_econ_depositos = df_econ_depositos[df_econ_depositos['02. Nome Região (Portugal)'] != 'Portugal']

        df_econ_depositos = df_econ_depositos.drop(columns=['08. Símbolo'])

        last_year = df_econ_depositos['01. Ano'].max()
        df_econ_depositos = df_econ_depositos[df_econ_depositos['01. Ano'] == last_year]

        df_econ_depositos.rename(columns={
                    '01. Ano': 'Ano',
                    '02. Nome Região (Portugal)': 'Região',
                    '03. Âmbito Geográfico': 'Âmbito Geográfico',
                    '04. Filtro 1': 'Deposito',
                    '09. Valor': 'Valor'
                }, inplace=True)

        df_econ_depositos = df_econ_depositos.astype({
                    'Ano': 'int',
                    'Região': 'str',
                    'Âmbito Geográfico': 'str',
                    'Deposito': 'str',
                    'Valor': 'float'
                })

        df_econ_depositos = df_econ_depositos.pivot_table(
                    index=['Ano', 'Região'],
                    columns='Deposito',
                    values='Valor',
                    aggfunc='sum',
                    fill_value=0
                )

        df_econ_depositos.reset_index(inplace=True)

        return df_econ_depositos
    @staticmethod
    def trata_econ_bancos(df_econ_estabelecimentos_bancos):
        
        df_econ_estabelecimentos_bancos = df_econ_estabelecimentos_bancos.dropna(axis=1, how='all')

        df_econ_estabelecimentos_bancos = df_econ_estabelecimentos_bancos[df_econ_estabelecimentos_bancos['02. Nome Região (Portugal)'] != 'Portugal']

        last_year = df_econ_estabelecimentos_bancos['01. Ano'].max()
        df_econ_estabelecimentos_bancos = df_econ_estabelecimentos_bancos[df_econ_estabelecimentos_bancos['01. Ano'] == last_year]

        df_econ_estabelecimentos_bancos.rename(columns={
                        '01. Ano': 'Ano',
                        '02. Nome Região (Portugal)': 'Região',
                        '03. Âmbito Geográfico': 'Âmbito Geográfico',
                        '09. Valor': 'Valor'
                    }, inplace=True)

        df_econ_estabelecimentos_bancos = df_econ_estabelecimentos_bancos.astype({
                        'Ano': 'int',
                        'Região': 'str',
                        'Âmbito Geográfico': 'str',
                        'Valor': 'float'
                    })

        return df_econ_estabelecimentos_bancos
    @staticmethod
    def trata_df_desemprego(df_desemprego):
        df_desemprego = df_desemprego.dropna(axis=1, how='all')
        df_desemprego = df_desemprego[df_desemprego['02. Nome Região (Portugal)'] != 'Portugal']
        df_desemprego = df_desemprego.drop(columns=['08. Símbolo'])

        last_year = df_desemprego['01. Ano'].max()
        df_desemprego = df_desemprego[df_desemprego['01. Ano'] == last_year]

        df_desemprego.rename(columns={
                    '01. Ano': 'Ano',
                    '02. Nome Região (Portugal)': 'Região',
                    '03. Âmbito Geográfico': 'Âmbito Geográfico',
                    '04. Filtro 1': 'Faixa Etária',
                    '09. Valor': 'Valor'
                }, inplace=True)

        df_desemprego = df_desemprego.astype({
                    'Ano': 'int',
                    'Região': 'str',
                    'Âmbito Geográfico': 'str',
                    'Faixa Etária': 'str',
                    'Valor': 'float'
                })
        df_desemprego = df_desemprego.reset_index(drop=True)

        df_desemprego = df_desemprego.pivot_table(
                        index=['Ano', 'Região'],
                        columns='Faixa Etária',
                        values='Valor',
                        aggfunc='sum',
                        fill_value=0
                    )

        df_desemprego.reset_index(inplace=True)

        return df_desemprego
    @staticmethod
    def trata_populacao_empregada_escolaridade(df_polacao_empregada):
        df_polacao_empregada = df_polacao_empregada.dropna(axis=1, how='all')
        df_polacao_empregada = df_polacao_empregada[df_polacao_empregada['02. Nome Região (Portugal)'] != 'Portugal']
        df_polacao_empregada = df_polacao_empregada.drop(columns=['08. Símbolo'])

        last_year = df_polacao_empregada['01. Ano'].max()
        df_polacao_empregada = df_polacao_empregada[df_polacao_empregada['01. Ano'] == last_year]

        df_polacao_empregada.rename(columns={
                        '01. Ano': 'Ano',
                        '02. Nome Região (Portugal)': 'Região',
                        '03. Âmbito Geográfico': 'Âmbito Geográfico',
                        '04. Filtro 1': 'Escolaridade',
                        '09. Valor': 'Valor'
                    }, inplace=True)

        df_polacao_empregada = df_polacao_empregada.astype({
                        'Ano': 'int',
                        'Região': 'str',
                        'Âmbito Geográfico': 'str',
                        'Escolaridade': 'str',
                        'Valor': 'float'
                    })

        df_polacao_empregada = df_polacao_empregada.pivot_table(
                            index=['Ano', 'Região'],
                            columns='Escolaridade',
                            values='Valor',
                            aggfunc='sum',
                            fill_value=0
                        )

        df_polacao_empregada.reset_index(inplace=True)

        return df_polacao_empregada
    @staticmethod
    def trata_econ_bancos(df_econ_estabelecimentos_bancos):
        df_econ_estabelecimentos_bancos = df_econ_estabelecimentos_bancos.dropna(axis=1, how='all')

        df_econ_estabelecimentos_bancos = df_econ_estabelecimentos_bancos[df_econ_estabelecimentos_bancos['02. Nome Região (Portugal)'] != 'Portugal']

        last_year = df_econ_estabelecimentos_bancos['01. Ano'].max()
        df_econ_estabelecimentos_bancos = df_econ_estabelecimentos_bancos[df_econ_estabelecimentos_bancos['01. Ano'] == last_year]

        df_econ_estabelecimentos_bancos.rename(columns={
                        '01. Ano': 'Ano',
                        '02. Nome Região (Portugal)': 'Região',
                        '03. Âmbito Geográfico': 'Âmbito Geográfico',
                        '09. Valor': 'Valor'
                    }, inplace=True)

        df_econ_estabelecimentos_bancos = df_econ_estabelecimentos_bancos.astype({
                        'Ano': 'int',
                        'Região': 'str',
                        'Âmbito Geográfico': 'str',
                        'Valor': 'float'
                    })

        return df_econ_estabelecimentos_bancos
    @staticmethod
    def trata_populacao_estrangeira(df_populacao_estrangeira):
        df_populacao_estrangeira = df_populacao_estrangeira.dropna(axis=1, how='all')
        df_populacao_estrangeira = df_populacao_estrangeira[df_populacao_estrangeira['02. Nome Região (Portugal)'] != 'Portugal']

        last_year = df_populacao_estrangeira['01. Ano'].max()
        df_populacao_estrangeira = df_populacao_estrangeira[df_populacao_estrangeira['01. Ano'] == last_year]

        df_populacao_estrangeira = df_populacao_estrangeira.drop(columns=['08. Símbolo'])

        df_populacao_estrangeira.rename(columns={
                                '01. Ano': 'Ano',
                                '02. Nome Região (Portugal)': 'Região',
                                '03. Âmbito Geográfico': 'Âmbito Geográfico',
                                '04. Filtro 1': 'Nacionalidade',
                                '09. Valor': 'Valor'
                            }, inplace=True)

        df_populacao_estrangeira = df_populacao_estrangeira.astype({
                                'Ano': 'int',
                                'Região': 'str',
                                'Âmbito Geográfico': 'str',
                                'Nacionalidade': 'str',
                                'Valor': 'float'
                            })

        df_populacao_estrangeira = df_populacao_estrangeira.pivot_table(
                                    index=['Ano', 'Região'],
                                    columns='Nacionalidade',
                                    values='Valor',
                                    aggfunc='sum',
                                    fill_value=0
                                )
        df_populacao_estrangeira.reset_index(inplace=True)

        return df_populacao_estrangeira
    @staticmethod
    def trata_populacao_empregada_ramo(df_polacao_empregada_ramo):
        df_polacao_empregada_ramo = df_polacao_empregada_ramo.dropna(axis=1, how='all')
        df_polacao_empregada_ramo = df_polacao_empregada_ramo[df_polacao_empregada_ramo['02. Nome Região (Portugal)'] != 'Portugal']

        last_year = df_polacao_empregada_ramo['01. Ano'].max()
        df_polacao_empregada_ramo = df_polacao_empregada_ramo[df_polacao_empregada_ramo['01. Ano'] == last_year]

        df_polacao_empregada_ramo = df_polacao_empregada_ramo.drop(columns=['08. Símbolo'])

        df_polacao_empregada_ramo.rename(columns={
                            '01. Ano': 'Ano',
                            '02. Nome Região (Portugal)': 'Região',
                            '03. Âmbito Geográfico': 'Âmbito Geográfico',
                            '04. Filtro 1': 'Ramo de Atividade',
                            '09. Valor': 'Valor'
                        }, inplace=True)

        df_polacao_empregada_ramo = df_polacao_empregada_ramo.astype({
                            'Ano': 'int',
                            'Região': 'str',
                            'Âmbito Geográfico': 'str',
                            'Ramo de Atividade': 'str',
                            'Valor': 'float'
                        })

        df_polacao_empregada_ramo = df_polacao_empregada_ramo.pivot_table(
                                index=['Ano', 'Região'],
                                columns='Ramo de Atividade',
                                values='Valor',
                                aggfunc='sum',
                                fill_value=0
                            )
        df_polacao_empregada_ramo.reset_index(inplace=True)

        return df_polacao_empregada_ramo
    @staticmethod
    def trata_Renda_estrangeira(df_Renda_estrangeira):
        df_Renda_estrangeira = df_populacao_estrangeira.dropna(axis=1, how='all')
        df_populacao_estrangeira = df_populacao_estrangeira[df_populacao_estrangeira['02. Nome Região (Portugal)'] != 'Portugal']

        last_year = df_populacao_estrangeira['01. Ano'].max()
        df_populacao_estrangeira = df_populacao_estrangeira[df_populacao_estrangeira['01. Ano'] == last_year]

        df_populacao_estrangeira = df_populacao_estrangeira.drop(columns=['08. Símbolo'])

        df_populacao_estrangeira.rename(columns={
                                '01. Ano': 'Ano',
                                '02. Nome Região (Portugal)': 'Região',
                                '03. Âmbito Geográfico': 'Âmbito Geográfico',
                                '04. Filtro 1': 'Nacionalidade',
                                '09. Valor': 'Valor'
                            }, inplace=True)

        df_populacao_estrangeira = df_populacao_estrangeira.astype({
                                'Ano': 'int',
                                'Região': 'str',
                                'Âmbito Geográfico': 'str',
                                'Nacionalidade': 'str',
                                'Valor': 'float'
                            })

        df_populacao_estrangeira = df_populacao_estrangeira.pivot_table(
                                    index=['Ano', 'Região'],
                                    columns='Nacionalidade',
                                    values='Valor',
                                    aggfunc='sum',
                                    fill_value=0
                                )
        df_populacao_estrangeira.reset_index(inplace=True)

        return df_populacao_estrangeira
    @staticmethod
    def trata_moradia_edificios(df_moradia_edificios):
        df_moradia_edificios = df_moradia_edificios.dropna(axis=1, how='all')
        df_moradia_edificios = df_moradia_edificios[df_moradia_edificios['02. Nome Região (Portugal)'] != 'Portugal']

        last_year = df_moradia_edificios['01. Ano'].max()
        df_moradia_edificios = df_moradia_edificios[df_moradia_edificios['01. Ano'] == last_year]

        df_moradia_edificios.rename(columns={
                                    '01. Ano': 'Ano',
                                    '02. Nome Região (Portugal)': 'Região',
                                    '03. Âmbito Geográfico': 'Âmbito Geográfico',
                                    '04. Filtro 1': 'Tipo de Obra',
                                    '05. Filtro 2': 'Fim que se destina',
                                    '09. Valor': 'Valor'
                                }, inplace=True)

        df_moradia_edificios = df_moradia_edificios.astype({
                                    'Ano': 'int',
                                    'Região': 'str',
                                    'Âmbito Geográfico': 'str',
                                    'Tipo de Obra': 'str',
                                    'Fim que se destina': 'str',
                                    'Valor': 'float'
                                })

        df_moradia_tipo = df_moradia_edificios[df_moradia_edificios['Fim que se destina'] == 'Total']

        df_moradia_edificios_tipo = df_moradia_tipo.pivot_table(
                                        index=['Ano', 'Região'],
                                        columns='Tipo de Obra',
                                        values='Valor',
                                        aggfunc='sum',
                                        fill_value=0
                                    )
        df_moradia_edificios_tipo.reset_index(inplace=True)

        df_moradia_destino = df_moradia_edificios[df_moradia_edificios['Tipo de Obra']=='Total']
        df_moradia_edificios_destino = df_moradia_destino.pivot_table(
                                        index=['Ano', 'Região'],
                                        columns='Fim que se destina',
                                        values='Valor',
                                        aggfunc='sum',
                                        fill_value=0
                                    )
        df_moradia_edificios_destino.reset_index(inplace=True)


        df_moradia_edificios_final = pd.merge(df_moradia_edificios_destino, df_moradia_edificios_tipo, on=['Ano', 'Região'], how='left')
        df_moradia_edificios_final.reset_index(inplace=True)

        df_moradia_edificios_final.drop(columns=['index','Total_x'], inplace=True)

        return df_moradia_edificios_final
    @staticmethod
    def trata_construcoes_novas(df_construcoes_novas):
        df_construcoes_novas = df_construcoes_novas.dropna(axis=1, how='all')
        df_construcoes_novas = df_construcoes_novas[df_construcoes_novas['02. Nome Região (Portugal)'] != 'Portugal']

        last_year = df_construcoes_novas['01. Ano'].max()
        df_construcoes_novas = df_construcoes_novas[df_construcoes_novas['01. Ano'] == last_year]

        df_construcoes_novas.rename(columns={
                                        '01. Ano': 'Ano',
                                        '02. Nome Região (Portugal)': 'Região',
                                        '03. Âmbito Geográfico': 'Âmbito Geográfico',
                                        '04. Filtro 1': 'Qtd. Quartos',
                                        '09. Valor': 'Valor'
                                    }, inplace=True)

        df_construcoes_novas = df_construcoes_novas.astype({
                                        'Ano': 'int',
                                        'Região': 'str',
                                        'Âmbito Geográfico': 'str',
                                        'Qtd. Quartos': 'str',
                                        'Valor': 'float'
                                    })

        df_construcoes_novas = df_construcoes_novas.pivot_table(
                                    index=['Ano', 'Região'],
                                    columns='Qtd. Quartos',
                                    values='Valor',
                                    aggfunc='sum',
                                    fill_value=0
                                )
        df_construcoes_novas.reset_index(inplace=True)

        return df_construcoes_novas
    @staticmethod
    def trata_sal_ganho_medio_mensal(df_sal_ganho_medio_mensal):
        df_sal_ganho_medio_mensal = df_sal_ganho_medio_mensal.dropna(axis=1, how='all')
        df_sal_ganho_medio_mensal = df_sal_ganho_medio_mensal[df_sal_ganho_medio_mensal['02. Nome Região (Portugal)'] != 'Portugal']

        last_year = df_sal_ganho_medio_mensal['01. Ano'].max()
        df_sal_ganho_medio_mensal = df_sal_ganho_medio_mensal[df_sal_ganho_medio_mensal['01. Ano'] == last_year]

        df_sal_ganho_medio_mensal.drop(columns=['08. Símbolo'], inplace=True)

        df_sal_ganho_medio_mensal.rename(columns={
            '01. Ano': 'Ano',
            '02. Nome Região (Portugal)': 'Região',
            '03. Âmbito Geográfico': 'Âmbito Geográfico',
            '09. Valor': 'Valor'
        }, inplace=True)

        df_sal_ganho_medio_mensal = df_sal_ganho_medio_mensal.astype({
            'Ano': 'int',
            'Região': 'str',
            'Âmbito Geográfico': 'str',
            'Valor': 'float'
        })

        return df_sal_ganho_medio_mensal
    @staticmethod
    def trata_sal_ganho_medio_mensal_escol(df_sal_ganho_medio_mensal_escol):
        df_sal_ganho_medio_mensal_escol = df_sal_ganho_medio_mensal_escol.dropna(axis=1, how='all')
        df_sal_ganho_medio_mensal_escol = df_sal_ganho_medio_mensal_escol[df_sal_ganho_medio_mensal_escol['02. Nome Região (Portugal)'] != 'Portugal']

        last_year = df_sal_ganho_medio_mensal_escol['01. Ano'].max()
        df_sal_ganho_medio_mensal_escol = df_sal_ganho_medio_mensal_escol[df_sal_ganho_medio_mensal_escol['01. Ano'] == last_year]

        df_sal_ganho_medio_mensal_escol.drop(columns=['08. Símbolo'], inplace=True)

        df_sal_ganho_medio_mensal_escol.rename(columns={
            '01. Ano': 'Ano',
            '02. Nome Região (Portugal)': 'Região',
            '03. Âmbito Geográfico': 'Âmbito Geográfico',
            '04. Filtro 1': 'Nível de Escolaridade',
            '09. Valor': 'Valor'
        }, inplace=True)

        df_sal_ganho_medio_mensal_escol = df_sal_ganho_medio_mensal_escol.astype({
            'Ano': 'int',
            'Região': 'str',
            'Âmbito Geográfico': 'str',
            'Nível de Escolaridade': 'str',
            'Valor': 'float'
        })

        df_sal_ganho_medio_mensal_escol = df_sal_ganho_medio_mensal_escol.pivot_table(
                                        index=['Ano', 'Região'],
                                        columns='Nível de Escolaridade',
                                        values='Valor',
                                        aggfunc='sum',
                                        fill_value=0
                                    )
        df_sal_ganho_medio_mensal_escol.reset_index(inplace=True)

        return df_sal_ganho_medio_mensal_escol
    @staticmethod
    def trata_econ_poder_compra():

        file_path = '..\\..\\Bases\\PorData\\economia\\Municipios_Proporcao_de_poder_de_compra.xlsx'

        excel_data = pd.ExcelFile(file_path)

        # Carregar a planilha 'Quadro'
        df = pd.read_excel(excel_data, sheet_name='Quadro')

        # Limpar e extrair as colunas e dados relevantes
        df_econ_poder_compra = df.iloc[10:, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
        df_econ_poder_compra.columns = ['Âmbito Geográfico', 'Anos', '1993', '2000', '2002', '2007', '2009', '2011', '2013', '2015', '2017', '2019', '2021']

        # Remover linhas completamente vazias
        df_econ_poder_compra = df_econ_poder_compra.dropna(how='all')

        # Resetar o índice
        df_econ_poder_compra.reset_index(drop=True, inplace=True)

        df_econ_poder_compra = df_econ_poder_compra.drop(0, axis=0).reset_index(drop=True)

        df_econ_poder_compra = df_econ_poder_compra[df_econ_poder_compra['Âmbito Geográfico'] == 'Município']

        df_econ_poder_compra = df_econ_poder_compra[['Âmbito Geográfico', 'Anos', df_econ_poder_compra.columns[-1]]]

        ultima_coluna = df_econ_poder_compra.columns[-1]
        df_econ_poder_compra.rename(columns={
            'Âmbito Geográfico': 'Âmbito Geográfico',
            'Anos': 'Região',
            ultima_coluna: 'Valor'
        }, inplace=True)

        df_econ_poder_compra = df_econ_poder_compra.astype({
            'Âmbito Geográfico': 'str',
            'Região': 'str',
            'Valor': 'float'
        })

        return df_econ_poder_compra
    @staticmethod
    def formatar_numericos(df):
            df = df.copy()  # Cria uma cópia, o original não será alterado!
            for col in df.select_dtypes(include='number').columns:
                df[col] = df[col].apply(lambda x: '{:,.2f}'.format(x).replace(',', 'X').replace('.', ',').replace('X', '.'))
            return df
    @staticmethod
    def salva_base_agregada(df_final):

            df_final.columns = [unidecode(col) for col in df_final.columns]
            df_formatado = formatar_numericos(df_final)
            df_formatado.to_csv(filePathBaseAgregada, index=False)
            del(df_formatado)
    @staticmethod
    def trata_seg_taxa_crime():
        file_path = '..\\..\\Bases\\InePt\\seguranca\\INE_PT-Taxa-de-criminalidade.xls'

        excel_data = pd.ExcelFile(file_path)

        # Carregar a planilha 'Quadro'
        df_seg_tax_crime = pd.read_excel(excel_data, sheet_name='Quadro')

        df_seg_tax_crime= df_seg_tax_crime.iloc[10:, [0, 1, 2, 4, 6, 8, 10, 12, 14]]
        df_seg_tax_crime.columns = [
            'Regiao', 
            'Codigo', 
            'Total', 
            'Crimes contra a integridade física', 
            'Furto/roubo por esticão e na via pública', 
            'Furto de veículo e em veículo motorizado', 
            'Condução de veículo com taxa de álcool igual ou superior a 1,2g/l',
            'Condução sem habilitação legal', 
            'Crimes contra o património'
            ]

        # Remover linhas completamente vazias
        df_seg_tax_crime = df_seg_tax_crime.dropna(how='all')

        df_seg_tax_crime['Codigo'] = df_seg_tax_crime['Codigo'].astype(str)
        df_seg_tax_crime = df_seg_tax_crime[df_seg_tax_crime['Codigo'].str.len() == 7]

        df_seg_tax_crime = df_seg_tax_crime.astype({
            'Regiao':'str',
            'Codigo':'str',
            'Total': 'float',
            'Crimes contra a integridade física': 'float',
            'Furto/roubo por esticão e na via pública': 'float',
            'Furto de veículo e em veículo motorizado': 'float',
            'Condução de veículo com taxa de álcool igual ou superior a 1,2g/l': 'float',
            'Condução sem habilitação legal': 'float',
            'Crimes contra o património': 'float'
        })

        return df_seg_tax_crime
    @staticmethod
    def trata_mora_valor_renda():
            file_path = '..\\..\\Bases\\InePt\\moradia\\INE_PT-Valor-mediano-das-rendas.xls'

            excel_data = pd.ExcelFile(file_path)

            # Carregar a planilha 'Quadro'
            df_mora_valor_rendas = pd.read_excel(excel_data, sheet_name='Quadro')

            df_mora_valor_rendas= df_mora_valor_rendas.iloc[9:, [0, 1, 2, 3]]
            df_mora_valor_rendas.columns = [
                    'Regiao', 
                    'Codigo', 
                    'Valor mediano das rendas por m2',
                    'lixo'
            ]

            # Remover linhas completamente vazias
            df_mora_valor_rendas = df_mora_valor_rendas.dropna(how='all')

            df_mora_valor_rendas.drop(columns=['lixo'], inplace=True)

            df_mora_valor_rendas['Codigo'] = df_mora_valor_rendas['Codigo'].astype(str)
            # df_1= df_mora_valor_rendas[df_mora_valor_rendas['Codigo'].str.match(r'^\d{2}$')]
            # df_3= df_mora_valor_rendas[df_mora_valor_rendas['Codigo'].str.match(r'^\d{3}$')]

            df_mora_valor_rendas = df_mora_valor_rendas[df_mora_valor_rendas['Codigo'].str.len() == 7]

            # Criar nova coluna com valor estimado para apartamento de 100m²
            df_mora_valor_rendas['Valor apartamento 100m²'] = df_mora_valor_rendas['Valor mediano das rendas por m2'] * 100

            # Criar nova coluna com valor estimado para apartamento de 80m²
            df_mora_valor_rendas['Valor apartamento 80m²'] = df_mora_valor_rendas['Valor mediano das rendas por m2'] * 80


            return df_mora_valor_rendas

if __name__ == "__main__":
    porDataProcess = PorDataProcess()

    file_path = 'pordataFiles.json'
    arquivos = porDataProcess.read_json_file(file_path)

    dataframes = porDataProcess.read_csv_files_from_folders(porDataProcess.folderPathPorData)

    # cultura ------------------------------------------------------------------
    df_cultura_cinemas = porDataProcess.find_df_by_name(dataframes, '623-recintos-de-cinema')
    df_cultura_cinemas = porDataProcess.trata_df_cultura(df_cultura_cinemas)
    porDataProcess.adiciona_df_tratado("Cultura","recintos-de-cinema" , df_cultura_cinemas)
 
    df_cultura_espetaculos = porDataProcess.find_df_by_name(dataframes, '631-sessoes-de-espetaculos-ao-vivo')
    df_cultura_espetaculos = porDataProcess.trata_df_cultura(df_cultura_espetaculos)
    porDataProcess.adiciona_df_tratado("Cultura","sessoes-de-espetaculos-ao-vivo" , df_cultura_espetaculos)

    df_populaccao_residente = porDataProcess.find_df_by_name(dataframes, '1-populacao-residente-por-sexo-e-grupo-etario')
    df_populaccao_residente = porDataProcess.trata_df_populacao_residente(df_populaccao_residente)
    porDataProcess.adiciona_df_tratado("Populacso","populacao-residente-por-sexo-e-grupo-etario" , df_populaccao_residente)


    df_densidade_populacional = porDataProcess.find_df_by_name(dataframes, '4-densidade-populacional')
    df_densidade_populacional = porDataProcess.trata_df_populacao_densidade(df_densidade_populacional)
    porDataProcess.adiciona_df_tratado("Populacao","densidade-populacional" , df_densidade_populacional)


    df_populacao_estrangeira = porDataProcess.find_df_by_name(dataframes, '354-populacao-estrangeira-com-estatuto-legal-de-residente')
    df_populacao_estrangeira = porDataProcess.trata_populacao_estrangeira(df_populacao_estrangeira)
    porDataProcess.adiciona_df_tratado("Populacao","populacao-estrangeira-com-estatuto-legal-de-residente" , df_populacao_estrangeira) 


    df_seguranca_crime_catetoria = porDataProcess.find_df_by_name(dataframes, '560-crimes-por-categoria')
    df_seg_categoria = porDataProcess.trata_crimes_catetoria(df_seguranca_crime_catetoria)
    porDataProcess.adiciona_df_tratado("Segurança","crimes-por-categoria" , df_seg_categoria)


    df_seg_crimes_tipo = porDataProcess.find_df_by_name(dataframes, '559-crimes-registados-pelas-policias-por-tipo-de-crime')
    df_seg_crimes_tipo = porDataProcess.trata_crimes_tipo(df_seg_crimes_tipo)
    porDataProcess.adiciona_df_tratado("Segurança","crimes-registados-pelas-policias-por-tipo-de-crime" , df_seg_crimes_tipo)


    df_taxa_crimes = porDataProcess.trata_seg_taxa_crime()
    porDataProcess.adiciona_df_tratado("Segurança","taxa_crimes", df_taxa_crimes)


    df_saude_hospitais = porDataProcess.find_df_by_name(dataframes, '470-hospitais-por-natureza-institucional')
    df_saude_hospitais = porDataProcess.trata_hospitais(df_saude_hospitais)
    porDataProcess.adiciona_df_tratado("Saude","hospitais-por-natureza-institucional" , df_saude_hospitais)


    df_saude_farmacias = porDataProcess.find_df_by_name(dataframes, '464-farmacias')
    df_saude_farmacias = porDataProcess.trata_farmacias(df_saude_farmacias)
    porDataProcess.adiciona_df_tratado("Saude","farmacias" , df_saude_farmacias)


    df_edu_ensino_n_superior =  porDataProcess.find_df_by_name(dataframes, '379-estabelecimentos-de-ensino-nao-superior-por-0')
    df_edu_escola_final = porDataProcess.trata_ensino_n_superior(df_edu_ensino_n_superior,2023)
    porDataProcess.adiciona_df_tratado("Educacao","estabelecimentos-de-ensino-nao-superior-por-0" , df_edu_escola_final)


    df_edu_ensino_superior =  porDataProcess.find_df_by_name(dataframes, '375-estabelecimentos-de-ensino-superior-por-subsistema')
    df_edu_ensino_superior = porDataProcess.trata_ensino_superior(df_edu_ensino_superior)
    porDataProcess.adiciona_df_tratado("Educacao","estabelecimentos-de-ensino-superior-por-subsistema" , df_edu_ensino_superior) 


    df_econ_depositos = porDataProcess.find_df_by_name(dataframes, '294-depositos-de-clientes-nos-bancos-caixas-economicas-e')
    df_econ_depositos = porDataProcess.trata_econ_depositos(df_econ_depositos)
    porDataProcess.adiciona_df_tratado("Economia","depositos-de-clientes-nos-bancos-caixas-economicas-e" , df_econ_depositos) 


    df_econ_estabelecimentos_bancos = porDataProcess.find_df_by_name(dataframes, '295-estabelecimentos-de-bancos-caixas-economicas-e-caixas')
    df_econ_estabelecimentos_bancos = porDataProcess.trata_econ_bancos(df_econ_estabelecimentos_bancos)
    porDataProcess.adiciona_df_tratado("Economia","estabelecimentos-de-bancos-caixas-economicas-e-caixas" , df_econ_estabelecimentos_bancos) 


    df_desemprego = porDataProcess.find_df_by_name(dataframes, '439-desemprego-registado-nos-centros-de-emprego-por-grup')
    df_desemprego = porDataProcess.trata_df_desemprego(df_desemprego)
    porDataProcess.adiciona_df_tratado("Emprego","desemprego-registado-nos-centros-de-emprego-por-grup" , df_desemprego) 

    df_polacao_empregada = porDataProcess.find_df_by_name(dataframes, '445-populacao-empregada-por-conta-de-outrem-por-nivel-de')
    df_polacao_empregada = porDataProcess.trata_populacao_empregada_escolaridade(df_polacao_empregada)
    porDataProcess.adiciona_df_tratado("Emprego","populacao-empregada-por-conta-de-outrem-por-nivel-de" , df_polacao_empregada) 

    df_polacao_empregada_ramo = porDataProcess.find_df_by_name(dataframes, '845-Pessoal-ao-servico-nas-empresas-por-ramo-de-atividade')
    df_polacao_empregada_ramo = porDataProcess.trata_populacao_empregada_ramo(df_polacao_empregada_ramo)
    porDataProcess.adiciona_df_tratado("Emprego","Pessoal-ao-servico-nas-empresas-por-ramo-de-atividade" , df_polacao_empregada_ramo) 

    df_moradia_edificios = porDataProcess.find_df_by_name(dataframes, '978-edificios-concluidos-por-tipo-de-obra-e-fim-que-se')
    df_moradia_edificios = porDataProcess.trata_moradia_edificios(df_moradia_edificios)
    porDataProcess.adiciona_df_tratado("Moradia","edificios-concluidos-por-tipo-de-obra-e-fim-que-se" , df_moradia_edificios) 

    df_construcoes_novas = porDataProcess.find_df_by_name(dataframes, '980-fogos-concluidos-em-construcoes-novas-para-habitacao')
    df_construcoes_novas = porDataProcess.trata_construcoes_novas(df_construcoes_novas)
    porDataProcess.adiciona_df_tratado("Moradia","fogos-concluidos-em-construcoes-novas-para-habitacao" , df_construcoes_novas) 

    df_mora_valor_rendas = porDataProcess.trata_mora_valor_renda()
    porDataProcess.adiciona_df_tratado("Moradia","Valor Medio Arrendamento", df_mora_valor_rendas)

    df_sal_ganho_medio_mensal= porDataProcess.find_df_by_name(dataframes, '581-ganho-medio-mensal')
    df_sal_ganho_medio_mensal = porDataProcess.trata_sal_ganho_medio_mensal(df_sal_ganho_medio_mensal)
    porDataProcess.adiciona_df_tratado("Renda","ganho-medio-mensal" , df_sal_ganho_medio_mensal) 

    df_sal_ganho_medio_mensal_escol= porDataProcess.find_df_by_name(dataframes, '582-ganho-medio-mensal-por-nivel-de-escolaridade')
    df_sal_ganho_medio_mensal_escol = porDataProcess.trata_sal_ganho_medio_mensal_escol(df_sal_ganho_medio_mensal_escol)
    porDataProcess.adiciona_df_tratado("Renda","ganho-medio-mensal-por-nivel-de-escolaridade" , df_sal_ganho_medio_mensal_escol) 

    df_econ_poder_compra = porDataProcess.trata_econ_poder_compra()
    porDataProcess.adiciona_df_tratado("Renda","poder-de-compra" , df_econ_poder_compra) 


    # cultura
    dfs = porDataProcess.dfs
    df_cinemas = porDataProcess.find_df_by_name(dfs,'recintos-de-cinema')
    df_sessoes = porDataProcess.find_df_by_name(dfs,'sessoes-de-espetaculos-ao-vivo')

    df_cinemas = df_cinemas.rename(columns={'Valor': 'Qtd.Cinemas'})
    df_sessoes = df_sessoes.rename(columns={'Valor': 'Qtd.Sessões'})

    df_cultura = pd.merge(df_cinemas, df_sessoes, on=['Região'], how='left')
    df_cultura.drop(columns=['Âmbito Geográfico_x','Âmbito Geográfico_y'], inplace=True)

    df_cultura.drop(columns=['Ano_y'], inplace=True)
    df_cultura.rename(columns={'Ano_x':'Ano'}, inplace=True)

    # saude
    df_saude_hospitais = porDataProcess.find_df_by_name(dfs,'hospitais-por-natureza-institucional')
    df_farmacias = porDataProcess.find_df_by_name(dfs,'farmacias')

    df_farmacias = df_farmacias.rename(columns={'Valor': 'Qtd.Farmacias'})
    df_saude_hospitais = df_saude_hospitais.rename(columns={'Parceria público-privada': 'Qtd.Hospitais.Publico.Privado'})
    df_saude_hospitais = df_saude_hospitais.rename(columns={'Privado': 'Qtd.Hospitais.Privado'})
    df_saude_hospitais = df_saude_hospitais.rename(columns={'Público': 'Qtd.Hospitais.Publico'})
    df_saude_hospitais = df_saude_hospitais.rename(columns={'Total': 'Qtd.Hospitais.Total'})

    df_saude = pd.merge(df_saude_hospitais, df_farmacias, on=['Região'], how='left')

    df_saude.drop(columns=['Âmbito Geográfico','Ano_y'], inplace=True)
    df_saude.rename(columns={'Ano_x':'Ano'}, inplace=True)

    # educacao
    df_ensino_n_superior = porDataProcess.find_df_by_name(dfs,'estabelecimentos-de-ensino-nao-superior-por-0')
    df_ensino_superior = porDataProcess.find_df_by_name(dfs,'estabelecimentos-de-ensino-superior-por-subsistema')
    df_ensino_superior = df_ensino_superior.rename(columns={'Privado': 'Qtd.Ensino.Superior.Privado'})
    df_ensino_superior = df_ensino_superior.rename(columns={'Público': 'Qtd.Ensino.Superior.Publico'})
    df_ensino_superior = df_ensino_superior.rename(columns={'Total': 'Qtd.Ensino.Superior.Total'})

    df_ensino_n_superior = df_ensino_n_superior.rename(columns={'Privado': 'Qtd.Ensino.NSuperior.Privado'})
    df_ensino_n_superior = df_ensino_n_superior.rename(columns={'Público': 'Qtd.Ensino.NSuperior.Publico'})
    df_ensino_n_superior = df_ensino_n_superior.rename(columns={'Escola artística': 'Qtd.Ensino.NSuperior.artistica'})
    df_ensino_n_superior = df_ensino_n_superior.rename(columns={'Escola básica': 'Qtd.Ensino.NSuperior.basica'})
    df_ensino_n_superior = df_ensino_n_superior.rename(columns={'Escola básica e secundária': 'Qtd.Ensino.NSuperior.basica.secundaria'})
    df_ensino_n_superior = df_ensino_n_superior.rename(columns={'Escola profissional': 'Qtd.Ensino.NSuperior.profissional'})
    df_ensino_n_superior = df_ensino_n_superior.rename(columns={'Escola secundária': 'Qtd.Ensino.NSuperior.secundaria'})
    df_ensino_n_superior = df_ensino_n_superior.rename(columns={'Jardim-de-infância': 'Qtd.Ensino.NSuperior.Jardim_de_infância'})
    df_ensino_n_superior = df_ensino_n_superior.rename(columns={'Total Estabelecimentos': 'Qtd.Ensino.NSuperior.Total'})

    df_educacao = pd.merge(df_ensino_n_superior, df_ensino_superior, on=['Região'], how='left')

    df_educacao.drop(columns=['Ano_y'], inplace=True)
    df_educacao.rename(columns={'Ano_x':'Ano'}, inplace=True)

    # Populacao
    df_populacao_residente = porDataProcess.find_df_by_name(dfs,'populacao-residente-por-sexo-e-grupo-etario')
    df_populacao_densidade = porDataProcess.find_df_by_name(dfs,'densidade-populacional')
    df_populacao_estrangeira = porDataProcess.find_df_by_name(dfs,'populacao-estrangeira-com-estatuto-legal-de-residente')

    df_populacao_residente = df_populacao_residente.rename(columns={
        'Homens': 'Qtd.Populacao.Homens',
        'Mulheres': 'Qtd.Populacao.Mulheres',
        '0 - 4 anos': 'Qtd.Populacao.0_4_anos',
        '5 - 9 anos': 'Qtd.Populacao.5_9_anos',
        '10 - 14 anos': 'Qtd.Populacao.10_14_anos',
        '15 - 19 anos': 'Qtd.Populacao.15_19_anos',
        '20 - 24 anos': 'Qtd.Populacao.20_24_anos',
        '25 - 29 anos': 'Qtd.Populacao.25_29_anos',
        '30 - 34 anos': 'Qtd.Populacao.30_34_anos',
        '35 - 39 anos': 'Qtd.Populacao.35_39_anos',
        '40 - 44 anos': 'Qtd.Populacao.40_44_anos',
        '45 - 49 anos': 'Qtd.Populacao.45_49_anos',
        '50 - 54 anos': 'Qtd.Populacao.50_54_anos',
        '55 - 59 anos': 'Qtd.Populacao.55_59_anos',
        '60 - 64 anos': 'Qtd.Populacao.60_64_anos',
        '65 - 69 anos': 'Qtd.Populacao.65_69_anos',
        '70 - 74 anos': 'Qtd.Populacao.70_74_anos',
        '75 - 79 anos': 'Qtd.Populacao.75_79_anos',
        '80 - 84 anos': 'Qtd.Populacao.80_84_anos',
        '85 ou mais anos': 'Qtd.Populacao.85_ou_mais_anos',
        'Total População': 'Qtd.Populacao.Total',
        'Total Crianças': 'Qtd.Populacao.Total.Criancas',
        'Total Adolescentes': 'Qtd.Populacao.Total.Adolescentes',
        'Total Jovens Adultos': 'Qtd.Populacao.Total.Jovens_Adultos',
        'Total Adultos': 'Qtd.Populacao.Total.Adultos',
        'Total Idosos': 'Qtd.Populacao.Total.Idosos',
        'Proporção Crianças': 'Qtd.Populacao.Proporcao.Criancas',
        'Proporção Adolescentes': 'Qtd.Populacao.Proporcao.Adolescentes',
        'Proporção Jovens Adultos': 'Qtd.Populacao.Proporcao.Jovens_Adultos',
        'Proporção Adultos': 'Qtd.Populacao.Proporcao.Adultos',
        'Proporção Idosos': 'Qtd.Populacao.Proporcao.Idosos'
    })

    df_populacao_densidade = df_populacao_densidade.rename(columns={
        'Valor': 'Qtd.Populacao.Densidade.hab.km'
    })

    df_populacao_densidade = df_populacao_densidade.drop(columns=['Âmbito Geográfico'])

    df_populacao_estrangeira = df_populacao_estrangeira.rename(columns={
        '1. África': 'Qtd.Populacao.Estrangeira.Africa',
        '1.1. Angola': 'Qtd.Populacao.Estrangeira.Angola',
        '1.2.Cabo-Verde': 'Qtd.Populacao.Estrangeira.Cabo_Verde',
        '1.3. Guiné-Bissau': 'Qtd.Populacao.Estrangeira.Guine_Bissau',
        '1.4. Moçambique': 'Qtd.Populacao.Estrangeira.Mocambique',
        '1.5. São Tomé e Príncipe': 'Qtd.Populacao.Estrangeira.Sao_Tome_e_Principe',
        '2. América': 'Qtd.Populacao.Estrangeira.America',
        '2.1. Brasil': 'Qtd.Populacao.Estrangeira.Brasil',
        '3. Ásia': 'Qtd.Populacao.Estrangeira.Asia',
        '3.1. China': 'Qtd.Populacao.Estrangeira.China',
        '3.2. Índia': 'Qtd.Populacao.Estrangeira.India',
        '3.3. Nepal': 'Qtd.Populacao.Estrangeira.Nepal',
        '4. Europa': 'Qtd.Populacao.Estrangeira.Europa',
        '4.1. Espanha': 'Qtd.Populacao.Estrangeira.Espanha',
        '4.2. França': 'Qtd.Populacao.Estrangeira.Franca',
        '4.3. Itália': 'Qtd.Populacao.Estrangeira.Italia',
        '4.4. República da Moldova': 'Qtd.Populacao.Estrangeira.Republica_da_Moldova',
        '4.5. Reino Unido': 'Qtd.Populacao.Estrangeira.Reino_Unido',
        '4.6. Roménia': 'Qtd.Populacao.Estrangeira.Romenia',
        '4.7. Ucrânia': 'Qtd.Populacao.Estrangeira.Ucrania',
        'Total': 'Qtd.Populacao.Estrangeira.Total'
    })


    df_populacao_ = pd.merge(df_populacao_residente, df_populacao_densidade, on=['Região'], how='left')
    df_populacao = pd.merge(df_populacao_, df_populacao_estrangeira, on=['Região'], how='left')

    df_populacao.drop(columns=['Ano_y'], inplace=True)
    df_populacao.rename(columns={'Ano_x':'Ano'}, inplace=True)

    # Segurança

    df_seg_crimes_cat = porDataProcess.find_df_by_name(dfs,'crimes-por-categoria')
    df_seg_crimes_tipo = porDataProcess.find_df_by_name(dfs,'crimes-registados-pelas-policias-por-tipo-de-crime')
    df_seg_crimes_taxa = porDataProcess.find_df_by_name(dfs,'taxa_crimes')

    df_seg_crimes_cat = df_seg_crimes_cat.rename(columns={
        '1. Contra as pessoas': 'Qtd.Crimes.Contra.Pessoas',
        '2. Contra o património': 'Qtd.Crimes.Contra.Patrimonio',
        '3. Contra a vida em sociedade': 'Qtd.Crimes.Contra.Vida.Em.Sociedade',
        '4. Contra o Estado': 'Qtd.Crimes.Contra.Estado',
        '5. Contra a identidade cultural, integridade pessoal': 'Qtd.Crimes.Contra.IdentidadeCultural.IntegridadePessoal',
        '6. Contra animais companhia': 'Qtd.Crimes.Contra.Animais.De.Companhia',
        '7. Legislação Avulsa e Outros': 'Qtd.Crimes.Outros.Leituras.Legislacao.Avulsa',
        'Total': 'Qtd.Crimes.Total'
    })

    df_seg_crimes_tipo = df_seg_crimes_tipo.rename(columns={
        'Furto em edifício comercial ou industrial': 'Qtd.Crime.Furto.ComercialOuIndustrial',
        'Furto em residência': 'Qtd.Crime.Furto.Residencia',
        'Furto em veículo motorizado': 'Qtd.Crime.Furto.VeiculoMotorizado',
        'Total': 'Qtd.Crime.Total',
        'Violência doméstica contra cônjuge ou análogos': 'Qtd.Crime.Violencia.Domestica.Contra.ConjugeOuAnalogos'
    })

    df_seg_crimes_taxa = df_seg_crimes_taxa.rename(columns={
        'Regiao': 'Região',
        'Total': 'Qtd.Taxa.Crime.Total',
        'Crimes contra a integridade física': 'Qtd.Taxa.Crime.Contra.Integridade.Fisica',
        'Furto/roubo por esticão e na via pública': 'Qtd.Taxa.Crime.FurtoOuRoubo.ViaPublica',
        'Furto de veículo e em veículo motorizado': 'Qtd.Taxa.Crime.Furto.Veiculo',
        'Condução de veículo com taxa de álcool igual ou superior a 1,2g/l': 'Qtd.Taxa.Crime.Conducao.Alcool.1_2g',
        'Condução sem habilitação legal': 'Qtd.Taxa.Crime.Conducao.Sem.Habilitacao',
        'Crimes contra o património': 'Qtd.Taxa.Crime.Contra.Patrimonio'
    })

    df_seguranca_ = pd.merge(df_seg_crimes_cat, df_seg_crimes_tipo, on=['Região'], how='left')
    df_seguranca = pd.merge(df_seguranca_, df_seg_crimes_taxa, on=['Região'], how='left')
    df_seguranca.drop(columns=['Ano_y'], inplace=True)
    df_seguranca = df_seguranca.rename(columns={
        'Ano_x': 'Ano'
    })


    # Ecomonia
    df_econ_depositos = porDataProcess.find_df_by_name(dfs,'depositos-de-clientes-nos-bancos-caixas-economicas-e')
    df_econ_bancos = porDataProcess.find_df_by_name(dfs,'estabelecimentos-de-bancos-caixas-economicas-e-caixas')

    df_econ_bancos = df_econ_bancos.rename(columns={'Valor': 'Qtd.Estabelecimentos.Bancarios'})
    df_econ_bancos.drop(columns=['Âmbito Geográfico','07. Escala'],inplace=True)

    df_econ_depositos = df_econ_depositos.rename(
        columns={
            'De emigrantes': 'Qtd.Depositos.De.Imigrantes',
            'De outros clientes':'Qtd.Depositos.De.Outros.Clientes',
            'Total': 'Qtd.Depositos.Total'
        }
    )

    df_economia = pd.merge(df_econ_bancos, df_econ_depositos, on=['Região'], how='left')
    df_economia.drop(columns=['Ano_y'],inplace=True)
    df_economia.rename(columns={'Ano_x':'Ano'},inplace=True)

    # Emprego
    df_emp_ramo = porDataProcess.find_df_by_name(dfs,'Pessoal-ao-servico-nas-empresas-por-ramo-de-atividade')
    df_emp_desemprego = porDataProcess.find_df_by_name(dfs,'desemprego-registado-nos-centros-de-emprego-por-grup')
    df_emp_emprego = porDataProcess.find_df_by_name(dfs,'populacao-empregada-por-conta-de-outrem-por-nivel-de')

    df_emp_emprego = df_emp_emprego.rename(columns={
        '01. Inferior ao 1.º ciclo': 'Qtd.Empregados.Inferior.1.Ciclo',
        '02. Básico - 1º Ciclo': 'Qtd.Empregados.Basico.1.Ciclo',
        '03. Básico - 2º Ciclo': 'Qtd.Empregados.Basico.2.Ciclo',
        '04. Básico - 3º Ciclo': 'Qtd.Empregados.Basico.3.Ciclo',
        '05. Secundário': 'Qtd.Empregados.Secundario',
        '06. Curso técnico superior profissional': 'Qtd.Empregados.TecnicoSuperiorProfissional',
        '07. Bacharelato': 'Qtd.Empregados.Bacharelado',
        '08. Licenciatura': 'Qtd.Empregados.Licenciatura',
        '09. Mestrado': 'Qtd.Empregados.Mestrado',
        '10. Doutoramento': 'Qtd.Empregados.Doutorado',
        'Total': 'Qtd.Empregados.Total'
    })

    df_emp_desemprego = df_emp_desemprego.rename(columns={
        'Menos de 25 anos': 'Qtd.Desempregados.Faixa.<25',
        '25-34': 'Qtd.Desempregados.Faixa.25-34',
        '35-44': 'Qtd.Desempregados.Faixa.35-44',
        '45-54': 'Qtd.Desempregados.Faixa.45-54',
        '55 ou mais anos': 'Qtd.Desempregados.Faixa.>55',
        'Total': 'Qtd.Desempregados.Faixa.Total'
    })

    df_emp_ramo.columns = df_emp_ramo.columns.str.strip().str.replace('\xa0', ' ', regex=False)
    df_emp_ramo.columns = [re.sub(r'\s+', ' ', col.strip()) for col in df_emp_ramo.columns]

    df_emp_ramo = df_emp_ramo.rename(columns={
        'A. Agricultura, produção animal, caça, floresta e pesca': 'Qtd.Empregos.Ramo.Agricultura_Caca_Floresta_Pesca',
        'A.01. Agricultura, produção animal, caça e atividades dos serviços relacionados': 'Qtd.Empregos.Ramo.Agricultura_Servicos_Relacionados',
        'A.02. Silvicultura e exploração florestal': 'Qtd.Empregos.Ramo.Silvicultura_Exploracao_Florestal',
        'A.03. Pesca e aquicultura': 'Qtd.Empregos.Ramo.Pesca_Aquicultura',
        'B. Indústrias extrativas': 'Qtd.Empregos.Ramo.Industrias_Extrativas',
        'C. Indústrias transformadoras': 'Qtd.Empregos.Ramo.Industrias_Transformadoras',
        'D. Eletricidade, gás, vapor, água quente e fria e ar frio': 'Qtd.Empregos.Ramo.Energia_Eletricidade_Gas',
        'E. Captação, tratamento e distribuição de água; saneamento, gestão de resíduos e despoluição': 'Qtd.Empregos.Ramo.Saneamento_Agua_Residuos',
        'F. Construção': 'Qtd.Empregos.Ramo.Construcao',
        'F.41. Promoção imobiliária (desenvolvimento de projetos de edifícios); construção de edifícios': 'Qtd.Empregos.Ramo.Promocao_Imobiliaria_Construcao',
        'F.42. Engenharia civil': 'Qtd.Empregos.Ramo.Engenharia_Civil',
        'F.43. Atividades especializadas de construção': 'Qtd.Empregos.Ramo.Construcao_Especializada',
        'G. Comércio por grosso e a retalho; reparação de veículos automóveis e motociclos': 'Qtd.Empregos.Ramo.Comercio_Manutencao_Veiculos',
        'G.45. Comércio, manutenção e reparação, de veículos automóveis e motociclos': 'Qtd.Empregos.Ramo.Comercio_Reparacao_Veiculos',
        'G.46. Comércio por grosso (inclui agentes), exceto de veículos automóveis e motociclos': 'Qtd.Empregos.Ramo.Comercio_Por_Grosso',
        'G.47. Comércio a retalho, exceto de veículos automóveis e motociclos': 'Qtd.Empregos.Ramo.Comercio_A_Retalho',
        'H. Transportes e armazenagem': 'Qtd.Empregos.Ramo.Transportes_Armazenagem',
        'I. Alojamento, restauração e similares': 'Qtd.Empregos.Ramo.Alojamento_Restauracao',
        'I.55. Alojamento': 'Qtd.Empregos.Ramo.Alojamento',
        'I.56. Restauração e similares': 'Qtd.Empregos.Ramo.Restauracao',
        'J. Atividades de informação e de comunicação': 'Qtd.Empregos.Ramo.Informacao_Comunicacao',
        'L. Atividades imobiliárias': 'Qtd.Empregos.Ramo.Atividades_Imobiliarias',
        'M. Atividades de consultoria, científicas, técnicas e similares': 'Qtd.Empregos.Ramo.Consultoria_Cientifica_Tecnica',
        'M.69. Atividades jurídicas e de contabilidade': 'Qtd.Empregos.Ramo.Juridico_Contabilidade',
        'M.70. Atividades das sedes sociais e de consultoria para a gestão': 'Qtd.Empregos.Ramo.Consultoria_Gestao',
        'M.71. Atividades de arquitetura, de engenharia e técnicas afins; atividades de ensaios e de análises técnicas': 'Qtd.Empregos.Ramo.Engenharia_Arquitetura_Analises',
        'N. Atividades administrativas e dos serviços de apoio': 'Qtd.Empregos.Ramo.Servicos_Administrativos_Apoio',
        'P. Educação': 'Qtd.Empregos.Ramo.Educacao',
        'Q. Atividades de saúde humana e apoio social': 'Qtd.Empregos.Ramo.Saude_Apoio_Social',
        'R. Atividades de teatro, de música, de dança e outras atividades artísticas e literárias': 'Qtd.Empregos.Ramo.Artes_Cultura',
        'S. Outras atividades de serviços': 'Qtd.Empregos.Ramo.Outros_Servicos',
        'Total':'Qtd.Empregos.Ramo.Total'
    })

    df_emprego_ = pd.merge(df_emp_emprego, df_emp_desemprego, on=['Região'], how='left')
    df_emprego = pd.merge(df_emprego_, df_emp_ramo, on=['Região'], how='left')

    df_emprego.drop(columns=['Ano','Ano_y'], inplace=True)
    df_emprego.rename(columns={'Ano_x': 'Ano'}, inplace=True)


    # Renda
    df_renda_mensal_escolaridade= porDataProcess.find_df_by_name(dfs,'ganho-medio-mensal-por-nivel-de-escolaridade')
    df_renda_mensal = porDataProcess.find_df_by_name(dfs,'ganho-medio-mensal')
    df_renda_poder_compra = porDataProcess.find_df_by_name(dfs,'poder-de-compra')

    df_renda_mensal_escolaridade = df_renda_mensal_escolaridade.rename(columns={
        '01. Inferior ao 1.º ciclo': 'Qtd.Renda.Escolaridade.Inferior.1.Ciclo',
        '02. Básico - 1º Ciclo': 'Qtd.Renda.Escolaridade.Basico.1.Ciclo',
        '03. Básico - 2º Ciclo': 'Qtd.Renda.Escolaridade.Basico.2.Ciclo',
        '04. Básico - 3º Ciclo': 'Qtd.Renda.Escolaridade.Basico.3.Ciclo',
        '05. Secundário': 'Qtd.Renda.Escolaridade.Secundario',
        '06. Curso técnico superior profissional': 'Qtd.Renda.Escolaridade.TecnicoSuperiorProfissional',
        '07. Bacharelato': 'Qtd.Renda.Escolaridade.Bacharelado',
        '08. Licenciatura': 'Qtd.Renda.Escolaridade.Licenciatura',
        '09. Mestrado': 'Qtd.Renda.Escolaridade.Mestrado',
        '10. Doutoramento': 'Qtd.Renda.Escolaridade.Doutorado',
        'Total': 'Qtd.Renda.Escolaridade.Total'
    })

    df_renda_mensal.drop(columns=['Âmbito Geográfico'], inplace=True)
    df_renda_mensal.rename(columns={'Valor': 'Qtd.Renda.Media.Mensal'},inplace=True)

    df_renda_poder_compra.drop(columns=['Âmbito Geográfico'], inplace=True)
    df_renda_poder_compra.rename(columns={'Valor': 'Qtd.Renda.Poder_de_Compra'},inplace=True)

    df_renda_ = pd.merge(df_renda_mensal,df_renda_mensal_escolaridade, on=['Região'], how='left')
    df_renda = pd.merge(df_renda_,df_renda_poder_compra, on=['Região'], how='left')

    df_renda.drop(columns=['Ano_y'], inplace=True)
    df_renda.rename(columns={'Ano_x': 'Ano'}, inplace=True)


    # Moradia
    df_moradia_arrendamento =  porDataProcess.find_df_by_name(dfs,'Valor Medio Arrendamento')
    df_moradia_construcoes = porDataProcess.find_df_by_name(dfs,'fogos-concluidos-em-construcoes-novas-para-habitacao')
    df_moradia_contrucoes_tipo = porDataProcess.find_df_by_name(dfs,'edificios-concluidos-por-tipo-de-obra-e-fim-que-se')

    df_moradia_construcoes = df_moradia_construcoes.rename(columns={
        'T0 ou T1': 'Qtd.Habitacao.Quartos.T0_T1',
        'T2': 'Qtd.Habitacao.Quartos.T2',
        'T3': 'Qtd.Habitacao.Quartos.T3',
        'T4 ou mais': 'Qtd.Habitacao.Quartos.T4_ou_mais',
        'Total': 'Qtd.Habitacao.Quartos.Total'
    })

    df_moradia_contrucoes_tipo = df_moradia_contrucoes_tipo.rename(columns={
        'Habitação familiar': 'Qtd.Habitacao.Familiar',
        'Outros': 'Qtd.Habitacao.Outros',
        'Ampliações, alterações e reconstruções': 'Qtd.Habitacao.Ampliacoes_Alteracoes_Reconstrucoes',
        'Construções novas': 'Qtd.Habitacao.Novas',
        'Total_y': 'Qtd.Habitacao.Total'
    })

    df_moradia_arrendamento = df_moradia_arrendamento.rename(columns={
        'Regiao': 'Região',
        'Codigo': 'Código',
        'Valor mediano das rendas por m2': 'Valor.Habitacao.Arendamento.m2',
        'Valor apartamento 100m²': 'Valor.Habitacao.Arendamento.100m2',
        'Valor apartamento 80m²': 'Valor.Habitacao.Arendamento.80m2'
    })

    df_moradia_ = pd.merge(df_moradia_construcoes,df_moradia_contrucoes_tipo, on=['Região'], how='left')
    df_moradia = pd.merge(df_moradia_,df_moradia_arrendamento, on=['Região'], how='left')
    df_moradia.drop(columns=['Ano_y','Código'], inplace=True)
    df_moradia.rename(columns={'Ano_x': 'Ano'}, inplace=True)



    df_emprego = df_emprego.drop(columns=['Ano'])
    df_populacao_emprego = pd.merge(df_populacao,df_emprego, on=['Região'], how='left')

    df_renda = df_renda.drop(columns=['Ano'])
    df_populacao_emprego_renda = pd.merge(df_populacao_emprego,df_renda, on=['Região'], how='left')

    df_moradia = df_moradia.drop(columns=['Ano'])
    df_populacao_emprego_renda_moradia = pd.merge(df_populacao_emprego_renda,df_moradia, on=['Região'], how='left')

    df_saude = df_saude.drop(columns=['Ano'])
    df_populacao_emprego_renda_moradia_saude = pd.merge(df_populacao_emprego_renda_moradia,df_saude, on=['Região'], how='left')

    df_educacao = df_educacao.drop(columns=['Ano'])
    df_populacao_emprego_renda_moradia_saude_educacao = pd.merge(df_populacao_emprego_renda_moradia_saude,df_educacao, on=['Região'], how='left')

    df_economia = df_economia.drop(columns=['Ano'])
    df_populacao_emprego_renda_moradia_saude_educacao_seguranca_economia = pd.merge(df_populacao_emprego_renda_moradia_saude_educacao,df_economia, on=['Região'], how='left')

    df_cultura = df_cultura.drop(columns=['Ano'])
    df_populacao_emprego_renda_moradia_saude_educacao_seguranca_economia_cultura = pd.merge(df_populacao_emprego_renda_moradia_saude_educacao_seguranca_economia,df_cultura, on=['Região'], how='left')

    df_populacao_emprego_renda_moradia_saude_educacao_seguranca_economia_cultura = df_populacao_emprego_renda_moradia_saude_educacao_seguranca_economia_cultura.loc[:, ~df_populacao_emprego_renda_moradia_saude_educacao_seguranca_economia_cultura.columns.duplicated()]

    # Exporta Base

    colunas_ordenadas = [
        'Ano', 'Região', 'Qtd.Populacao.Homens', 'Qtd.Populacao.Mulheres',
        'Qtd.Populacao.0_4_anos', 'Qtd.Populacao.5_9_anos', 'Qtd.Populacao.10_14_anos',
        'Qtd.Populacao.15_19_anos', 'Qtd.Populacao.20_24_anos', 'Qtd.Populacao.25_29_anos',
        'Qtd.Populacao.30_34_anos', 'Qtd.Populacao.35_39_anos', 'Qtd.Populacao.40_44_anos',
        'Qtd.Populacao.45_49_anos', 'Qtd.Populacao.50_54_anos', 'Qtd.Populacao.55_59_anos',
        'Qtd.Populacao.60_64_anos', 'Qtd.Populacao.65_69_anos', 'Qtd.Populacao.70_74_anos',
        'Qtd.Populacao.75_79_anos', 'Qtd.Populacao.80_84_anos', 'Qtd.Populacao.85_ou_mais_anos',
        'Qtd.Populacao.Total', 'Qtd.Populacao.Total.Criancas', 'Qtd.Populacao.Total.Adolescentes',
        'Qtd.Populacao.Total.Jovens_Adultos', 'Qtd.Populacao.Total.Adultos',
        'Qtd.Populacao.Total.Idosos', 'Qtd.Populacao.Proporcao.Criancas', 'Qtd.Populacao.Proporcao.Adolescentes',
        'Qtd.Populacao.Proporcao.Jovens_Adultos', 'Qtd.Populacao.Proporcao.Adultos',
        'Qtd.Populacao.Proporcao.Idosos', 'Qtd.Populacao.Densidade.hab.km',
        'Qtd.Populacao.Estrangeira.Africa', 'Qtd.Populacao.Estrangeira.Angola',
        'Qtd.Populacao.Estrangeira.Cabo_Verde', 'Qtd.Populacao.Estrangeira.Guine_Bissau',
        'Qtd.Populacao.Estrangeira.Mocambique', 'Qtd.Populacao.Estrangeira.Sao_Tome_e_Principe',
        'Qtd.Populacao.Estrangeira.America', 'Qtd.Populacao.Estrangeira.Brasil',
        'Qtd.Populacao.Estrangeira.Asia', 'Qtd.Populacao.Estrangeira.China',
        'Qtd.Populacao.Estrangeira.India', 'Qtd.Populacao.Estrangeira.Nepal',
        'Qtd.Populacao.Estrangeira.Europa', 'Qtd.Populacao.Estrangeira.Espanha',
        'Qtd.Populacao.Estrangeira.Franca', 'Qtd.Populacao.Estrangeira.Italia',
        'Qtd.Populacao.Estrangeira.Republica_da_Moldova', 'Qtd.Populacao.Estrangeira.Reino_Unido',
        'Qtd.Populacao.Estrangeira.Romenia', 'Qtd.Populacao.Estrangeira.Ucrania',
        'Qtd.Populacao.Estrangeira.Total', 'Qtd.Empregados.Inferior.1.Ciclo',
        'Qtd.Empregados.Basico.1.Ciclo', 'Qtd.Empregados.Basico.2.Ciclo',
        'Qtd.Empregados.Basico.3.Ciclo', 'Qtd.Empregados.Secundario',
        'Qtd.Empregados.TecnicoSuperiorProfissional', 'Qtd.Empregados.Bacharelado',
        'Qtd.Empregados.Licenciatura', 'Qtd.Empregados.Mestrado', 'Qtd.Empregados.Doutorado',
        'Qtd.Empregados.Total', 'Qtd.Desempregados.Faixa.<25', 'Qtd.Desempregados.Faixa.25-34',
        'Qtd.Desempregados.Faixa.35-44', 'Qtd.Desempregados.Faixa.45-54',
        'Qtd.Desempregados.Faixa.>55', 'Qtd.Desempregados.Faixa.Total',
        'Qtd.Empregos.Ramo.Agricultura_Caca_Floresta_Pesca',
        'Qtd.Empregos.Ramo.Agricultura_Servicos_Relacionados',
        'Qtd.Empregos.Ramo.Silvicultura_Exploracao_Florestal',
        'Qtd.Empregos.Ramo.Pesca_Aquicultura', 'Qtd.Empregos.Ramo.Industrias_Extrativas',
        'Qtd.Empregos.Ramo.Industrias_Transformadoras', 'Qtd.Empregos.Ramo.Energia_Eletricidade_Gas',
        'Qtd.Empregos.Ramo.Saneamento_Agua_Residuos', 'Qtd.Empregos.Ramo.Construcao',
        'Qtd.Empregos.Ramo.Promocao_Imobiliaria_Construcao', 'Qtd.Empregos.Ramo.Engenharia_Civil',
        'Qtd.Empregos.Ramo.Construcao_Especializada', 'Qtd.Empregos.Ramo.Comercio_Manutencao_Veiculos',
        'Qtd.Empregos.Ramo.Comercio_Reparacao_Veiculos', 'Qtd.Empregos.Ramo.Comercio_Por_Grosso',
        'Qtd.Empregos.Ramo.Comercio_A_Retalho', 'Qtd.Empregos.Ramo.Transportes_Armazenagem',
        'Qtd.Empregos.Ramo.Alojamento_Restauracao', 'Qtd.Empregos.Ramo.Alojamento',
        'Qtd.Empregos.Ramo.Restauracao', 'Qtd.Empregos.Ramo.Informacao_Comunicacao',
        'Qtd.Empregos.Ramo.Atividades_Imobiliarias', 'Qtd.Empregos.Ramo.Consultoria_Cientifica_Tecnica',
        'Qtd.Empregos.Ramo.Juridico_Contabilidade', 'Qtd.Empregos.Ramo.Consultoria_Gestao',
        'Qtd.Empregos.Ramo.Engenharia_Arquitetura_Analises',
        'Qtd.Empregos.Ramo.Servicos_Administrativos_Apoio', 'Qtd.Empregos.Ramo.Educacao',
        'Qtd.Empregos.Ramo.Saude_Apoio_Social', 'Qtd.Empregos.Ramo.Artes_Cultura',
        'Qtd.Empregos.Ramo.Outros_Servicos', 'Qtd.Empregos.Ramo.Total',
        'Qtd.Renda.Media.Mensal', 'Qtd.Renda.Escolaridade.Inferior.1.Ciclo',
        'Qtd.Renda.Escolaridade.Basico.1.Ciclo', 'Qtd.Renda.Escolaridade.Basico.2.Ciclo',
        'Qtd.Renda.Escolaridade.Basico.3.Ciclo', 'Qtd.Renda.Escolaridade.Secundario',
        'Qtd.Renda.Escolaridade.TecnicoSuperiorProfissional', 'Qtd.Renda.Escolaridade.Bacharelado',
        'Qtd.Renda.Escolaridade.Licenciatura', 'Qtd.Renda.Escolaridade.Mestrado',
        'Qtd.Renda.Escolaridade.Doutorado', 'Qtd.Renda.Escolaridade.Total',
        'Qtd.Renda.Poder_de_Compra', 'Qtd.Habitacao.Quartos.T0_T1', 'Qtd.Habitacao.Quartos.T2',
        'Qtd.Habitacao.Quartos.T3', 'Qtd.Habitacao.Quartos.T4_ou_mais',
        'Qtd.Habitacao.Quartos.Total', 'Qtd.Habitacao.Familiar', 'Qtd.Habitacao.Outros',
        'Qtd.Habitacao.Ampliacoes_Alteracoes_Reconstrucoes', 'Qtd.Habitacao.Novas',
        'Qtd.Habitacao.Total', 'Valor.Habitacao.Arendamento.m2', 'Valor.Habitacao.Arendamento.100m2',
        'Valor.Habitacao.Arendamento.80m2', 'Qtd.Hospitais.Publico.Privado', 'Qtd.Hospitais.Privado',
        'Qtd.Hospitais.Publico', 'Qtd.Hospitais.Total', 'Qtd.Farmacias',
        'Qtd.Ensino.NSuperior.Privado', 'Qtd.Ensino.NSuperior.Publico', 'Qtd.Ensino.NSuperior.artistica',
        'Qtd.Ensino.NSuperior.basica', 'Qtd.Ensino.NSuperior.basica.secundaria',
        'Qtd.Ensino.NSuperior.profissional', 'Qtd.Ensino.NSuperior.secundaria',
        'Qtd.Ensino.NSuperior.Jardim_de_infância', 'Qtd.Ensino.NSuperior.Total',
        'Qtd.Ensino.Superior.Privado', 'Qtd.Ensino.Superior.Publico', 'Qtd.Ensino.Superior.Total',
        'Qtd.Estabelecimentos.Bancarios', 'Qtd.Depositos.De.Imigrantes',
        'Qtd.Depositos.De.Outros.Clientes', 'Qtd.Depositos.Total', 'Qtd.Cinemas', 'Qtd.Sessões'
    ]

    df_final = df_populacao_emprego_renda_moradia_saude_educacao_seguranca_economia_cultura[colunas_ordenadas]

    porDataProcess.salva_base_agregada(df_final)

    filtro = df_final[df_final['Regiao'] == 'Lisboa'].head(5)

    for i, linha in filtro.iterrows():
        print(linha.to_string(), '\n')
