
import os
import re
import json
import pandas as pd
import re'
from unidecode import unidecode
from sklearn.cluster import KMeans
class IdealistaProcessData():
    folderPathBaseIdealista = '..\\..\\Bases\\idealista\\202508'
    folderPathBaseFinal = '..\\..\\Bases\\idealista\\base_processada\\base_imoveis.csv'
    folderPathBaseMunicipioFinal = '..\\..\\Bases\\idealista\\base_processada\\base_imoveis_municipio.csv'

    @staticmethod
    def listar_jsons(raiz):
        arquivos_json = []
        for pasta_atual, subpastas, arquivos in os.walk(raiz):
            for arquivo in arquivos:
                if arquivo.endswith('.json'):
                    caminho_completo = os.path.join(pasta_atual, arquivo)
                    arquivos_json.append(caminho_completo)
        return arquivos_json
    
    @staticmethod
    def ler_todos_jsons_para_dataframe(raiz):
        lista_df = []
        arquivos_json = listar_jsons(raiz)
        for caminho in arquivos_json:
            try:
                df = pd.read_json(caminho)
                df['arquivo_origem'] = os.path.basename(caminho)  # nome do arquivo
                df['caminho_pasta'] = os.path.dirname(caminho)    # caminho da pasta
                lista_df.append(df)
            except Exception as e:
                print(f"Erro ao ler {caminho}: {e}")
        if lista_df:
            df_final = pd.concat(lista_df, ignore_index=True)
        else:
            df_final = pd.DataFrame()

        return df_final

    @staticmethod
    def trata_imoveis(df):
        # Aplica ao DataFrame inteiro:
        df['municipio'] = df['arquivo_origem'].apply(extrair_municipio)
        df['caminho_pasta'] = df['caminho_pasta'].str.replace(r"^\.\.\\\.\.\\Bases\\idealista\\202508\\", "", regex=True)
        df['preco_mes'] = df['Preco'].apply(tratar_preco)
        df['preco_metro_quadrado'] = df['preco_mes'] / df['areaBrutaM2']
        df['numero_andar'] = df['pisoResumo'].apply(extrair_numero_andar)
        df['tem_elevador'] = df['pisoResumo'].apply(extrair_elevador)
        df['qtd_quartos'] = df['tipologia'].str.replace('T', '').astype(int)
        df['cat_quartos'] = df['qtd_quartos'].apply(categoria_quartos)
        df = marcar_outliers(df, 'preco_mes')
        df = marcar_outliers(df, 'preco_metro_quadrado')
        df = marcar_outliers(df, 'areaBrutaM2')
        

        colunas = [
            'arquivo_origem',
            'caminho_pasta',
            'municipio',
            'id',
            'Titulo',
            'detalhe do item',
            'Descricao do item',
            # 'estacionamento',
            # 'tipologia',
            'qtd_quartos',
            'cat_quartos',
            'numero_andar',
            'tem_elevador',
            # 'pisoResumo',
            'Link',
            'areaBrutaM2',
            # 'Preco',
            'preco_mes',
            'preco_metro_quadrado',
            # 'precoOriginal',
            # 'descontoPercentual',
            # 'tempoDestaque',
            'imagens',
            'tags'

        ]
        
        df = df[colunas]
        df = trata_tags(df)
        df = df.dropna(axis=1, how='all')

        return df

    @staticmethod
    def extrair_municipio(nome_arquivo):
        # Pega tudo antes de "-Paginas"
        resultado = re.match(r"^(.*?)-Paginas", nome_arquivo)
        if resultado:
            return resultado.group(1)
        else:
            return nome_arquivo.replace('.json', '')  # fallback: tira o .json
    @staticmethod 
    def tratar_preco(preco):
        # Remove tudo que não é número, ponto ou vírgula
        preco_limpo = ''.join(c for c in preco if c.isdigit() or c in '.,')
        # Se tiver mais de um ponto, é separador de milhar
        if preco_limpo.count('.') > 0 and preco_limpo.count(',') == 0:
            # Ex: 5.000 -> 5000
            preco_limpo = preco_limpo.replace('.', '')
        # Se tiver vírgula, é decimal
        preco_limpo = preco_limpo.replace(',', '.')
        try:
            return float(preco_limpo)
        except:
            return None
    @staticmethod
    def extrair_numero_andar(piso):
        if pd.isnull(piso):
            return None
        # Checa por 'Rés do chão' ou 'Cave'
        if "rés do chão" in piso.lower():
            return 0
        if "cave" in piso.lower():
            return -1
        
        # Novo padrão para 'Andar -2', 'Andar -1', etc
        match_andar_neg = re.search(r"andar\s*([-+]?\d+)", piso.lower())
        if match_andar_neg:
            return int(match_andar_neg.group(1))
        
        # Busca padrão de número
        match = re.search(r"(\d+)[ºo]? andar", piso.lower())
        if match:
            return int(match.group(1))
        return None
    
     @staticmethod
    def extrair_elevador(piso):
        if pd.isnull(piso):
            return None
        piso_lower = piso.lower()
        if "com elevador" in piso_lower:
            return 1
        if "sem elevador" in piso_lower:
            return 0
        # Se só tem "elevador" e não especificou com/sem, marca como True
        if "elevador" in piso_lower:
            return 1
        return None

    @staticmethod
    def categoria_quartos(q):
        if q == 0:
            return "0.Quarto"
        if q == 1:
            return "1.Quarto"
        elif q == 2:
            return "2.Quartos"
        elif q == 3:
            return "3.Quartos"
        elif q == 4:
            return "4.Quartos"
        else:
            return ">5.Quartos"
    @staticmethod
    def trata_tags(df):
        # Explode as tags em linhas
        df_exploded = df.explode('tags')
        # Gera as colunas de dummies (1 para cada tag)
        dummies = pd.get_dummies(df_exploded['tags'])

        # Junta com o id e agrupa pelo id, pegando o máximo (se tiver a tag, vira 1)
        df_tags = pd.concat([df_exploded['id'], dummies], axis=1).groupby('id').max().reset_index()

        tag_cols = dummies.columns
        df_tags[tag_cols] = df_tags[tag_cols].astype(int)

        # Junta de volta no dataframe original (se quiser)
        df = pd.merge(df, df_tags, on='id')
        
        return df
    
    @staticmethod
    def formatar_numericos(df):
            df = df.copy()  # Cria uma cópia, o original não será alterado!
            for col in df.select_dtypes(include='number').columns:
                df[col] = df[col].apply(lambda x: '{:,.2f}'.format(x).replace(',', 'X').replace('.', ',').replace('X', '.'))
            return df
    
    @staticmethod
    def marcar_outliers(df, coluna, fator=1.5):
        """
        Marca os outliers de uma coluna no DataFrame utilizando o método do IQR (Intervalo Interquartil).
        
        Parâmetros:
        - df: DataFrame que contém os dados.
        - coluna: o nome da coluna para verificar os outliers.
        - fator: o fator multiplicador para definir o limite superior e inferior (default: 1.5).
        
        Retorna:
        - O DataFrame com uma coluna 'outlier' indicando 'Outlier' ou 'Normal'.
        """
        # Calcular o IQR
        Q1 = df[coluna].quantile(0.25)
        Q3 = df[coluna].quantile(0.75)
        IQR = Q3 - Q1

        # Definir limites superior e inferior
        limite_inferior = Q1 - fator * IQR
        limite_superior = Q3 + fator * IQR

        # Marcar outliers
        column_name = 'outiler' + coluna
        df[column_name] = df[coluna].apply(lambda x: 'Outlier' if x < limite_inferior or x > limite_superior else 'Normal')

        return df

    @staticmethod
    def trata_municipios(df_imoveis):
        pivot = pd.pivot_table(
            df_imoveis,
            index='municipio',
            columns='cat_quartos',
            values='preco_mes',
            aggfunc=['mean', 'median'],
            fill_value=0
        )

        # Ajustando nome das colunas
        pivot.columns = [f'{func}.{col}' for func, col in pivot.columns]

        # Ordenando as colunas para ficar na sequência desejada
        ordem = [
            'mean.0.Quarto','mean.1.Quarto', 'mean.2.Quartos', 'mean.3.Quartos', 'mean.4.Quartos', 'mean.>5.Quartos',
            'median.0.Quarto','median.1.Quarto', 'median.2.Quartos', 'median.3.Quartos', 'median.4.Quartos', 'median.>5.Quartos'
        ]
        pivot = pivot.reindex(columns=ordem, fill_value=0)

        pivot = pivot.round(2)

        df_moveis_municipio = df_imoveis.groupby(['caminho_pasta','municipio']).agg(
        # Quantidades
        qtd_imoveis = ('id','count'),
        qtd_imoveis_T0 = ('qtd_quartos', lambda x: (x == 0).sum()),
        qtd_imoveis_T1 = ('qtd_quartos', lambda x: (x == 1).sum()),
        qtd_imoveis_T2 = ('qtd_quartos', lambda x: (x == 2).sum()),
        qtd_imoveis_T3 = ('qtd_quartos', lambda x: (x == 3).sum()),
        qtd_imoveis_T4 = ('qtd_quartos', lambda x: (x == 4).sum()),
        qtd_imoveis_T5_mais = ('qtd_quartos', lambda x: (x >= 5).sum()),

        # Valores
        media_valor_arrendamento = ('preco_mes', 'mean'),
        mediana_valor_arrendamento = ('preco_mes', 'median'),
        max_valor_arrendamento = ('preco_mes', 'max'),
        min_valor_arrendamento = ('preco_mes', 'min'),
        sdt_valor_arrendamento = ('preco_mes', 'std'),

        media_valor_metro_quadrado = ('preco_metro_quadrado', 'mean'),
        mediana_valor__metro_quadrado = ('preco_metro_quadrado', 'median'),
        max_valor_metro_quadrado = ('preco_metro_quadrado', 'max'),
        min_valor_metro_quadrado = ('preco_metro_quadrado', 'min'),
        std_valor_metro_quadrado = ('preco_metro_quadrado', 'std'),

        # Area
        media_metro_quadrado = ('areaBrutaM2', 'mean'),
        mediana_metro_quadrado  = ('areaBrutaM2', 'median'),
        max_metro_quadrado = ('areaBrutaM2', 'max'),
        min_metro_quadrado = ('areaBrutaM2', 'min'),
        std_metro_quadrado = ('areaBrutaM2', 'std'),

        ).reset_index()

        # join com o pivot
        df_moveis_municipio = pd.merge(df_moveis_municipio, pivot, how='left', on='municipio')

        df_moveis_municipio.rename(columns={
            'caminho_pasta': 'distrito',
            'municipio': 'municipio',
            'qtd_imoveis': 'qtd_imoveis',
            'media_valor_arrendamento': 'media_valor_arrendamento',
            'mediana_valor_arrendamento': 'mediana_valor_arrendamento',
            'max_valor_arrendamento': 'max_valor_arrendamento',
            'min_valor_arrendamento': 'min_valor_arrendamento',
            'sdt_valor_arrendamento': 'sdt_valor_arrendamento',
            'media_valor_metro_quadrado': 'media_valor_metro_quadrado',
            'mediana_valor__metro_quadrado': 'mediana_valor__metro_quadrado',
            'max_valor_metro_quadrado': 'max_valor_metro_quadrado',
            'min_valor_metro_quadrado': 'min_valor_metro_quadrado',
            'std_valor_metro_quadrado': 'std_valor_metro_quadrado',
            'media_metro_quadrado': 'media_metro_quadrado',
            'mediana_metro_quadrado': 'mediana_metro_quadrado',
            'max_metro_quadrado': 'max_metro_quadrado',
            'min_metro_quadrado': 'min_metro_quadrado',
            'std_metro_quadrado': 'std_metro_quadrado',
            'qtd_imoveis_T0': 'qtd_imoveis_T0',
            'qtd_imoveis_T1': 'qtd_imoveis_T1',
            'qtd_imoveis_T2': 'qtd_imoveis_T2',
            'qtd_imoveis_T3': 'qtd_imoveis_T3',
            'qtd_imoveis_T4': 'qtd_imoveis_T4',
            'qtd_imoveis_T5_mais': 'qtd_imoveis_T5_mais',
            'mean.0.Quarto': 'valor.medio.0.Quarto',
            'mean.1.Quarto': 'valor.medio.1.Quarto',
            'mean.2.Quartos': 'valor.medio.2.Quartos',
            'mean.3.Quartos': 'valor.medio.3.Quartos',
            'mean.4.Quartos': 'valor.medio.4.Quartos',
            'mean.5.ou.mais.Quartos': 'valor.medio.5.ou.mais.Quartos',
            'median.0.Quarto': 'valor.mediano.0.Quarto',
            'median.1.Quarto': 'valor.mediano.1.Quarto',
            'median.2.Quartos': 'valor.mediano.2.Quartos',
            'median.3.Quartos': 'valor.mediano.3.Quartos',
            'median.4.Quartos': 'valor.mediano.4.Quartos',
            'median.5.ou.mais.Quartos': 'valor.mediano.5.ou.mais.Quartos'
        }, inplace=True)

        return df_moveis_municipio
    
    @staticmethod
    def salva_base(df, filePath):
        df.to_csv(filePath, index=False)


if __name__ == "__main__":
    idProc = IdealistaProcessData()
    
    df_total = idProc.ler_todos_jsons_para_dataframe(idProc.folderPathBaseIdealista)
    df_imoveis = idProc.trata_imoveis(df_total)

    base = idProc.formatar_numericos(df_imoveis)
    idProc.salva_base(base, idProc.folderPathBaseFinal)

    df_moveis_municipio = idProc.trata_municipios(df_imoveis)

    base_municipio = idProc.formatar_numericos(df_moveis_municipio)
    idProc.salva_base(base_municipio, idProc.folderPathBaseMunicipioFinal)
