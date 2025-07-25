#pip install geopandas
#pip install plotly

import pandas as pd
import numpy as np
import os
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import plotly.express as px
from unidecode import unidecode


class ScoreUtils:
    SCORE_EXCELENTE = 80
    SCORE_MUITO_BOM = 70
    SCORE_BOM = 60
    SCORE_ACEITAVEL = 50

    @classmethod
    def classifica_score(cls, score):
        if score >= cls.SCORE_EXCELENTE:
            return 'Excelente'
        elif score >= cls.SCORE_MUITO_BOM:
            return 'Muito Bom'
        elif score >= cls.SCORE_BOM:
            return 'Bom'
        elif score >= cls.SCORE_ACEITAVEL:
            return 'Aceitável'
        else:
            return 'Marginal'
        
class WeatherClassifier:
    filePathClima = '..\\..\\Bases\\Clima\\worldWeatherApi\\'
    filePathMunicipios = '..\\..\\Bases\\Municipios\\portugalMunicipios.csv'
    filePathGeo = '..\\..\\Bases\\Geocoding\\portugalMunicipiosGeo.csv'
    filePathRankingBase = '..\\..\\Bases\\Clima\\worldWeatherApi\\ranking\\ranking_base.csv'
    filePathRankingMensal = '..\\..\\Bases\\Clima\\worldWeatherApi\\ranking\\ranking_Mensal.csv'
    filePathRankingGeral = '..\\..\\Bases\\Clima\\worldWeatherApi\\ranking\\ranking_Geral.csv'
    filePathRankingTCI = '..\\..\\Bases\\Clima\\worldWeatherApi\\ranking\\ranking_TCI.csv'

    shapefile_path = '..\\..\\Bases\\Geocoding\\files\\Gadm - Portugal - Shape\\gadm41_PRT_0.shp' 

    # === FUNÇÕES DE DETECÇÃO DE EVENTOS EXTREMOS ===
    def is_hot_extreme(self, row):
        """Evento de calor extremo: Temperatura máxima > 35°C OU Heat Index > 40°C."""
        return row['maxtempC'] > 35 or row['HeatIndexC'] > 40

    def is_cold_extreme(self, row):
        """Evento de frio extremo: Temperatura mínima < 1°C OU Wind Chill < -5°C."""
        return row['mintempC'] < 1 or row['WindChillC'] < -5

    def is_rain_extreme(self, row):
        """Evento de chuva extrema: Precipitação diária > 35 mm."""
        return row['precipMM'] > 35

    def is_snow_extreme(self, row):
        """Evento de neve: Qualquer valor positivo já é extremo em Portugal continental."""
        return row['totalSnow_cm'] > 0

    def is_uv_extreme(self, row):
        """Radiação UV extrema: Índice UV >= 8 (OMS: muito alto)."""
        return row['uvIndex'] >= 8

    def assign_season(self, month):
        if month in [12, 1, 2]:
            return 'Winter'
        elif month in [3, 4, 5]:
            return 'Spring'
        elif month in [6, 7, 8]:
            return 'Summer'
        else:
            return 'Autumn'

    # Removed unused classify_city_index method.

    def compute_tci_monthly(self, df):
        df = df.copy()
        df['month'] = df['date_time'].dt.month
        tci_list = []
        for (year, month), dfg in df.groupby([df['date_time'].dt.year, 'month']):
            # ---- CID: conforto diário (temp média)
            tmean = dfg['tempC'].mean()
            rhmean = dfg['humidity'].mean()
            if 20 <= tmean <= 27:
                cid = 10
            elif 16 <= tmean < 20 or 27 < tmean <= 30:
                cid = 7
            elif 13 <= tmean < 16 or 30 < tmean <= 33:
                cid = 5
            elif 10 <= tmean < 13 or 33 < tmean <= 36:
                cid = 3
            else:
                cid = 0
            if rhmean > 90 or rhmean < 30:
                cid -= 1

            # ---- CIA: conforto amplitude diária (temp máxima)
            tmax = dfg['maxtempC'].mean()
            rhmax = dfg['humidity'].mean()
            if 20 <= tmax <= 27:
                cia = 10
            elif 16 <= tmax < 20 or 27 < tmax <= 30:
                cia = 7
            elif 13 <= tmax < 16 or 30 < tmax <= 33:
                cia = 5
            elif 10 <= tmax < 13 or 33 < tmax <= 36:
                cia = 3
            else:
                cia = 0
            if rhmax > 90 or rhmax < 30:
                cia -= 1

            # ---- P: índice de precipitação do mês
            p = dfg['precipMM'].sum()
            if p <= 30:
                pi = 10
            elif p <= 60:
                pi = 7
            elif p <= 90:
                pi = 5
            elif p <= 120:
                pi = 3
            else:
                pi = 0

            # ---- S: insolação média diária
            s = dfg['sunHour'].mean()
            if s >= 9:
                si = 10
            elif s >= 7:
                si = 7
            elif s >= 5:
                si = 5
            elif s >= 3:
                si = 3
            else:
                si = 0

            # ---- W: vento médio no mês
            w = dfg['windspeedKmph'].mean()
            if w < 15:
                wi = 10
            elif w < 25:
                wi = 7
            elif w < 35:
                wi = 5
            elif w < 45:
                wi = 3
            else:
                wi = 0

            tci = 2*cid + 2*cia + 0.5*pi + 0.5*si + wi
            tci = max(0, min(100, tci))
            tci_list.append({'ano': year, 'mes': month, 'tci': tci})

        tci_df = pd.DataFrame(tci_list)
        tci_annual = tci_df['tci'].mean() if not tci_df.empty else np.nan
        return tci_annual, tci_df

    def marcar_extremo(self, row):
        """Retorna True se houver qualquer evento extremo (calor, frio, chuva, neve)"""
        return (
            self.is_hot_extreme(row) or
            self.is_cold_extreme(row) or
            self.is_rain_extreme(row) or
            self.is_snow_extreme(row)
        )

    def score_dia(self, row):
        maxtemp = row['maxtempC']
        mintemp = row['mintempC']
        humidity = row['humidity']
        sun = row['sunHour']
        precip = row['precipMM']

        # Conforto máxima
        ideal_temp = 1 if (20 <= maxtemp <= 28) else 0
        moderate_temp = 1 if (18 < maxtemp < 32) else 0
        comfort_temp = 0.7 * ideal_temp + 0.3 * moderate_temp

        # Conforto mínima (peso de 15%)
        if 16 <= mintemp <= 20:
            comfort_mintemp = 1
        elif 13 <= mintemp < 16 or 20 < mintemp <= 22:
            comfort_mintemp = 0.7
        elif 10 <= mintemp < 13 or 22 < mintemp <= 24:
            comfort_mintemp = 0.4
        else:
            comfort_mintemp = 0

        comfort_hum = 1 if (30 <= humidity <= 80) else 0
        comfort_sun = 1 if sun >= 5 else 0
        comfort_precip = 1 if precip <= 20 else 0

        penalizacao = 0
        if self.is_hot_extreme(row): penalizacao += 0.3
        if self.is_cold_extreme(row): penalizacao += 0.4
        if self.is_rain_extreme(row): penalizacao += 0.2

        penalizacao = min(penalizacao, 1)

        # Score ponderado (máxima 50%, mínima 15%, umidade 10%, sol 10%, chuva 10%)
        score = 100 * (
            0.6 * comfort_temp +
            0.12 * comfort_mintemp +
            0.10 * comfort_hum +
            0.09 * comfort_sun +
            0.09 * comfort_precip
        )
        score -= penalizacao * 20
        return max(score, 0)

    def load_municipio_geo(self):
        geo = pd.read_csv(self.filePathGeo)
        return geo
    
    def load_municipio_data(self):
        df_municipios =  pd.read_csv(self.filePathMunicipios)
        return df_municipios
    
    def merge_municipios_with_weather(self, df_weather, df_municipios):

        df_weather['location_merge'] = df_weather['municipio'].apply(lambda x: unidecode(str(x)).lower().strip())
        df_municipios['Cidades_merge'] = df_municipios['Cidades'].apply(lambda x: unidecode(str(x)).lower().strip())


        # Junta pelo nome da cidade padronizado
        df_weather = pd.merge(
            df_weather,
            df_municipios,
            left_on='location_merge',
            right_on='Cidades_merge',
            how='left'
        )
        return df_weather
    
    def merge_geo_with_weather(self, df_resumo_geral, df_geo):
        # Padroniza nomes para garantir o merge
        df_resumo_geral['location_merge'] = df_resumo_geral['municipio'].apply(lambda x: unidecode(str(x)).lower().strip())
        df_geo['Cidades_merge'] = df_geo['Cidades'].apply(lambda x: unidecode(str(x)).lower().strip())

        # Junta pelo nome da cidade padronizado
        df_resumo_geral_geo = pd.merge(
            df_resumo_geral,
            df_geo,
            left_on='location_merge',
            right_on='Cidades_merge',
            how='inner'
        )

        return df_resumo_geral_geo
    
    def load_weather_data(self, min_days=365):
        
        # Lista dos nomes das colunas (caso leia de um CSV sem cabeçalho)
        colunas = [
            'date_time','maxtempC','mintempC','totalSnow_cm','sunHour','uvIndex','moon_illumination',
            'moonrise','moonset','sunrise','sunset','DewPointC','FeelsLikeC','HeatIndexC','WindChillC',
            'WindGustKmph','cloudcover','humidity','precipMM','pressure','tempC','visibility',
            'winddirDegree','windspeedKmph','location'
        ]

        # Dicionário de tipos para cada coluna
        tipos = {
            'date_time': 'datetime64[ns]',
            'maxtempC': 'Int64',
            'mintempC': 'Int64',
            'totalSnow_cm': 'float',
            'sunHour': 'float',
            'uvIndex': 'Int64',
            'moon_illumination': 'Int64',
            'moonrise': 'string',
            'moonset': 'string',
            'sunrise': 'string',
            'sunset': 'string',
            'DewPointC': 'Int64',
            'FeelsLikeC': 'Int64',
            'HeatIndexC': 'Int64',
            'WindChillC': 'Int64',
            'WindGustKmph': 'Int64',
            'cloudcover': 'Int64',
            'humidity': 'Int64',
            'precipMM': 'float',
            'pressure': 'Int64',
            'tempC': 'Int64',
            'visibility': 'Int64',
            'winddirDegree': 'Int64',
            'windspeedKmph': 'Int64',
            'location': 'string'
        }

        dfs = []
        arquivos_descartados = []
        for filename in os.listdir(self.filePathClima):
            if filename.endswith(".csv"):
                file_path = os.path.join(self.filePathClima, filename)
                df = pd.read_csv(file_path, parse_dates=['date_time'])
                city_name = filename.split(',')[0].replace("+", " ")
                df['municipio'] = city_name

                num_dias = df['date_time'].nunique()
                periodo = df['date_time'].max() - df['date_time'].min()

                if num_dias >= min_days:
                    dfs.append(df)
                else:
                    arquivos_descartados.append((filename, num_dias, periodo.days))

        print("\nArquivos descartados (menos de", min_days, "dias de histórico):")
        for nome, dias, periodo in arquivos_descartados:
            print(f"{nome}: {dias} dias distintos, período coberto: {periodo} dias")

        if not dfs:
            print("Nenhum arquivo com histórico suficiente.")
            return pd.DataFrame()

        combined_df = pd.concat(dfs, ignore_index=True)

        # Converte as colunas de acordo com o dicionário de tipos
        for col, tipo in tipos.items():
            if tipo == 'datetime64[ns]':
                combined_df[col] = pd.to_datetime(combined_df[col])
            else:
                combined_df[col] = combined_df[col].astype(tipo)

        return combined_df

    def formatar_numericos(self, df):
        df = df.copy()  # Cria uma cópia, o original não será alterado!
        for col in df.select_dtypes(include='number').columns:
            df[col] = df[col].apply(lambda x: '{:,.2f}'.format(x).replace(',', 'X').replace('.', ',').replace('X', '.'))
        return df

    def resumo_mensal(self, df):
        resumo_mensal = df.groupby(['Regiao','municipio', 'ano_mes','estacao']).agg(
            dias_no_mes = ('classificacao_dia', 'count'),
            dias_bons = ('classificacao_dia', lambda x: (x.isin(['Excelente','Muito Bom','Bom'])).sum()),
            dias_aceitaveis = ('classificacao_dia', lambda x: (x == 'Aceitável').sum()),
            dias_marginais = ('classificacao_dia', lambda x: (x == 'Marginal').sum()),
            extremos = ('evento_extremo', 'sum'),
            percentual_extremos = ('evento_extremo', lambda x: 100*x.sum()/len(x)),
            media_score = ('score_dia', 'mean'),
            score_max = ('score_dia', 'max'),
            score_min = ('score_dia', 'min'),
            score_std = ('score_dia', 'std'),
            media_score_continuous = ('score_dia_cont', 'mean'),
            media_maxtemp = ('maxtempC', 'mean'),
            maxtemp_max = ('maxtempC', 'max'),
            media_mintemp = ('mintempC', 'mean'),
            mintemp_min = ('mintempC', 'min'),
            media_totalSnow_cm = ('totalSnow_cm', 'mean'),
            media_sunHour = ('sunHour', 'mean'),
            media_uvIndex = ('uvIndex', 'mean'),
            media_FeelsLikeC = ('FeelsLikeC', 'mean'),
            media_HeatIndexC = ('HeatIndexC', 'mean'),
            media_WindChillC = ('WindChillC', 'mean'),
            media_humidity = ('humidity', 'mean'),
            media_precipMM = ('precipMM', 'mean'),
            media_tempC = ('tempC', 'mean'),
            calor_extremo = ('calor_extremo', 'sum'),
            frio_extremo = ('frio_extremo', 'sum'),
            chuva_extrema = ('chuva_extrema', 'sum'),
            neve = ('neve', 'sum'),
            uv_extremo = ('uv_extremo', 'sum'),
            class_mais_comum = ('classificacao_dia', lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan)
        ).reset_index()

        resumo_mensal['classificacao_mes'] = resumo_mensal['media_score'].apply(ScoreUtils.classifica_score)
        resumo_mensal['classificacao_geral_continuous'] = resumo_mensal['media_score_continuous'].apply(ScoreUtils.classifica_score)

        return resumo_mensal

    def resumo_geral(self, df):
        resumo_geral = df.groupby(['Regiao','municipio']).agg(
            total_dias = ('classificacao_dia', 'count'),
            dias_bons = ('classificacao_dia', lambda x: (x.isin(['Excelente','Muito Bom','Bom'])).sum()),
            dias_aceitaveis = ('classificacao_dia', lambda x: (x == 'Aceitável').sum()),
            dias_marginais = ('classificacao_dia', lambda x: (x == 'Marginal').sum()),
            extremos = ('evento_extremo', 'sum'),
            percentual_extremos = ('evento_extremo', lambda x: 100*x.sum()/len(x)),
            calor_extremo = ('calor_extremo', 'sum'),
            frio_extremo = ('frio_extremo', 'sum'),
            chuva_extrema = ('chuva_extrema', 'sum'),
            neve = ('neve', 'sum'),
            uv_extremo = ('uv_extremo', 'sum'),
            media_score = ('score_dia', 'mean'),
            media_score_continuous = ('score_dia_cont', 'mean'),
            media_maxtemp = ('maxtempC', 'mean'),
            media_mintemp = ('mintempC', 'mean'),
            media_totalSnow_cm = ('totalSnow_cm', 'mean'),
            media_sunHour = ('sunHour', 'mean'),
            media_uvIndex = ('uvIndex', 'mean'),
            media_FeelsLikeC = ('FeelsLikeC', 'mean'),
            media_HeatIndexC = ('HeatIndexC', 'mean'),
            media_WindChillC = ('WindChillC', 'mean'),
            media_humidity = ('humidity', 'mean'),
            media_precipMM = ('precipMM', 'mean'),
            media_tempC = ('tempC', 'mean')
        ).reset_index()

        resumo_geral['classificacao_geral'] = resumo_geral['media_score'].apply(ScoreUtils.classifica_score)
        resumo_geral['classificacao_geral_continuous'] = resumo_geral['media_score_continuous'].apply(ScoreUtils.classifica_score)
        return resumo_geral

    def tci(self, df):
        tci_registros = []

        for municipio, dados in df.groupby(['Regiao','municipio']):
            # Calcula TCI mensal e anual
            tci_annual, tci_mensal = self.compute_tci_monthly(dados)
            # Para cada mês desse município, anote também as médias dos componentess do TCI
            dados['year'] = dados['date_time'].dt.year
            dados['month'] = dados['date_time'].dt.month
            for i, row in tci_mensal.iterrows():
                ano = row['ano']
                mes = row['mes']
                # Seleciona só aquele mês/ano
                dados_mes = dados[(dados['year'] == ano) & (dados['month'] == mes)]
                # Calcula as médias/indicadores do TCI:
                tmean = dados_mes['tempC'].mean()
                rhmean = dados_mes['humidity'].mean()
                tmax = dados_mes['maxtempC'].mean()
                # Se quiser rhmax separado (UR máxima), coloque aqui, mas no seu código estava usando média:
                rhmax = dados_mes['humidity'].mean()
                precip = dados_mes['precipMM'].sum()
                sun = dados_mes['sunHour'].mean()
                wind = dados_mes['windspeedKmph'].mean()
                # Monta registro
                reg = {
                    'municipio': municipio,
                    'ano': ano,
                    'mes': mes,
                    'tci': row['tci'],
                    'tci_anual': tci_annual,
                    'tempC_media': tmean,
                    'humidity_media': rhmean,
                    'maxtempC_media': tmax,
                    'humidity_max_media': rhmax,  # Aqui está igual ao rhmean, pode mudar se quiser pegar o valor máximo.
                    'precipMM_total': precip,
                    'sunHour_media': sun,
                    'windspeedKmph_media': wind
                }
                tci_registros.append(reg)

        # Junta tudo em um DataFrame
        tci_df = pd.DataFrame(tci_registros)

        # Se quiser, pode reordenar as colunas
        colunas = [
            'municipio', 'ano', 'mes', 'tci', 'tci_anual',
            'tempC_media', 'humidity_media', 'maxtempC_media', 'humidity_max_media',
            'precipMM_total', 'sunHour_media', 'windspeedKmph_media'
        ]
        tci_df = tci_df[colunas]

        return tci_df

    def marcacao_extremos_diarios(self, df):
        df['calor_extremo']   = df.apply(self.is_hot_extreme, axis=1).astype(int)
        df['frio_extremo']    = df.apply(self.is_cold_extreme, axis=1).astype(int)
        df['chuva_extrema']   = df.apply(self.is_rain_extreme, axis=1).astype(int)
        df['neve']            = df.apply(self.is_snow_extreme, axis=1).astype(int)
        df['uv_extremo']      = df.apply(self.is_uv_extreme, axis=1).astype(int)
        df['evento_extremo'] = df.apply(self.marcar_extremo, axis=1)
        return df

    def classifica_diario(self, df):
        df['score_dia'] = df.apply(self.score_dia, axis=1)
        df['classificacao_dia'] = df['score_dia'].apply(ScoreUtils.classifica_score)
        df['ano_mes'] = df['date_time'].dt.to_period('M')
        df['ano'] = df['date_time'].dt.year
        df['estacao'] = df['ano_mes'].apply(lambda x: self.assign_season(x.month))

        df = df[['date_time', 'maxtempC', 'mintempC', 'totalSnow_cm', 'sunHour',
        'uvIndex', 'moon_illumination', 'moonrise', 'moonset', 'sunrise',
        'sunset', 'DewPointC', 'FeelsLikeC', 'HeatIndexC', 'WindChillC',
        'WindGustKmph', 'cloudcover', 'humidity', 'precipMM', 'pressure',
        'tempC', 'visibility', 'winddirDegree', 'windspeedKmph', 'location',
        'municipio',  'Regiao', 'calor_extremo', 'frio_extremo', 'chuva_extrema',
        'neve', 'uv_extremo', 'evento_extremo', 'score_dia',
        'classificacao_dia', 'ano_mes', 'ano', 'estacao']]
        
        return df

    def salva_rankings(self, df, df_resumo_mensal, df_resumo_geral, df_tci):
        df_formatado = self.formatar_numericos(df)
        df_formatado.to_csv(self.filePathRankingBase, index=False)
        del(df_formatado)

        # Salva Ranking Mensal
        resumo_mensal_formatado = self.formatar_numericos(df_resumo_mensal)
        resumo_mensal_formatado.to_csv(self.filePathRankingMensal, index=False)
        del(resumo_mensal_formatado)

        # Salva Ranking Geral
        
        resumo_geral_formatado = self.formatar_numericos(df_resumo_geral)
        resumo_geral_formatado.to_csv(self.filePathRankingGeral, index=False)
        del(resumo_geral_formatado)

        # Salva Ranking TCI
        
        tci_df_formatado = self.formatar_numericos(df_tci)
        tci_df_formatado.to_csv(self.filePathRankingTCI, index=False)
        del(tci_df_formatado)

    def topx_cidades(self, df_resumo_geral, n=100, score_type='media_score'):
        topx = df_resumo_geral.sort_values(score_type, ascending=False).head(n)

        texto = f"🏆 Top {n} cidades com melhor score climático ({score_type}) 🏆\n"
        for i, row in enumerate(topx.itertuples(), 1):
            score_valor = getattr(row, score_type)
            texto += (f"{i:02d}. {row.municipio}: {score_valor:.1f} pontos | "
                    f"Extremos: {row.extremos} | "
                    f"Máx: {row.media_maxtemp:.1f}°C | "
                    f"Mín: {row.media_mintemp:.1f}°C\n")
        print(texto)

    def topx_cidades_por_estacao(self, df_resumo_mensal, n=10, score_type='media_score'):

        for estacao in ['Summer', 'Autumn', 'Spring', 'Winter']:
            print(f"\n🏅 Top {n} cidades - {estacao} ({score_type}) 🏅")
            # Seleciona o melhor mês de cada cidade na estação (pelo maior score_tipe)
            top_por_cidade = (
                df_resumo_mensal[df_resumo_mensal['estacao'] == estacao]
                .sort_values(score_type, ascending=False)
                .groupby('municipio', as_index=False)
                .first()
            )
            # Agora pega só os n melhores
            top10 = top_por_cidade.sort_values(score_type, ascending=False).head(n)
            for i, row in enumerate(top10.itertuples(), 1):
                score_valor = getattr(row, score_type)
                print(
                    f"{i:02d}. {row.municipio}: {score_valor:.1f} pontos | "
                    f"Extremos: {row.extremos} | "
                    f"Máx: {row.media_maxtemp:.1f}°C | "
                    f"Mín: {row.media_mintemp:.1f}°C | "
                    f"{row.classificacao_mes} | {row.ano_mes}"
                )

    def cidades_ranking(self, df_resumo_geral, score_tipe='media_score'):
        # Decide qual campo de classificação usar
        if score_tipe == 'score_continuous':
            campo_classificacao = 'classificacao_geral_continuous'
        else:
            campo_classificacao = 'classificacao_geral'

        ordem_classificacao = ['Excelente', 'Muito Bom', 'Bom', 'Aceitável', 'Marginal']

        texto = f'🏆 Lista de cidades agrupadas por classificação (score: {score_tipe}) 🏆\n'
        for classificacao in ordem_classificacao:
            grupo = df_resumo_geral[df_resumo_geral[campo_classificacao] == classificacao] \
                .sort_values(score_tipe, ascending=False)
            if not grupo.empty:
                texto += f"\n🔹 {classificacao}:\n"
                for idx, row in grupo.iterrows():
                    texto += (
                        f"   - [{row['Regiao']}] {row['municipio']}: "
                        f"{row[score_tipe]:.1f} pontos | "
                        f"Extremos: {row['extremos']} | "
                        f"Máx: {row['media_maxtemp']:.1f}°C | "
                        f"Mín: {row['media_mintemp']:.1f}°C\n"
                    )

        print(texto)
    
    def cidades_ranking_tci(self, df_tci):

        tci_ordenado = df_tci.sort_values('tci_anual', ascending=False)

        colunas_para_mostrar = ['municipio', 'Regiao', 'tci_anual'] if 'Regiao' in tci_ordenado.columns else ['municipio', 'tci_anual']

        # Imprime o ranking completo
        print("🏆 Ranking das cidades pelo TCI Anual 🏆\n")
        for i, row in enumerate(tci_ordenado[colunas_para_mostrar].drop_duplicates(subset=['municipio']).itertuples(), 1):
            print(f"{i:02d}. {getattr(row, 'municipio')} - {getattr(row, 'Regiao', '')} | TCI Anual: {getattr(row, 'tci_anual'):.1f}")

    def plot_map(self, resumo_geral_geo):
        # 1. Carrega e limpa o shapefile
        portugal = gpd.read_file(self.shapefile_path)

        # Corrige eventuais geometrias inválidas
        portugal['geometry'] = portugal['geometry'].buffer(0)
        # Remove linhas sem geometria válida
        portugal = portugal[portugal.is_valid & ~portugal.is_empty]

        if portugal.empty:
            raise ValueError('O shapefile está vazio ou inválido!')

        # Faz a união de todos os polígonos do shape (Continente + Madeira + Açores)
        portugal_union = portugal.geometry.unary_union

        # 2. Converte sua tabela de cidades para GeoDataFrame
        # ranking_geo deve ter as colunas 'municipio', 'latitude', 'longitude', 'classificacao_geral'
        cores = {
            'Excelente': 'blue',
            'Muito Bom': 'blue',
            'Bom': 'green',
            'Aceitável': 'yellow',
            'Marginal': 'red'
        }
        resumo_geral_geo['cor'] = resumo_geral_geo['classificacao_geral'].map(cores)

        gdf = gpd.GeoDataFrame(
            resumo_geral_geo,
            geometry=gpd.points_from_xy(resumo_geral_geo.longitude, resumo_geral_geo.latitude),
            crs="EPSG:4326"
        )

        # 3. Marque pontos dentro e fora de Portugal (incluindo ilhas)
        gdf['dentro_portugal'] = gdf.geometry.within(portugal_union)

        # Separe pontos
        dentro_portugal = gdf[gdf['dentro_portugal']]
        fora_portugal = gdf[~gdf['dentro_portugal']]

        # 4. Mostre os possíveis erros
        if not fora_portugal.empty:
            print('Cidades possivelmente com coordenadas erradas:')
            print(fora_portugal[['municipio', 'latitude', 'longitude']])
        else:
            print('Todas as cidades estão corretamente localizadas em Portugal.')

        # 5. Plot bonito!
        fig, ax = plt.subplots(figsize=(11, 13))
        portugal.plot(ax=ax, color='lightgrey', edgecolor='black', zorder=0)
        dentro_portugal.plot(
            ax=ax, color=dentro_portugal['cor'], markersize=40,
            marker='o', edgecolor='black', linewidth=0.7, zorder=2
        )
        if not fora_portugal.empty:
            fora_portugal.plot(
                ax=ax, color='black', markersize=70, marker='x',
                label='Possível erro', zorder=3
            )

        patches = [
            mpatches.Patch(color='blue', label='Excelente/Muito Bom'),
            mpatches.Patch(color='green', label='Bom'),
            mpatches.Patch(color='yellow', label='Aceitável'),
            mpatches.Patch(color='red', label='Marginal'),
            mpatches.Patch(color='black', label='Possível erro')
        ]
        plt.legend(handles=patches, loc='lower left', fontsize=10)
        plt.title('Classificação Climática das Cidades de Portugal (GADM, incl. ilhas)')
        plt.axis('off')
        plt.tight_layout()
        plt.show()

    def plot_temperaturas(self, cidades_lista, df_resumo_mensal):
        # Converte de Period para string formato 'ano-mês' (sem warning do pandas)
        df_resumo_mensal = df_resumo_mensal.copy()  # Evita SettingWithCopyWarning
        df_resumo_mensal['ano_mes'] = df_resumo_mensal['ano_mes'].astype(str)
        
        for cidade in cidades_lista:
            cidade_data = df_resumo_mensal[df_resumo_mensal['municipio'] == cidade]

            # Derrete os dados para formato long (necessário para legendas separadas em px.line)
            cidade_long = cidade_data.melt(
                id_vars=['ano_mes', 'estacao'],
                value_vars=['media_maxtemp', 'media_mintemp'],
                var_name='Tipo',
                value_name='Temperatura'
            )

            # Cria o gráfico com Plotly Express
            fig = px.line(
                cidade_long,
                x='ano_mes',
                y='Temperatura',
                color='Tipo',
                labels={'ano_mes': 'Ano-Mês', 'Temperatura': 'Temperatura (°C)', 'Tipo': 'Tipo'},
                title=f'Temperaturas Mensais - {cidade}',
            )

            # Personaliza as cores manualmente (vermelho e azul)
            fig.for_each_trace(
                lambda trace: trace.update(
                    line=dict(color='red') if trace.name == 'media_maxtemp' else dict(color='blue'),
                    name='MaxTemp (mean)' if trace.name == 'media_maxtemp' else 'MinTemp (mean)'
                )
            )

            # Estações e linhas verticais
            estacoes = cidade_data['estacao'].unique()
            y_min = cidade_long['Temperatura'].min()
            y_max = cidade_long['Temperatura'].max()

            for estacao in estacoes:
                estacao_rows = cidade_data[cidade_data['estacao'] == estacao]
                if not estacao_rows.empty:
                    estacao_mes = estacao_rows.iloc[0]['ano_mes']

                    fig.add_shape(
                        type="line",
                        x0=estacao_mes, x1=estacao_mes,
                        y0=y_min, y1=y_max,
                        line=dict(color="gray", dash="dash", width=2),
                        xref='x', yref='y'
                    )

                    fig.add_annotation(
                        x=estacao_mes,
                        y=y_max,
                        text=estacao,
                        showarrow=True,
                        arrowhead=2,
                        ax=0,
                        ay=-30,
                        font=dict(size=12, color="black"),
                        bgcolor="white",
                        borderpad=4,
                        bordercolor="gray",
                        borderwidth=1
                    )

            fig.update_layout(
                xaxis_title="Ano-Mês",
                yaxis_title="Temperatura (°C)",
                title={'x': 0.5, 'xanchor': 'center'},
                template="plotly_white",
                xaxis_tickangle=45
            )

            fig.show()


        
class WeatherClassifierContinuous:

    def triangular_score(self, value: float, start: float, peak: float, end: float) -> float:
        """Return a triangular score rising from ``start`` to ``peak`` and falling to ``end``."""
        if value <= start or value >= end:
            return 0.0
        if value == peak:
            return 1.0
        if value < peak:
            return (value - start) / (peak - start)
        return (end - value) / (end - peak)

    def linear_scale(
        self,
        value: float,
        ideal_min: float,
        ideal_max: float,
        extreme_min: float,
        extreme_max: float,
    ) -> float:
        """Return a linear score of ``value`` between ``extreme_min`` and ``extreme_max``.

        Values inside ``[ideal_min, ideal_max]`` return ``1`` while values at the
        extremes return ``0`` with a linear transition between these ranges.
        """
        if value <= extreme_min or value >= extreme_max:
            return 0.0
        if ideal_min <= value <= ideal_max:
            return 1.0
        if value < ideal_min:
            return (value - extreme_min) / (ideal_min - extreme_min)
        return (extreme_max - value) / (extreme_max - ideal_max)

    def score_dia_continuous(self, row) -> float:
        """Compute the daily climate score using continuous comfort curves."""
        maxtemp = row["maxtempC"]
        mintemp = row["mintempC"]
        humidity = row["humidity"]
        sun = row["sunHour"]
        precip = row["precipMM"]

        # Ideal maximum temperature peaks at 24ºC and drops linearly to 0 at 20ºC
        # and 28ºC.
        comfort_temp = self.triangular_score(maxtemp, 20, 24, 28)

        # Minimum temperature comfort decreases outside 16–20ºC with linear tails
        # reaching zero at 10ºC and 24ºC.
        comfort_mintemp = self.linear_scale(mintemp, 16, 20, 10, 24)

        # Humidity between 30% and 80% is ideal; outside this range comfort falls
        # linearly towards the extremes 0% and 100%.
        comfort_hum = self.linear_scale(humidity, 30, 80, 0, 100)

        # Days with at least five hours of sol get full points, otherwise scale
        # proportionally.
        comfort_sun = 1.0 if sun >= 5 else sun / 5.0

        # Up to 20 mm of precipitation reduces comfort linearly from 1 to 0.
        comfort_precip = 1.0 - min(max(precip, 0), 20) / 20.0

        penalizacao = 0.0
        if bool(row.get("calor_extremo", False)):
            penalizacao += 0.3
        if bool(row.get("frio_extremo", False)):
            penalizacao += 0.4
        if bool(row.get("chuva_extrema", False)):
            penalizacao += 0.2
        penalizacao = min(penalizacao, 1.0)

        score = 100 * (
            0.6 * comfort_temp
            + 0.12 * comfort_mintemp
            + 0.10 * comfort_hum
            + 0.09 * comfort_sun
            + 0.09 * comfort_precip
        )
        score -= penalizacao * 20
        return max(score, 0.0)
    
    def classifica_diario_continuous(self, df_weather):
        # Continuous score
        df_weather['score_dia_cont'] = df_weather.apply(self.score_dia_continuous, axis=1)
        df_weather['classificacao_dia_cont'] = df_weather['score_dia_cont'].apply(ScoreUtils.classifica_score)
        return df_weather
    
if __name__ == "__main__":
    weatherClassifier = WeatherClassifier()
    weatherClassifierContinuous = WeatherClassifierContinuous()
