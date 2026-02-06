
# Modelo preditivo de Alugueis na Região Metropolitana de Lisboa

### O objetivo do trabalho é predizer o valor do aluguel de acordo com a região, quantidade de quartos e metros quadrados.

### A predição será realizada utilizando uma base extraída do site Idealista.com, modelos estatísticos e algoritmos de Inteligência Artificial, que selecionarão as características mais relevantes que explicam o evento em questão.

### Desta formar será possível identificar o melhor as regiões com valores ideais de locação, bem como saber se o valor está na media praticada na região.

### Como próximo passo, este trabalho pode incluir uma analise do perfil do arrendador afim de realizar recomendações da local ideal, baseado no clima, comércios locais, hospitais, faixa etária e outras variáveis![image](https://github.com/johnosd/MPAlugueisLisboa/assets/22200130/578869a3-c55b-48f4-af9a-b7ce287fa8c7)


## Como executar esse processo

<details>
  <summary>0. Atualize os dados de Imoveis do Idealista</summary>
    Você precisa rodar o scraping que obtem dados do idealista para fins de estudos
    esses dados devem ser salvos na pasta: 
    ```Base/idealista/```
</details>

<details>
  <summary>1. Obter dados de Clima</summary>

## 1. Obter dados de Clima
src/Weather/worldWeatherApi.py

Este arquivo define a classe WorldWeatherApi, que automatiza o download de dados históricos de clima para cidades de Portugal usando a biblioteca wwo_hist (World Weather Online).

Inicie obtendo dados de clima dos municipios de portugal
use este arquivo que contém a lista de municipios de portugal: 
- BASES_DIR / "Municipios" / "portugalMunicipios.csv"

O que o arquivo faz
Lê uma lista de municípios de Portugal a partir de um arquivo CSV.
Baixa dados históricos de clima para cada cidade, salvando os resultados em arquivos CSV.

**Como usar para obter dados de clima**
Instale as dependências:
```pip install -r src/requirements.txt```
Prepare o arquivo de municípios:

Certifique-se de que o arquivo portugalMunicipios.csv está no caminho esperado.
Execute o script:

No terminal, rode:
```python src/Weather/worldWeatherApi.py```

Isso irá baixar os dados de clima para todas as cidades listadas, salvando os CSVs na pasta configurada.
Personalize datas ou cidades:


Os dados são salvos na pasta:
/Bases/Clima/worldWeatherApi.

**Obs**
O arquivo done.txt evita downloads duplicados.
</details>


<details>
  <summary>2. Geolocalizar municipios</summary>
src/Weather/geocoding.py

O arquivo geocoding.py normalmente serve para converter nomes de cidades ou endereços em coordenadas geográficas (latitude e longitude), processo chamado de geocodificação. Isso é útil para análises espaciais, mapas ou para integrar dados de localização em modelos de previsão.

**O que o arquivo faz**
- Recebe uma lista de cidades ou endereços (geralmente de um arquivo CSV).
- Consulta um serviço de geocodificação (como Google Maps, OpenStreetMap, etc.) para obter latitude e longitude de cada local.
- Salva os resultados em um novo arquivo CSV, geralmente incluindo as colunas de latitude e longitude junto com o nome da cidade/endereço.
- Pode atualizar um arquivo existente adicionando as coordenadas às linhas correspondentes.

**Como usar**
- Coloque a lista de cidades/endereços em um arquivo CSV no local esperado pelo script (verifique o caminho e o nome do arquivo no código).
- Execute o script:
```python src/Weather/geocoding.py```

- Aguarde o processamento. O script irá consultar o serviço de geocodificação para cada cidade/endereço.

Arquivos gerados ou atualizados

- Arquivo de saída: normalmente um CSV chamado algo como municipios_geocoded.csv ou similar, contendo as cidades e suas coordenadas.

- Arquivo de entrada: o script pode ler e atualizar o arquivo original de municípios, adicionando as colunas de latitude e longitude.

**Resumo**
O geocoding.py automatiza a obtenção de coordenadas geográficas para cidades/endereços, salvando os resultados em um arquivo CSV para uso posterior em análises ou visualizações. Basta preparar o arquivo de entrada e executar o script; o arquivo de saída será gerado ou atualizado automaticamente.

</details>

<details>
  <summary>3. Obter dados da Wikipedia</summary>
src/wikipedia/wikipediaApi.ipynb

O notebook `wikipediaApi.ipynb` automatiza a coleta de informações de cidades portuguesas a partir da Wikipedia. Ele pode ser usado para enriquecer a base de dados com descrições, histórico, dados demográficos e outros textos relevantes para cada município.

**O que o notebook faz**
- Lê uma lista de cidades de Portugal (normalmente de um arquivo CSV).
- Para cada cidade, consulta a Wikipedia e extrai informações textuais.
- Salva os textos extraídos em arquivos `.txt` ou `.csv` para uso posterior em análises, visualizações ou modelos.

**Como usar**
- Certifique-se de que a lista de cidades está disponível (por exemplo, `portugalMunicipios.csv`).
- Abra o notebook em VS Code ou Jupyter.
- Execute as células sequencialmente.
- O notebook irá buscar e salvar os textos da Wikipedia para cada cidade.

**Entradas**
- Arquivo CSV com a lista de cidades (ex: `Bases/Municipios/portugalMunicipios.csv`).

**Saídas**
- Arquivos `.txt` ou `.csv` contendo os textos extraídos da Wikipedia, geralmente salvos em `Bases/Wikipedia/`.

**Resumo**
O notebook `wikipediaApi.ipynb` automatiza a coleta de informações da Wikipedia para cidades portuguesas, gerando arquivos de texto para cada município. Basta preparar a lista de cidades, abrir o notebook e executar as células; os arquivos de saída serão gerados automaticamente.

</details>

<details>
  <summary>4. Obter dados do Pordata</summary>
src/PordataPt/1-PorDataDownload.ipynb

O notebook `1-PorDataDownload.ipynb` automatiza o download e processamento de dados socioeconômicos do portal Pordata, que reúne estatísticas sobre municípios portugueses. Esses dados podem ser usados para enriquecer a análise preditiva de aluguel, incluindo variáveis como população, educação, saúde, economia, entre outras.

**Local do arquivo**
- `src/PordataPt/1-PorDataDownload.ipynb`

**O que o notebook faz**
- Lê uma lista de municípios ou temas de interesse.
- Baixa dados do portal Pordata para cada município ou tema.
- Processa e organiza os dados em tabelas para uso em análises ou modelos.
- Salva os dados em arquivos `.csv` ou `.xlsx` para uso posterior.

**Entradas**
- Arquivo CSV com a lista de municípios ou temas (ex: `Bases/Municipios/portugalMunicipios.csv` ou arquivos de temas em `Bases/PorData/`).

**Saídas**
- Arquivos `.csv` ou `.xlsx` contendo os dados baixados e processados, geralmente salvos em `Bases/PorData/base_agregada/` ou similar.

**Como executar**
- Certifique-se de que a lista de municípios ou temas está disponível.
- Abra o notebook em VS Code ou Jupyter.
- Execute as células sequencialmente.
- Os arquivos de saída serão gerados automaticamente nas pastas configuradas.

**Resumo**
O notebook `1-PorDataDownload.ipynb` automatiza o download e processamento de dados do Pordata para municípios portugueses, gerando arquivos de dados para uso em análises e modelos. Basta preparar a lista de municípios ou temas, abrir o notebook e executar as células; os arquivos de saída serão salvos automaticamente.

</details>

<details>
  <summary>5. Processar e Agregar Dados do Pordata</summary>
src/PordataPt/porDataProcess.py

O arquivo `porDataProcess.py` realiza o processamento, limpeza, transformação e agregação dos dados socioeconômicos baixados do Pordata, organizando-os em uma base única e estruturada para análise e modelagem.

**Local do arquivo**
- `src/PordataPt/porDataProcess.py`

**O que o arquivo faz**
- Lê múltiplos arquivos CSV do Pordata (população, economia, saúde, moradia, educação, segurança, etc.).
- Realiza tratamento, limpeza e padronização dos dados de cada tema.
- Agrega e cruza as informações por município/região.
- Gera uma base final consolidada, pronta para análise ou uso em modelos preditivos.

**Entradas**
- Arquivos CSV de dados brutos do Pordata, organizados em subpastas de `Bases/PorData/`.
- Um arquivo JSON (`pordataFiles.json`) com a lista dos arquivos a serem processados.

**Saídas**
- Um arquivo CSV consolidado: `Bases/PorData/base_agregada/estatisticas_agregadas.csv`, contendo todas as variáveis tratadas e agregadas por município.

**Como executar**
- Certifique-se de que os arquivos CSV do Pordata e o arquivo `pordataFiles.json` estão nas pastas corretas.
- No terminal, execute:
  ```bash
  python src/PordataPt/porDataProcess.py
  ```
- O arquivo de saída será gerado automaticamente na pasta indicada.

**Resumo**
O script `porDataProcess.py` automatiza o tratamento e a integração dos dados do Pordata, gerando uma base única e limpa para análise de municípios portugueses. Basta garantir os arquivos de entrada, executar o script e utilizar o CSV gerado para análises ou modelos.

</details>

<details>
  <summary>6. Processar Dados do Idealista</summary>
src/idealista/idealistaProcessData.py

O arquivo `idealistaProcessData.py` automatiza o processamento dos dados de imóveis extraídos do site Idealista, organizando e limpando as informações para análise e geração de bases agregadas por município.

**Local do arquivo**
- `src/idealista/idealistaProcessData.py`

**O que o arquivo faz**
- Lê todos os arquivos JSON de imóveis baixados do Idealista.
- Converte os dados em um DataFrame, limpa e padroniza variáveis (preço, área, quartos, tags, etc.).
- Marca outliers e categoriza imóveis.
- Gera uma base detalhada de imóveis e uma base agregada por município, com estatísticas de preço, área e quantidade de imóveis por tipologia.

**Entradas**
- Arquivos JSON de imóveis, localizados em `Bases/idealista/202508/`.

**Saídas**
- Base detalhada: `Bases/idealista/base_processada/base_imoveis.csv`
- Base agregada por município: `Bases/idealista/base_processada/base_imoveis_municipio.csv`

**Como executar**
- Certifique-se de que os arquivos JSON estão na pasta correta.
- No terminal, execute:
  ```bash
  python src/idealista/idealistaProcessData.py
  ```
- As bases processadas serão geradas automaticamente nas pastas indicadas.

**Resumo**
O script `idealistaProcessData.py` automatiza o tratamento e agregação dos dados de imóveis do Idealista, gerando bases limpas e estatísticas por município para análise e modelagem. Basta garantir os arquivos de entrada, executar o script e utilizar os CSVs gerados.

</details>

<details>
  <summary>7. Rankings</summary>
Atualmente, o código e notebooks do projeto criam rankings principalmente relacionados a imóveis e municípios. Com base na estrutura dos arquivos e nomes dos notebooks, os rankings identificados são:

- **Rankings criados pelo projeto**
    - **Ranking de municípios por preço de aluguel**
        - Calculado nos notebooks como 4 - Calcula Ranking.ipynb e nos scripts de processamento do Idealista.
        - Ordena municípios/regiões pelo valor médio do aluguel.

    - **Ranking de municípios por preço por metro quadrado**
        - Baseado nos dados processados do Idealista, compara o preço por área.
    
    - **Ranking de municípios por quantidade de imóveis disponíveis**
         - Agregado nos scripts e notebooks, mostra onde há mais oferta de imóveis.
    
    **Ranking de municípios por tipologia (quantidade de quartos)**
        - Classifica municípios conforme a distribuição de imóveis por número de quartos.
    
    **Ranking de municípios por variáveis socioeconômicas (Pordata)**
        - Pode incluir rankings por população, densidade populacional, segurança, saúde, cultura, etc., conforme os dados agregados em porDataProcess.py e notebooks do Pordata.
    **Ranking de municípios por clima**
        - Possível nos notebooks e scripts de clima, ordenando municípios por variáveis climáticas (temperatura, precipitação, etc.).
    
    **Resumo**
    Os rankings atualmente criados são:

    - Preço médio de aluguel por município
    - Preço por metro quadrado por município
    - Quantidade de imóveis por município
    - Distribuição por tipologia (quartos)
    - Rankings por variáveis socioeconômicas (população, segurança, saúde, cultura)
    - Rankings por clima
</details>
___
<details>
  <summary>8. Bases </summary>

# Principais bases finais geradas

Esta seção lista as principais bases finais processadas por cada etapa do projeto, que são usadas para criar a base final unificada.

---

## Bases finais de cada processo
### 0. Municipiod
- **Bases/Municipios/portugalMunicipios.csv**
    - Base inicial contendo os municipios de portugal


### 1. Idealista (Imóveis)
- **Bases/idealista/base_processada/base_imoveis_municipio.csv**
    - base processada de imoveis do site idealista
    - #TODO: criar um ranking Geral e por municipio, usar metro quadrado como referencia

### 2. Clima
- **Bases/Clima/worldWeatherApi/ranking/ranking_base.csv**
    - Para cada dia e município, é calculado um score climático diário (score_dia) baseado em:
        - Temperatura máxima e mínima
        - Umidade
        - Horas de sol
        - Precipitação
        - Penalizações para eventos extremos (calor, frio, chuva)
    - Classificação:
        Cada dia recebe uma classificação (classificacao_dia) como "Excelente", "Muito Bom", "Bom", "Aceitável" ou "Marginal" de acordo com o score.
    - Saída:
        Um ranking detalhado por dia, município e região, com scores e classificações.

- **Bases/Clima/worldWeatherApi/ranking/ranking_Mensal.csv**
    
    Ranking mensal de clima por município.
    - Os dados diários são agregados por mês, município e estação do ano. São calculadas:
        - Média dos scores diários do mês
        - Quantidade de dias bons, aceitáveis, marginais e extremos
        - Médias de temperatura, umidade, precipitação, etc.
    - Classificação:
        Cada mês recebe uma classificação climática baseada na média dos scores diários.
    - Saída:
        Um ranking mensal para cada município, indicando a qualidade climática de cada mês.


- **Bases/Clima/worldWeatherApi/ranking/ranking_Geral.csv**
    
    Ranking geral (anual) de clima por município.
    
    - Os dados diários são agregados ao longo de todo o período disponível para cada município. São calculadas:
        - Média dos scores diários no período
        - Percentual de dias extremos
        - Médias de temperatura, umidade, precipitação, etc.
    - Classificação:
        Cada município recebe uma classificação geral baseada na média dos scores diários.
    - Saída:
        Um ranking geral dos municípios com melhor conforto climático ao longo do ano.


- **Bases/Clima/worldWeatherApi/ranking/ranking_TCI.csv**

    Ranking baseado no Tourism Climate Index (TCI), um índice internacional de conforto climático para turismo.

    - Para cada município e mês, o TCI é calculado considerando:
        - Temperatura média e máxima
        - Umidade relativa
        - Precipitação total
        - Horas de sol
        - Velocidade do vento
    
        O TCI mensal é agregado para obter o TCI anual de cada município.
    
    - Classificação:
    Os municípios são ranqueados pelo valor anual do TCI, indicando os melhores climas para turismo.
    
    - Saída:
    Um ranking dos municípios com maior conforto climático para turismo.

### 3. Geocoding
- Bases/Geocoding/portugalMunicipiosGeo.csv
- Bases/Geocoding/portugalMunicipiosGeoDistances.csv

### 4. Pordata (Socioeconômico)
- Bases/PorData/base_agregada/base_agregada.csv

### 5. Wikipedia
- Bases/Wikipedia/[municipio].txt

### 6. Bases Unificadas
- Bases/Base_fato_unificada.csv
- Bases/Imoveis/BaseFinal.csv
- Bases/Imoveis/BaseFinalv2.csv
- Bases/df_municipios_ranking_geral.csv
- Bases/Municipios_geo_clima.csv

---

Essas bases finais são usadas para criar a base final unificada do projeto, reunindo dados de imóveis, clima, geolocalização, indicadores socioeconômicos e descrições dos municípios.
</details>
