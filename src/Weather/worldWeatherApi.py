
# !pip install wwo-hist
# !pip install unidecode

from pathlib import Path
import pandas as pd
from wwo_hist import retrieve_hist_data
from typing import List, Iterable
import os
import time
import sys
import unicodedata
class WorldWeatherApi:
    API_KEY_LIST = [
        "a95cd7210fbf4b85a7e185308250307",
        "5ccf4c0bfb8849ddaf8204104250207",
        "9089449cfc564000aef202420250307",
        "a486a31885364323a5a211207250307",
        "3a66d0209bd94f57a76212049250307"
        ]

    API_KEY = "5ccf4c0bfb8849ddaf8204104250207"
    NOTEBBOKS_DIR = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
    BASES_DIR = Path.cwd().parent.parent / "Bases"
    WWO_Api_DIR = BASES_DIR / "Clima" / "worldWeatherApi"
    DONE_FILE  = WWO_Api_DIR /"Temporaria"  / "done.txt"
    arquivo_municipios = BASES_DIR / "Municipios" / "portugalMunicipios.csv"


    def show_paths(self):
        print("NOTEBBOKS_DIR:", self.NOTEBBOKS_DIR)
        print("WWO_Api_DIR:", self.WWO_Api_DIR)
        print("DONE_FILE:", self.DONE_FILE)
        print("BASES_DIR:", self.BASES_DIR)
        print("arquivo_municipios:", self.arquivo_municipios)


    def remover_acentos(self, texto):
        return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')   


    def get_municipios_portugal(self) -> List[str]:
        """
        Lê o arquivo de municípios de Portugal e retorna uma lista de cidades.
        O arquivo deve estar no formato CSV com colunas 'Regiao' e 'Cidades'.
        """

        if not self.arquivo_municipios.exists():
            raise FileNotFoundError(f"Arquivo de municípios não encontrado: {self.arquivo_municipios}")

        # ler o arquivo de municipios em um dataframe
        # separadando os campos por vírgula
        # monte uma lista concatenando: "Portugal," [Cidades]
        df = pd.read_csv(self.arquivo_municipios, sep=",", encoding="utf-8")

        #ordene por Regiao e cidades
        df = df.sort_values(by=["Regiao", "Cidades"])

        # monte uma lista concatenando: "Portugal," [Cidades]
        cidades = [cidade + ",Portugal" for cidade in df["Cidades"].tolist()]
        
        return cidades



    # ------------------------------------------------------------------ utilidades
    def save_lines(self, lines: Iterable[str], file: Path) -> None:
        file.write_text("\n".join(lines), encoding="utf-8")

    def read_lines(self, file: Path) -> List[str]:
        return file.read_text(encoding="utf-8").splitlines() if file.exists() else []

    # ------------------------------------------------------------------ download
    def fetch_history(self, city: str, start: str, end: str, freq: int = 24, pause: float = 2):
        """Baixa os dados da cidade e grava o CSV DENTRO de BASE_DIR."""
        time.sleep(pause)

        cwd = Path.cwd()            # guarda onde eu estava
        try:
            os.chdir(self.WWO_Api_DIR)      # muda só durante o download
            retrieve_hist_data(
                self.API_KEY, [city], start, end, freq,
                location_label=False, export_csv=True, store_df=True
            )
        finally:
            os.chdir(cwd)           # volta ao normal

# ------------------------------------------------------------------ orquestra
    def process_weather(self,
        locations: List[str],
        start_date="2025-06-25",
        end_date="2025-07-01",
        freq=24,
        retries=3,
    ):
        done = set(self.read_lines(self.DONE_FILE))

        for city in locations:
            if city in done:
                continue

            for attempt in range(1, retries + 1):
                try:
                    self.fetch_history(city, start_date, end_date, freq)
                    done.add(city)
                    self.save_lines(sorted(done), self.DONE_FILE)
                    break
                except Exception as err:
                    print(f"Falhou {city} ({attempt}/{retries}): {err}")
                    # Verifica se a mensagem do erro tem "Too Many Requests"
                    if "429" in str(err) or "Too Many Requests" in str(err):
                        print("Erro 429: Requisições em excesso. Encerrando o processo.")
                        sys.exit(1)  # ou raise err, se preferir deixar o erro estourar
                    if attempt == retries:
                        print("Desisti dessa cidade.")

    def test_download(self, city="Lisboa", start_date="2025-01-01", end_date="2025-01-05", freq=24):
        print(f"Processando clima Cidade: {city}")
        self.fetch_history(city, start_date, end_date, freq)

if __name__ == "__main__":
    weatherApi = WorldWeatherApi()
    municipios_portugal = weatherApi.get_municipios_portugal()

    # substua espacos por "+"
    municipios_portugal = [cidade.replace(" ", "+") for cidade in municipios_portugal]
    municipios_portugal = [weatherApi.remover_acentos(cidade) for cidade in municipios_portugal]

    weatherApi.process_weather(
        municipios_portugal,
        start_date="2024-06-30",
        end_date="2025-06-30",
        freq=24,
        retries=3)
    

    
