from wwo_hist import retrieve_hist_data
import os
import unidecode
import time
import csv

api_key = "5ccf4c0bfb8849ddaf8204104250207"


class weather_py:
    def write_to_csv(self, Even_list, fileName="done.csv"):
        with open(fileName, "a", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([Even_list])

    def exportaLista(self, lst, fileName="done.csv"):
        with open(fileName, "w") as filehandle:
            for listitem in lst:
                filehandle.write("%s\n" % listitem)

    def leArquivo(self, filePath):
        lsttime = None
        with open(filePath, encoding="utf-8", errors="ignore") as f:
            lsttime = f.read().splitlines()
            lsttime = list(dict.fromkeys(lsttime))
        return lsttime

    # EXECUTA API DE CLIMA
    def set_filelocation(base_dir="..\\..\\Bases\\Clima\\worldWeatherApi\\"):
        os.chdir(base_dir)
        os.getcwd()

    def process_wheather(
        self,
        location_list,
        frequency=24,
        start_date="2021-01-01",
        end_date="2021-12-30",
        retries=10,
        fileDone="..\\Temporaria\\done.csv",
        resetDoneFile=False,
    ):
        # Apaga todos arquivos para iniciar um processo novo
        if resetDoneFile and os.path.exists(fileDone):
            os.remove(fileDone)

        # cria arquivo de saida se não existir
        # esse aruivo é usado para manter o status da atualizao
        # caso pare em alguma cidade ele é consultado para voltar a partir dessa cidade
        if not os.path.exists(fileDone):
            self.write_to_csv("", fileDone)

        lst_done = []
        retry = retries
        retry_count = 0
        while retry_count <= retry:
            for location in location_list:
                lst_done = self.leArquivo(fileDone)

                if location not in lst_done:
                    try:
                        print(location)
                        time.sleep(2)
                        hist_weather_data = retrieve_hist_data(
                            api_key,
                            [location],
                            start_date,
                            end_date,
                            frequency,
                            location_label=False,
                            export_csv=True,
                            store_df=True,
                        )
                        lst_done.append(location)
                        self.exportaLista(lst_done, fileDone)
                    except:
                        self.exportaLista(lst_done, fileDone)
                        break
            retry_count = retry_count + 1
