
#!pip install googlemaps

import googlemaps
import requests
import pandas as pd
import re
from datetime import timedelta

class Geocoding:
    filePathMunicipios = '..\\..\\Bases\\Municipios\\' + 'portugalMunicipios' + '.csv'
    filePathMunicipiosGeo = '..\\..\\Bases\\Geocoding\\' + 'portugalMunicipiosGeo' + '.csv'
    filepathMunicipiosDistance = '..\\..\\Bases\\Geocoding\\' + 'portugalMunicipiosGeoDistances' + '.csv'
    
    google_api_key ='AIzaSyCFY5VAs9nTOkrKlVmZy2hGMTA_XX-4R5Q'

    # get country from json
    def get_country_from_json(self,json_data):
        if json_data and 'address_components' in json_data[0]:
            for component in json_data[0]['address_components']:
                if 'country' in component['types']:
                    return component['long_name']
        return None

    # get administrative_area_level_1 long name
    def get_administrative_area_level_1(self,json_data):
        if json_data and 'address_components' in json_data[0]:
            for component in json_data[0]['address_components']:
                if 'administrative_area_level_1' in component['types']:
                    return component['long_name']
        return None


    # get administrative_area_level_2 long name
    def get_administrative_area_level_2(self,json_data):
        if json_data and 'address_components' in json_data[0]:
            for component in json_data[0]['address_components']:
                if 'administrative_area_level_2' in component['types']:
                    return component['long_name']
        return None

    # get formatted address
    def get_formatted_address(self,json_data):
        if json_data and 'formatted_address' in json_data[0]:
            return json_data[0]['formatted_address']
        return None


    # get place_id  
    def get_place_id(self,json_data):
        if json_data and 'place_id' in json_data[0]:
            return json_data[0]['place_id']
        return None

    def get_lat_lon(self,address, api_key):
        # Inicializando o cliente do Google Maps com a chave da API
        gmaps = googlemaps.Client(key=api_key)
        
        # Fazendo a requisição para obter informações sobre o endereço
        geocode_result = gmaps.geocode(address)
        
        # Verificando se a requisição retornou algum resultado
        if geocode_result:
            # Pega a latitude e longitude do primeiro resultado
            place_id = self.get_place_id(geocode_result)
            country = self.get_country_from_json(geocode_result)
            administrative_area_level_1 = self.get_administrative_area_level_1(geocode_result)
            administrative_area_level_2 = self.get_administrative_area_level_2(geocode_result)
            formatted_address = self.get_formatted_address(geocode_result)
            latitude = geocode_result[0]["geometry"]["location"]["lat"]
            longitude = geocode_result[0]["geometry"]["location"]["lng"]
            return latitude, longitude, geocode_result
        else:
            return "Endereço não encontrado!"


    def get_distance_and_time(self, city1, city2, api_key):
        # URL base para a API Directions
        base_url = "https://maps.googleapis.com/maps/api/directions/json"
        
        # Parâmetros para a requisição de carro (driving)
        params_driving = {
            "origin": city1,
            "destination": city2,
            "mode": "driving",  # Modo de transporte para carro
            "key": api_key
        }
        
        # Parâmetros para a requisição de transporte público (transit)
        params_transit = {
            "origin": city1,
            "destination": city2,
            "mode": "transit",  # Modo de transporte público
            "key": api_key
        }
        
        # Fazendo a requisição para obter informações de carro
        response_driving = requests.get(base_url, params=params_driving)
        # Fazendo a requisição para obter informações de transporte público
        response_transit = requests.get(base_url, params=params_transit)
        
        # Verificando se as requisições foram bem-sucedidas
        if response_driving.status_code == 200 and response_transit.status_code == 200:
            data_driving = response_driving.json()
            data_transit = response_transit.json()

            # Verificando se há resultados para a rota de carro
            if data_driving["status"] == "OK":
                driving_distance = data_driving["routes"][0]["legs"][0]["distance"]["text"]
                driving_duration = data_driving["routes"][0]["legs"][0]["duration"]["text"]
            else:
                driving_distance = "Não disponível"
                driving_duration = "Não disponível"
            
            # Verificando se há resultados para a rota de transporte público
            if data_transit["status"] == "OK":
                transit_distance = data_transit["routes"][0]["legs"][0]["distance"]["text"]
                transit_duration = data_transit["routes"][0]["legs"][0]["duration"]["text"]
            else:
                transit_distance = "Não disponível"
                transit_duration = "Não disponível"
            
            return {
                "driving": {
                    "distance": driving_distance,
                    "duration": driving_duration
                },
                "transit": {
                    "distance": transit_distance,
                    "duration": transit_duration
                }
            }
        else:
            return "Erro nas requisições: Verifique a chave de API ou a conectividade."
        
    # Função para converter o texto "1 hour 10 mins" em timestamp (em segundos)
    def convert_time_to_timestamp(self,time_str):
        hours = 0
        minutes = 0
        
        # Procurar horas e minutos na string usando regex
        hours_match = re.search(r"(\d+)\s*hour", time_str)
        minutes_match = re.search(r"(\d+)\s*min", time_str)

        # Se houver correspondência para horas
        if hours_match:
            hours = int(hours_match.group(1))

        # Se houver correspondência para minutos
        if minutes_match:
            minutes = int(minutes_match.group(1))

        # Convertendo para segundos (1 hora = 3600 segundos, 1 minuto = 60 segundos)
        total_seconds = (hours * 3600) + (minutes * 60)
        
        # Retornando o timestamp como timedelta (tempo total)
        return timedelta(seconds=total_seconds)


    def remove_km_from_distance(self,distance_str):
        if isinstance(distance_str, str):
            # Remove " km" from the string
            return distance_str.replace(" km", "")
        return distance_str
    
    def geocode_municipios_file(self):
        df = pd.read_csv(self.filePathMunicipios, sep=',', encoding='utf-8', index_col=0)

    # itere over the dataframe and get lat, lon for each address
        for index, row in df.iterrows():
            address = row['Cidades'] + ', ' +'Portugal'
            print(f"Processing address: {address}")
        # Call the function to get latitude and longitude
            lat, lon, geocode_result = self.get_lat_lon(address, self.google_api_key)
            df.at[index, 'latitude'] = lat
            df.at[index, 'longitude'] = lon
            df.at[index, 'place_id'] = self.get_place_id(geocode_result)
            df.at[index, 'country'] = self.get_country_from_json(geocode_result)
            df.at[index, 'administrative_area_level_1'] = self.get_administrative_area_level_1(geocode_result)
            df.at[index, 'administrative_area_level_2'] = self.get_administrative_area_level_2(geocode_result)
            df.at[index, 'formatted_address'] = self.get_formatted_address(geocode_result)
        return df

    def get_distance_by_cities(self,df, cities_to_compare = ['Porto', 'Lisboa']):
        for index, row in df.iterrows():
            city1 = row['Cidades'] + ', ' +'Portugal'
            for city in cities_to_compare:
            # Check if the columns for this city already exist, if not create them
                if f'{city}_distance' not in df.columns:
                    df[f'{city}_distance'] = None
                if f'{city}_duration' not in df.columns:
                    df[f'{city}_duration'] = None

            # Check if the distance and duration for this city are already processed
                if df.at[index, f'{city}_distance'] is None and df.at[index, f'{city}_duration'] is None:
                    city2 = city + ', ' + 'Portugal'
                    try:
                        print(f"Processing distance from {city1} to {city2}")
                        results  = self.get_distance_and_time(city1, city2, self.google_api_key)
                        df.at[index, f'{city}_distance'] = results['driving']['distance']
                        df.at[index, f'{city}_duration'] = results['driving']['duration']
                        df.at[index, f'{city}_transit_distance'] = results['transit']['distance']
                        df.at[index, f'{city}_transit_duration'] = results['transit']['duration']
                    except Exception as e:
                        print(f"Error processing distance from {city1} to {city2}: {e}")
                        df.at[index, f'{city}_distance'] = None
                        df.at[index, f'{city}_duration'] = None
                        df.at[index, f'{city}_transit_distance'] = None
                        df.at[index, f'{city}_transit_duration'] = None

        ## FORMAT COLUMNS
        for column in df.columns:
            if 'distance' in column:
                df[column] = df[column].apply(lambda x: self.remove_km_from_distance(x) if isinstance(x, str) else x)

        # format the duration columns 1 hour 10 mins to hh:mm:ss convert_time_to_timestamp
        for column in df.columns:
            if 'duration' in column:
                df[column] = df[column].apply(lambda x: self.convert_time_to_timestamp(x) if isinstance(x, str) else x)

        for column in df.columns:
            if 'duration' in column:
                df[column] = df[column].apply(lambda x: str(x).split(' ')[-1] if isinstance(x, timedelta) else x)

        return df
    
    def show_porto_cities_by_driving_distance(self, df, hoursDriving =1.5):
        print("Cidades com tempo de viagem para o Porto menor que 1 hora (ordenadas do menor para o maior):")

        filtered_df = df[
        (df['Porto_duration'].apply(lambda x: isinstance(x, str) and x != 'Não disponível')) &
        (df['Porto_distance'].apply(lambda x: isinstance(x, str) and x != 'Não disponível'))
    ]

        filtered_df = filtered_df[filtered_df['Porto_duration'].apply(lambda x: pd.to_timedelta(x) < timedelta(hours=hoursDriving))]
        filtered_df = filtered_df.sort_values(by='Porto_duration', ascending=True)

        for index, row in filtered_df.iterrows():
            print(f"{row['Cidades']}: {row['Porto_duration']} ({row['Porto_distance']})")

    def show_lisboa_cities_by_driving_distance(self,df, hoursDriving =1.5):
        print("Cidades com tempo de viagem para o Lisboa menor que 1 hora (ordenadas do menor para o maior):")
        filtered_df = df[
        (df['Lisboa_duration'].apply(lambda x: isinstance(x, str) and x != 'Não disponível')) &
        (df['Lisboa_distance'].apply(lambda x: isinstance(x, str) and x != 'Não disponível'))
    ]
        filtered_df = filtered_df[filtered_df['Lisboa_duration'].apply(lambda x: pd.to_timedelta(x) < timedelta(hours=hoursDriving))]
        filtered_df = filtered_df.sort_values(by='Lisboa_duration', ascending=True)
        for index, row in filtered_df.iterrows():
            print(f"{row['Cidades']}: {row['Lisboa_duration']} ({row['Lisboa_distance']})")

if __name__ == "__main__":
    # %% GEOCODING----------------------------------------------------------------------
    geocoding = Geocoding()

    df = geocoding.geocode_municipios_file()

    # save the dataframe to csv
    df.to_csv(geocoding.filePathMunicipiosGeo, sep=',', encoding='utf-8')


    # %% GET DISTANCE BY CITIES ----------------------------------------------------------------------
    df = pd.read_csv(geocoding.filePathMunicipiosGeo, sep=',', encoding='utf-8', index_col=0)
    df = geocoding.get_distance_by_cities(df)
    df.to_csv(geocoding.filepathMunicipiosDistance, sep=',', encoding='utf-8')


    #%% # CIDADES PROXIMA AO PORTO TEMPO DE CARRO ATÉ 1h ----------------------------------------------------------------------
    geocoding.show_porto_cities_by_driving_distance(df)


    # # Cidades proximoas de lisboa até 1 hora ----------------------------------------------------------------------
    geocoding.show_lisboa_cities_by_driving_distance(df)