#!/usr/bin/env python
# coding: utf-8

#!pip install googlemaps
#!pip install geopy
#!pip install --upgrade google-auth-oauthlib
#!pip install simplejson


import googlemaps
from geopy.geocoders import Nominatim
from geopy import distance

API_key = "AIzaSyASKLT8a2NTzcp5Wfl9EBvMZYgJAE-_HXs"


class geo_py:
    def geopy_geocode(self, df):
        # Initialize Nominatim API
        geolocator = Nominatim(user_agent="MyApp")
        print("-- GEO_PY - Geocoding Started -----------------")
        for index, row in df.iterrows():
            city = "Portugal, " + row["Cidades"]
            location = geolocator.geocode(city)

            if location is not None:
                # Salva no DF
                df.loc[index, "Latitude"] = location.latitude
                df.loc[index, "Longitude"] = location.longitude
                df.loc[index, "Address"] = location.address
            else:
                print("ERRO: " + city)
        print("-- GEO_PY - Geocoding Ended -----------------")

    def geopy_distance(self, df, cidades):
        geolocator = Nominatim(user_agent="MyApp")

        origens = []
        for cidade in cidades:
            origens.append(geolocator.geocode(cidade))

        # Loop pelos registros do DataFrame
        for index, row in df.iterrows():
            for Cidade, loc_origen in origens:
                try:
                    # Obtém as coordenadas do registro atual
                    loc_destino = (
                        round(row["Latitude"], 6),
                        round(row["Longitude"], 6),
                    )
                    distancia_km = distance.distance(loc_origen, loc_destino).km
                    distancia_km = round(distancia_km, 1)
                    column_name = "distance_driving_" + Cidade.replace(", ", "_")
                    df.loc[index, [column_name]] = distancia_km

                except ValueError as error:
                    print(
                        f"There is something wrong with this location {row['Cidades']}"
                    )
                    print(f"The following error occurred: {error}")


class google_maps:
    def googlemaps_city_distance_from_city(
        self,
        df_municipios,
        _mode="driving",
        city_origin_=["lisboa, Portugal", "porto, Portugal"],
    ):
        # Initialize googlemaps API
        gmaps = googlemaps.Client(key=API_key)

        for city_origin in city_origin_:
            for index, row in df_municipios.iterrows():
                column_name = ""
                city_destination = row["Cidades"] + ", Portugal"
                region_destination = row["Regiao"] + ", Portugal"
                distance = "None"
                json_response = gmaps.distance_matrix(
                    city_origin, city_destination, mode=_mode
                )

                status = json_response["rows"][0]["elements"][0]["status"]
                print(status)
                if status != "OK":
                    json_response = gmaps.distance_matrix(
                        region_destination, city_destination, mode=_mode
                    )
                    status = json_response["rows"][0]["elements"][0]["status"]
                    city_destination = region_destination

                if status == "OK":
                    duration = json_response["rows"][0]["elements"][0]["duration"][
                        "value"
                    ]

                    if duration is not None:
                        duration_hours = round(duration / 3600, 1)
                        # Salva no DF
                        column_name = "duration_hours_" + _mode + "_" + city_origin
                        # df_municipios.loc[index, column_name] = distance
                        df_municipios.at[index, [column_name]] = duration_hours

                    distance = json_response["rows"][0]["elements"][0]["distance"][
                        "value"
                    ]
                    if distance is not None:
                        distance = round(distance / 1000, 1)
                        column_name = "distance_driving" + _mode + "_" + city_origin
                        df_municipios.at[index, [column_name]] = distance

                print(
                    "Origin: {} x Destination: {} = distance ={}".format(
                        city_origin, city_destination, distance
                    )
                )

    def googlemaps_get_distance(
        self,
        origin="Lisboa, Portugal",
        destination="Cascais, Portugal",
        _mode="driving",
    ):
        gmaps = googlemaps.Client(key=API_key)
        result = gmaps.distance_matrix(origin, destination, mode=_mode)
        # print('Origin: {}'.format(origin))
        # print('Destination: {}'.format(destination))
        # print(result)
        return result
