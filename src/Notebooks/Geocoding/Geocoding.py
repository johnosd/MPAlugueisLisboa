#!/usr/bin/env python
# coding: utf-8

# In[131]:


#!pip install googlemaps
#!pip install geopy
#!pip install --upgrade google-auth-oauthlib
#!pip install simplejson


# In[153]:


import googlemaps
from geopy.geocoders import Nominatim
from geopy import distance
API_key = 'AIzaSyCKIJruHmEjKm4O7SniE9GnExV-unspPYQ'

class geo_py:  
    
    def geopy_geocode(self, df_city):
        # Initialize Nominatim API
        geolocator = Nominatim(user_agent="MyApp")

        lisboa_location = geolocator.geocode('Portugal, lisboa')
        lisboa_ri = (lisboa_location.latitude, lisboa_location.longitude)

        porto_location = geolocator.geocode('Portugal, porto')
        porto_ri = (porto_location.latitude, porto_location.longitude)

        for index, row in df_city.iterrows():
            city = 'Portugal, '+ row['Cidades']
            location = geolocator.geocode(city)

            if location is not None:
                # Calcula distancias
                city_ri = (location.latitude, location.longitude)
                distance_lisboa = distance.distance(lisboa_ri, city_ri).km
                distance_porto = distance.distance(porto_ri, city_ri).km

                # Salva no DF
                df_city.at[index,['Latitude','Longitude','Address','Distance_Lisboa','Distance_Porto']] = (location.latitude, location.longitude,location.address,distance_lisboa, distance_porto)

            else:
                print('ERRO: ' + city)

class google_maps:
    

    def googlemaps_city_distance_from_city(self, df_city,_mode='driving', city_origin_ = ['lisboa, Portugal','porto, Portugal']):
        # Initialize googlemaps API
        gmaps = googlemaps.Client(key= API_key)
        
        for city_origin in city_origin_:
            for index, row in df_city.iterrows():
                column_name=''
                city_destination = row['Cidades'] + ', Portugal'
                region_destination = row['Regiao'] + ', Portugal'
                distance = 'None'
                json_response = gmaps.distance_matrix(city_origin, city_destination, mode=_mode)
                
                status = json_response["rows"][0]["elements"][0]["status"]
                
                if status != 'OK':
                    json_response = gmaps.distance_matrix(region_destination, city_destination, mode=_mode)
                    status = json_response["rows"][0]["elements"][0]["status"]
                    city_destination = region_destination

                if status == 'OK':   
                    duration = json_response["rows"][0]["elements"][0]["duration"]["value"]

                    if duration is not None:
                        duration_hours = round(duration/3600,1)
                        # Salva no DF
                        column_name = 'duration_hours_'+_mode +'_'+ city_origin
                        df_city.at[index,[column_name]] = duration_hours



                    distance = json_response["rows"][0]["elements"][0]["distance"]["value"]
                    if distance is not None:
                        distance = round(distance/1000,1)
                        column_name = 'distance_driving' +_mode +'_'+ city_origin
                        df_city.at[index,[column_name]] = distance

                print('Origin: {} x Destination: {} = distance ={}'.format(city_origin,city_destination,distance))
                        
    def googlemaps_get_distance(self, origin = 'Lisboa, Portugal',destination= 'Cascais, Portugal', _mode = 'driving'):
        gmaps = googlemaps.Client(key= API_key)
        result = gmaps.distance_matrix(origin, destination, mode=_mode)
        #print('Origin: {}'.format(origin))
        #print('Destination: {}'.format(destination))
        #print(result)
        return result



