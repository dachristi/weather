
import math


def distance2(lat1, lon1, lat2, lon2):
    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)
    delta_lat = (lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = (math.sin(delta_lat / 2)**2 +
             math.sin(delta_lon / 2)**2 *
             math.cos(lat1) * math.cos(lat2))
    earth_radius = 6371
    c = 2 * math.asin(math.sqrt(a))
    d = earth_radius * c
    return kilometers_to_miles(d)


def distance(lat1, lon1, lat2, lon2):
    '''
    d=2*asin(sqrt((sin((lat1-lat2)/2))^2 +
                     cos(lat1)*cos(lat2)*(sin((lon1-lon2)/2))^2))
    Source: http://edwilliams.org/avform147.htm#Dist
    Referenced from: https://www.nhc.noaa.gov/gccalc.shtml
    '''

    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)
    lon1 = math.radians(lon1)
    lon2 = math.radians(lon2)
    d = 2*math.asin(math.sqrt((math.sin((lat1-lat2)/2))**2 +
                    math.cos(lat1)*math.cos(lat2)*(math.sin((lon1-lon2)/2))**2))
    earth_radius = 6371  # kilometers
    d = d * earth_radius
    return kilometers_to_miles(d)


def kilometers_to_miles(km):
    miles = km / 1.609344
    return miles
