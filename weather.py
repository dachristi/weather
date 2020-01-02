





def weighted_average_distance(stations):
    '''stations parameter is a dictionary {'station_id': 'distance'}.
    This function returns a new dictionary of wieghts {station_id: wieght}'''
    sum_of_distances = sum(stations.values())
    for key in stations.keys():
        weight = 1 - stations[key] / sum_of_distances
        stations[key] = weight
    return stations
