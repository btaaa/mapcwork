## This peice of the code does the routing between pais of OD for two types of trips:
## 1. Trips that have a zero distance repoerted by lime which are longer than 100(m) and 1(minute) 
## 2. Trips that have jump_ratio>= 0.6, which means 60% of the trip consists of jumps longer than 300 meters

import pandas, json, requests
from geopy.distance import great_circle


def od_extractor(route): ## This function returns origin/destination coordinates

    route = json.loads(route.replace("\'", "\""))['features']
    lon_o,lat_o = route[0]['geometry']['coordinates']
    lon_d, lat_d  = route[-1]['geometry']['coordinates']
    return lon_o, lat_o, lon_d, lat_d



## Getting the ones that are zero-distance and longer than one minute and shoter than 5 hours:

def zero_dist_trips(df):

	df_0 = df[(df['trip_distance']==0) & (df['trip_duration']<18000) & (df['trip_duration']>59)]
	print(len(df_0))

	result = df_0.apply(lambda row: od_extractor(row['route']), axis=1 )
	df_0['lon_o'], df_0['lat_o'], df_0['lon_d'], df_0['lat_d'] = zip(*result)
	df_0['trip_dist_cal'] = df_0.apply(lambda row: great_circle((row['lat_o'], row['lon_o']), (row['lat_d'], row['lon_d'])).meters, axis=1)
	df_0 = df_0.loc[(df_0['trip_dist_cal']>100) & (df_0['trip_dist_cal']<20000)]
	return df_0


def routing(lat, lon):   ## Requesting for getting route using mapcrider2

	resj = json.loads(requests.get(
		
		f'http://10.10.10.249:8000/maps/?point={lat}%2C{lon}&locale=en-US&vehicle=mapcrider2&weighting=fastest&elevation=true&use_miles=false&layer=Mapbox%20Tile'
		).content)
	print(resj)
	lon_n, lat_n = resj['coordinates']
	return(lon_n, lat_n)








linesCollection = dict(type="FeatureCollection",features=[])
	for trip in all_good_trip_lst:

		try:
			mapped_points, df2, error = lib.get_mapped_route(trip, data = DATA)
			linefeature = lib.add_mapped_points(df2, mapped_points, error)
			linesCollection['features'].append(linefeature)
			json_temp = json.dumps(linesCollection)

		except Exception as e:
			print(trip)
		
		
	# f = open("/mnt/c/Users/bitas/folders/MAPC/geojson_files/301_320/%s_shortest.json" % prefix,"w")
	f = open(f"/mnt/c/Users/bitas/folders/MAPC/geojson_files/{trip}_bike_20.json"  ,"w")
	f.write(json_temp)
	f.close()
