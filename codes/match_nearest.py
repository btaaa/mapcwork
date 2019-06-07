
import lib, multiprocessing, json, pandas


def multi_worker(all_good_trip_lst, prefix=1):

	print('Loading %d' % len(all_good_trip_lst))

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




      



NUMTHREADS = 1
NUMLINES = 10000

if __name__ == '__main__':

	# all_good_301 = []

	# with open('/mnt/c/Users/bitas/folders/MAPC/csv_files/all_good_ids__301_320.txt','r') as f:
	# 	for line in f:
	# 		all_good_301.append((line.strip()))


	# all_good_281 = all_good_281[:10]
	# df = pandas.read_csv('/mnt/c/Users/bitas/folders/MAPC/csv_files/lime_trips_301_320.csv')
# 
	# DATA = df.loc[df['trip_id'].isin(all_good_301)]

	# all_good_trip_lst = DATA['trip_id'].tolist()


	DATA = pandas.read_csv('/mnt/c/Users/bitas/folders/MAPC/analysis/all_good_trips.csv')
	all_good_trip_lst = [
'53602a46-fb87-4969-b77c-875b1fd454a2'

]
    
	N = len(all_good_trip_lst)

	pool = multiprocessing.Pool(processes=NUMTHREADS)
	pool.starmap(
		multi_worker, 
		[

        (all_good_trip_lst[line:line + NUMLINES], line)
        for line in range(0, N, NUMLINES)
        ]
        )
	pool.close()
	pool.join()