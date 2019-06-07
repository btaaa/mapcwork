
import multiprocessing
import pandas, time, re, json
from xml.etree.ElementTree import Comment, tostring
from xml.etree.ElementTree import ElementTree
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import SubElement
import xml.etree.ElementTree as ET
from xml.dom import minidom


def json_to_csv(trip_id, df= data): ## reads the json file requested from lime url and retrives route points of each trip in format of a dataframe
	
    df2 = df[df['trip_id']==trip_id]
    print(df2)
    route = json.loads(df2['route'].item().replace("\'", "\""))['features']
    route_points={k:[] for k in ['lon','lat','timestamp']}

    for p in route:

        route_points['lon'].append(p['geometry']['coordinates'][0])
        route_points['lat'].append(p['geometry']['coordinates'][1])
        route_points['timestamp'].append(p['properties']['timestamp'])
    route_df = pandas.DataFrame(route_points)
    route_df.to_csv(f'/mnt/c/Users/bitas/folders/MAPC/imp_csv/{trip_id}.csv')
    # return route_df, df2



def csv_to_gpx(trip_id): ## Gets the dataframe created above and converts it into a gpx format

	root = Element('gpx')
	root.set('version', '1.1')
	root.set('xmlns',"http://www.topografix.com/GPX/1/1")
	root.set('xmlns:xsi',"http://www.w3.org/2001/XMLSchema-instance")
	root.set('creator',"data from lime bike analysis by mapc")
	root.set('xmlns:gh',"https://graphhopper.com/public/schema/gpx/1.1")

	trk = SubElement(root,'trk')
	_name = SubElement(trk, 'name')
	_name.text = "Track with Pyton"

	df = pandas.read_csv(f'/mnt/c/Users/bitas/folders/MAPC/imp_csv/{trip_id}.csv')
	df['date_time'] = df["timestamp"].apply(lambda x:  pandas.to_datetime(x,unit='s'))
	df["datetime"] = df["date_time"].apply(lambda x : re.sub(r'\s','T', f"{x}"))
	df["datetime"] = df["datetime"].apply(lambda x : f"{x}Z")
	df = df.reset_index()
	# trip_id = file_path.split('/')[-1].split('.')[0]

	trkseg = SubElement(trk, 'trkseg')

	for i in range(len(df)):
	    
		iindex = df.index[i]
		this_lat = df.loc[iindex,["lat"]].values[0]
		this_lon = df.loc[iindex,["lon"]].values[0]
		this_record = df.loc[iindex,["datetime"]].values[0]
		trkpt = SubElement(trkseg, 'trkpt', {'lat': f"{this_lat}", 'lon':f"{this_lon}"})
		_ele = SubElement(trkpt, 'ele')
		_ele.text = "16.39"
		_time = SubElement(trkpt,'time')
		_time.text = f"{this_record}"
	tree = ElementTree(root)
	tree.write(open("/mnt/c/Users/bitas/folders/MAPC/imp_gpx/{0}.gpx".format(trip_id), 'wb'), encoding='UTF-8')



def multi_worker(trip_id):

	json_to_csv(trip_id)
	
	# csv_to_gpx(trip_id)
	
	




      


# data = df.loc[df['trip_id'].isin(all_good_261)]

NUMTHREADS = 5
NUMLINES = 100

if __name__ == '__main__':


	data = pandas.read_csv('/mnt/c/Users/bitas/folders/MAPC/lime_trips_261_280.csv')
	
	all_good_261 = []

	with open('/mnt/c/Users/bitas/folders/MAPC/261_280/imp.txt','r') as f:
		for line in f:
			all_good_261.append((line.strip()))

	all_good_261 = all_good_261[:10]
	print(all_good_261)

    
	N = len(all_good_261)

	pool = multiprocessing.Pool(processes=NUMTHREADS)
	pool.map(
		multi_worker, 
		

        (all_good_261[line:line + NUMLINES]
        for line in range(0, N, NUMLINES)
        
        ))
	pool.close()
	pool.join()


