import pandas, time, re, requests, json
from xml.etree.ElementTree import Comment, tostring
from xml.etree.ElementTree import ElementTree
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import SubElement
import xml.etree.ElementTree as ET

from xml.dom import minidom



# import multiprocessing, os

def prettify(elem):
    """Return a pretty-printed XML string for the Element.
    """
    rough_string = tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")




def json_to_csv(df, trip_id): ## reads the json file requested from lime url and retrives route points of each trip in format of a dataframe

    df2 = df[df['trip_id']==trip_id]
    route = json.loads(df2['route'].item().replace("\'", "\""))['features']
    route_points={k:[] for k in ['lon','lat','timestamp']}

    for p in route:

        route_points['lon'].append(p['geometry']['coordinates'][0])
        route_points['lat'].append(p['geometry']['coordinates'][1])
        route_points['timestamp'].append(p['properties']['timestamp'])
    route_df = pandas.DataFrame(route_points)
    # route_df.to_csv(f'/mnt/c/Users/bitas/folders/MAPC/csv_files/{trip_id}.csv')
    return route_df, df2


def csv_to_gpx(df, trip_id): ## Gets the dataframe created above and converts it into a gpx format

	root = Element('gpx')
	root.set('version', '1.1')
	root.set('xmlns',"http://www.topografix.com/GPX/1/1")
	root.set('xmlns:xsi',"http://www.w3.org/2001/XMLSchema-instance")
	root.set('creator',"data from lime bike analysis by mapc")
	root.set('xmlns:gh',"https://graphhopper.com/public/schema/gpx/1.1")

	trk = SubElement(root,'trk')
	_name = SubElement(trk, 'name')
	_name.text = "Track with Pyton"

	# df = pandas.read_csv(file_path)
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
	# return tree

def error(dist, original_dist):

    error = (100 * abs(1 -  dist /original_dist))
    error = math.floor(error * 100) / 100.0
    return error

def match(gpx_file, trip_id):  ## Feeds the GPX formatted files to graphhopper and yields the snapped points in format of a list


    headers = {
            'Content-Type': 'application/gpx+xml'
    }

    ###### test multiple gps accuracies and pick the best one


    df = pandas.DataFrame([
        {
            "gps_accuracy": accuracy,
            "response": requests.post(
                'http://localhost:8989/match?locale=en&gps_accuracy=20&max_visited_nodes=2000&type=json&vehicle=bfoot&points_encoded=false&weighting=priority&ch.disable=true', 
                data = open("/mnt/c/Users/bitas/folders/MAPC/gpx_files/{0}.gpx".format(trip_id),'rb'), 
                headers = headers
            ).json()
        }
        for accuracy in [20, 40]
    ])
    df["diff"] = df["response"].apply(
        lambda response: error(
            response["map_matching"]["distance"],
            response["map_matching"]["original_distance"]
        )
    )

    # if error is less than 5%, return response with accuracy 20
    if (response.iloc[0]["diff"] < 5):
        result = df[df["gps_accuracy"] == 20].iloc[0]
    # otherwise return the minimum diff
    else:
        result = df.sort_values("diff", ascending = True).iloc[0]

    return (result["response"]['paths'][0]['points']['coordinates'], result["diff"])

    
    gps_accuracy =20
    resp_20 = requests.post(
        'http://localhost:8989/match?locale=en&gps_accuracy=20&max_visited_nodes=2000&type=json&vehicle=bfoot&points_encoded=false&weighting=priority&ch.disable=true', 
        data = open("/mnt/c/Users/bitas/folders/MAPC/gpx_files/{0}.gpx".format(trip_id),'rb'), 
        headers = headers
    )
    response_20 = resp_20.json()
    # print(response)
    # map_match_20 = response_20['paths'][0]['points']['coordinates']
    ## calculating the difference between original and matched routes' distances 
    dist_20 = response_20['map_matching']['distance']
    original_dist_20 = response_20['map_matching']['original_distance']
    diff_20 = error(dist_20, original_dist_20)

    ## testing gps accuracy=40:

    gps_accuracy =40
    resp_40 = requests.post(
        'http://localhost:8989/match?locale=en&gps_accuracy=40&max_visited_nodes=2000&type=json&vehicle=bfoot&points_encoded=false&weighting=priority&ch.disable=true', 
        data = open("/mnt/c/Users/bitas/folders/MAPC/gpx_files/{0}.gpx".format(trip_id),'rb'), 
        headers = headers
    )
    response_40 = resp_40.json()
    # print(response)
    # map_match40 = response_40['paths'][0]['points']['coordinates']
     
    dist_40 = response_40['map_matching']['distance']
    original_dist_40 = response_40['map_matching']['original_distance']
    diff_40 = error(dist_40, original_dist_40)

    diff = min(diff_20, diff_40)

    if (diff_20> diff_40) and (diff_20 <6.9):
        diff = diff_20
        map_match = response_20['paths'][0]['points']['coordinates']
    else:
        map_match = response_40['paths'][0]['points']['coordinates']
    return map_match, diff


def get_mapped_route(trip_id, data): ## combines functions for map-matching and returns a nested dictionary including needed fields in the shapefile for each  trip 


	df, df_trip = json_to_csv(data,trip_id)
	
	gpx = csv_to_gpx(df, trip_id)
	matched_points, error = match(gpx, trip_id)
	
	return matched_points, df_trip, error  ## returns error as a relative difference between the original path and the matched one


def add_mapped_points(df, matched_points, error):  ## adds the snapped path to the df retrieved from lime url

    
    propertDict = dict(
        accuracy=df['accuracy'].item(),
        device_id=df['device_id'].item(),
    
        propulsion_type=df['propulsion_type'].item(),
        provider_id=df['provider_id'].item(),
        provider_name=df['provider_name'].item(),
    
        trip_distance=df['trip_distance'].item(),
        trip_duration=df['trip_duration'].item(),
        trip_id=df['trip_id'].item(),
        vehicle_id=df['vehicle_id'].item(),
        vehicle_type=df['vehicle_type'].item(),
        lat_o = json.loads(df['route'].item().replace("\'", "\""))['features'][0]['geometry']['coordinates'][1],
        lon_o = json.loads(df['route'].item().replace("\'", "\""))['features'][0]['geometry']['coordinates'][0],
        lat_d = json.loads(df['route'].item().replace("\'", "\""))['features'][-1]['geometry']['coordinates'][1],
    	lon_d = json.loads(df['route'].item().replace("\'", "\""))['features'][-1]['geometry']['coordinates'][0],
        matching_error = error,
        start_time = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime((df['start_time'].item())))),
        end_time = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime((df['end_time'].item()))))
    )
    linegeo = dict(type="LineString",coordinates=[])
    # the following block goes through the points' coordinates of one trip (one row of the table) and puts it into 
    # the line format of the geo json -- any noise cleaning happens here.
    for f in matched_points:  ## for i in df['map_matched']

        linegeo['coordinates'].append(f)
    linefeature = dict(type="feature",geometry=linegeo,properties=propertDict)
    return linefeature


#############

