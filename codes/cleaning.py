import cleaning_lib
import multiprocessing, json, pandas as pd, os



def processer(file_path):

## Read lime_bike CSVs:

	df = pd.read_csv(file_path)
	name = file_path.split('.')[0].split('trips')[1]
	## Distance and duration thresholds on the trip 

	df = df.loc[(df['trip_duration']>59) & (df['trip_distance']>99) & (df['trip_duration']<18000) & (df['trip_distance']<20000)]

	## Calculating nodes' speeds:

	trips = list(df['trip_id'])
	print(len(trips))
	df_speed = pd.DataFrame()
	for i in trips:
	    df_speed = df_speed.append(cleaning_lib.nodes_speed(df,i))
	df_speed = df_speed.dropna()

	## Adjusting speed:
	df_speed = df_speed.groupby('trip_id').apply(cleaning_lib.adjust_speed)

	## saving speeds and trips that need no imputing:

	df_speed = df_speed.dropna()
	df_speed.to_csv(f'/home/bita/lime_bike/data/speed/speed_{name}.csv')
	cleaningNeeding_trips = list(set(df_speed.loc[(df_speed['step_dist']>201) | (df_speed['adjusted_speed']>12)]['trip_id'])) ## Trips with jupms or high speed
	all_good_ids = list(set(df[~df['trip_id'].isin(cleaningNeeding_trips)]['trip_id'])) ## Trips that need no imputing
	with open(f"/home/bita/lime_bike/data/all_good_trips/all_good_ids_{name}.txt", "w") as f:
	    for s in all_good_ids:
	        f.write(str(s) +"\n") 





def multi_worker(file_paths):
    print('Loading %s' % file_paths)
    for fp in file_paths:
        
        processer(fp)
       


DATA_PATH = '/home/bita/lime_bike/data/lime_data/'
NUMTHREADS = 3
NUMLINES = 1

if __name__ == '__main__':

    # Get staypoint data filepaths
    fps = [os.path.join(DATA_PATH, f) for f in os.listdir(DATA_PATH)]
    # print(fps)

    #df = pd.concat([pd.read_csv(fp) for fp in fps])
    N = len(fps)

    pool = multiprocessing.Pool(processes=NUMTHREADS)
    pool.map(multi_worker, 
        (fps[line:line + NUMLINES] for line in range(0, N, NUMLINES)))
    pool.close()
    pool.join()
