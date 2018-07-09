# coding: utf-8
import numpy as np
from time import time

class DataPool(object):
    """Generate test dataset from supplied data for network simulation"""
    def __init__(self, **datafile):
        """Initilize call duration, arrival and car speed files"""
        self.default = {
                "duration" : "call_duration_g01.txt",
                "arrival": "call_arrival_g01.txt",
                "speed": "car_speed_g01.txt"
                }
        self.default.update(datafile)

    @staticmethod
    def read_input_file(file_name, column):
        """ read file and compute some values. """
        input_file = open(file_name)
        lines = input_file.readlines()
        i = 0
        list_of_column = []
        for line in lines:
            line = line.split('\t')
            line[-1] = line[-1][:-1]
            if i < 2:
                i+=1
                continue
            list_of_column.append(float(line[column]))
        mean = np.mean(list_of_column)
        std = np.std(list_of_column)
        if column == 1:
            return mean,std
        list_of_delta = []
        for i in range(len(list_of_column)):
            if i-1>0:
                list_of_delta.append(list_of_column[i]-list_of_column[i-1])
        if column == 2:
            mean_delta = np.mean(list_of_delta)
            return mean_delta   
        if column == -1:
            car_speed_mean_std  = (np.mean(list_of_column),np.std(list_of_column))
            return car_speed_mean_std

    def generate_dataset(self, size=2000000):
        """ generate a dataset and keep it in the member value dataset i.e.  self.dataset"""
        call_duration_mean = self.read_input_file(self.default['duration'],1)[0]
        call_arrival_mean = self.read_input_file(self.default['arrival'],2)
        car_speed = self.read_input_file(self.default['speed'],-1)
        dataset = {}
        for i in range(size):
            dataset[i] = {
                'call_duration':np.random.exponential(scale=call_duration_mean),# the unit are seconds    
                'call_arrival':np.random.exponential(scale=call_arrival_mean),# seconds
                'car_speed':np.random.normal(loc=car_speed[0], scale=car_speed[1])/3600, # km/s
                'car_position':np.random.uniform(low=0, high=40) # km
            }
        self.dataset = dataset

class BaseStations(object):
    def __init__(self, num_of_stations, num_of_channels_per_station=10):
        self.num_of_stations = num_of_stations
        self.drop_num = 0
        self.blocked_num = 0
        self.status = np.zeros(num_of_stations)
        self.time = 0
        self.num_of_channels_per_station = num_of_channels_per_station
    def arrival_update(self, idx):
        arrival_result = self.bs_add(idx)
        if not arrival_result :
            self.blocked_num += 1
        return arrival_result
        
    def hand_over_update(self, idx, reserve_channel=False):
        '''
        from idx to idx+1 interval
        for example: pos = 2.0
        exchange  from the 0th bs to the 1st bs
        '''
        r1 = self.bs_minus(idx-1)
        r2 = r1
        if reserve_channel == False:
            r2 = self.bs_add(idx)
        else:
            r2 = self.reserve_bs_add(idx)
        hand_over_result = (r1 and r2)
        if not hand_over_result:
            self.drop_num += 1
        return hand_over_result
    
    def reserve_bs_add(self, idx):
        if idx >= self.num_of_stations:
            return True
        if self.status[idx] < self.num_of_channels_per_station+1:
            self.status[idx] += 1
            return True
        else:
            return False
        
    def call_ended_update(self, idx):
        return self.bs_minus(idx)
        
    def bs_add(self, idx):
        if idx >= self.num_of_stations:
            return True
        if self.status[idx] < self.num_of_channels_per_station:
            self.status[idx] += 1
            return True
        else:
            return False
            
    def bs_minus(self, idx):
        if idx >= self.num_of_stations:
            return True
        if self.status[idx] > 0:
            self.status[idx] -= 1
            return True
        else:
            return False
class Car(object):
    def __init__(self, car_dict):
        self.duration = car_dict['call_duration']
        self.arrival = car_dict['call_arrival']
        self.speed = car_dict['car_speed']
        self.position = car_dict['car_position']
    def generate_time_points(self, contemporary_time=0):
        """
        bss is an instance of BaseStations
        for time_points all of these are hand_over except the last one,which is the call_ended
        """
        beg = self.position # begin position
        end = self.position+self.speed*self.duration # call ended position
        hop_count = 0 #hand over points count
        hops = [] # hand over points
        for i in range(int(beg)+1,int(end)+1):
            if i%2 == 0 :
                hop_count+=1
                hops.append(i)
        time_points = []
        for ele in hops:
            temp = (ele-beg)/self.speed
            time_points.append(temp+contemporary_time)
        time_points.append(contemporary_time+(end-beg)/self.speed)
        hops.append(end)
        self.time_points = time_points
        self.positions = hops
        assert len(self.time_points)==len(self.positions)       
def main_function(dataset, num_of_channels=10, reserve_channel=False):
    tic = time()
    new_tic = time()
    #dataset = generate_datasets(2000000)
    i = 1
    bss = BaseStations(20,num_of_channels)
    car = Car(dataset[0])
    car.generate_time_points()
    last_car = car
    calling_list = [car]
    while True:
        #car = Car(dataset[i])
        #car.generate_time_points(bss.time)
        min_event_time, index = min([(ele.time_points[0],calling_list.index(ele))for ele in calling_list])
        if bss.time< min_event_time:
            car = Car(dataset[i])
            idx = int(last_car.position/2)
            bss.arrival_update(idx)
            if last_car not in calling_list:
                last_car.generate_time_points(bss.time)
                calling_list.append(last_car)
            i+=1
            bss.time+=car.arrival
            last_car = car
        else:
            # hand_over or end calls
            deal_car = calling_list[index]
            deal_car.time_points.pop(0)
            pos = deal_car.positions.pop(0)
            idx = int(pos/2)
            # hand over
            if len(deal_car.time_points) > 0: 
                hand_over_result = bss.hand_over_update(idx,reserve_channel=reserve_channel)
                if not hand_over_result: # hand over failure
                    calling_list.pop(index)
            # end calls
            else:
                end_result = bss.call_ended_update(idx) 
                calling_list.pop(index)
        if i== len(dataset)-1:
            new_tic = time()
            break
    time_total = new_tic -tic
    return bss,time_total


size = 50000
dp = DataPool()
dp.generate_dataset(size)
dataset = dp.dataset
bss0,time_total0 = main_function(dataset,9,reserve_channel=True)
bss1,time_total1 = main_function(dataset,10,reserve_channel=False)
# reserve channel
print("######## reserve channel ########") 
print("blocked_rate:%f percent"%(bss0.blocked_num*100.0/size))
print("droped_rate:%f percent"%(bss0.drop_num*100.0/size))
print("program running time:%f seconds"%time_total0)
# do not reserve channel
print("######## do not reserve channel ########")
print("blocked_rate:%f percent"%(bss1.blocked_num*100.0/size))
print("droped_rate:%f percent"%(bss1.drop_num*100.0/size))
print("program running time:%f seconds"%time_total1)
