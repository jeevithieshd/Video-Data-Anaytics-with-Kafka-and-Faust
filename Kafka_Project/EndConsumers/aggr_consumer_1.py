import cv2
import numpy as np
from kafka import KafkaConsumer
import json
import time

#keeps a moving count of vehicles per frame

consumer = KafkaConsumer(
    'aggregation',
    bootstrap_servers=['localhost:9094', 'localhost:9095'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='accumulators',
    consumer_timeout_ms=10000
)

d = {}

vehicles = {'car', 'bus', 'train', 'bicycle', 'motorcycle', 'airplane', 'truck', 'boat'}

start = 0
frame_no = 1

for msg in consumer:
    if frame_no == 1:
        start = time.time()
    #read message(image) & header data
    nparr = np.frombuffer(msg.value, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    res = json.loads(msg.headers[0][1].decode('UTF-8'))

    #number of bboxes output by yolo model
    num_bbox = len(res['xmin'])

    vehicle_count = 0

    for i in range(num_bbox):

        #only count vehicles with confidence greater than 0.5
        if(res['confidence'][str(i)] > 0.5 and res['name'][str(i)] in vehicles):
            vehicle_count += 1

    #store vehicle count in a dictionary

    key = msg.value.decode('UTF-8')
    if key in d.keys():
        dict = d[msg.value.decode('UTF-8')]
        dict[msg.headers[0][0]] = vehicle_count
        d[msg.value.decode('UTF-8')] = dict
    else:
        d[msg.value.decode('UTF-8')] = {}
        d[msg.value.decode('UTF-8')][msg.headers[0][0]] = vehicle_count

    frame_no += 1
print(d)

end = time.time()

print("Time taken to process " ,  frame_no, " messages : ", end - start - 10, start, end - 10)