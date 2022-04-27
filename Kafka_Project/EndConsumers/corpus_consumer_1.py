import cv2
import numpy as np
from kafka import KafkaConsumer
import os
import json
import time

#build a corpus of images per label

consumer = KafkaConsumer(
    'corpus',
    bootstrap_servers=['localhost:9094', 'localhost:9095'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='database_creators',
    consumer_timeout_ms=10000
)

start = 0
count = 0
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

    for i in range(num_bbox):

        #only select predictions with high confidence
        if(res['confidence'][str(i)] > 0.7):

            #calculate the coordinates of bbox
            x1 = int(res['xmin'][str(i)])
            x2 = int(res['xmax'][str(i)])
            y1 = int(res['ymin'][str(i)])
            y2 = int(res['ymax'][str(i)])

            #crop bbox image
            cimg = img[y1:y2,x1:x2]

            #save it with numbered file name
            filename = res['name'][str(i)]
            path = './output/'
            cv2.imwrite(os.path.join(path , filename+str(count)+'.png'), cimg)
            count += 1
    frame_no += 1

end = time.time()
print("Time taken to process " ,  frame_no, " messages : ", end - start - 10, start, end - 10)