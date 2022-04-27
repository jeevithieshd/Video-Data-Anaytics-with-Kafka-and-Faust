import faust
import json
from yarl import URL
import os
import cv2
import numpy as np

app = faust.App(
    'end_storer',
    broker=[URL("kafka://localhost:9092"), URL("kafka://localhost:9093")],
    value_serializer='raw',
    web_port=7003
)

stream_in = app.topic('corpus')

@app.agent(stream_in)
async def process(stream) :
    async for event in stream.events():
        #read message(image) & header data
        nparr = np.frombuffer(event.value, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        k = list(event.headers.keys())
        v = list(event.headers.values())

        print(k)
        
        res = json.loads(v[0].decode('UTF-8'))

        #number of bboxes output by yolo model
        num_bbox = len(res['xmin'])
        count = 0
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
    