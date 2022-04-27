import faust
import json
from yarl import URL

app = faust.App(
    'end_reader',
    broker=[URL("kafka://localhost:9092"), URL("kafka://localhost:9093")],
    value_serializer='raw',
    web_port=7005
)

stream_in = app.topic('aggregation')

table = app.Table("vehicle_counts", partitions=2, default=int)

vehicles = {'car', 'bus', 'train', 'bicycle', 'motorcycle', 'airplane', 'truck', 'boat'}

@app.agent(stream_in)
async def process(stream) :
    async for event in stream.events():

        #read message(image) & header data        
        v = list(event.headers.values())
        
        res = json.loads(v[0].decode('UTF-8'))

        #number of bboxes output by yolo model
        num_bbox = len(res['xmin'])

        for i in range(num_bbox):

            #only count vehicles with confidence greater than 0.5
            if(res['confidence'][str(i)] > 0.5 and res['name'][str(i)] in vehicles):
                table[res['name'][str(i)]] += 1
        
        print(table.data.items())