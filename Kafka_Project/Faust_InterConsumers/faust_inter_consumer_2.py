import faust
import json
from yarl import URL
import torch
import cv2
import numpy as np

app = faust.App(
    'data_streamers',
    broker=[URL("kafka://localhost:9092"), URL("kafka://localhost:9093")],
    value_serializer='raw',
    web_port=7001
)

stream_in = app.topic('image_frames')
stream_out_1 = app.topic('corpus')
stream_out_2 = app.topic('aggregation')

model = torch.hub.load('ultralytics/yolov5', 'yolov5s', device="cpu")

@app.agent(stream_in)
async def process(stream) :
    async for event in stream.events():
        nparr = np.frombuffer(event.value, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        results = model(img)

        data = results.pandas().xyxy[0]

        _,img_buffer_arr = cv2.imencode(".jpg", img)
        img_bytes = img_buffer_arr.tobytes()

        k = list(event.headers.keys())
        v = list(event.headers.values())
        
        print("frame :", k[0], "Recieved")

        await stream_out_1.send(value = img_bytes, headers = [(k[0], str.encode(json.dumps(data.to_dict())))])
        await stream_out_2.send(value = v[0], headers = [(k[0], str.encode(json.dumps(data.to_dict())))])