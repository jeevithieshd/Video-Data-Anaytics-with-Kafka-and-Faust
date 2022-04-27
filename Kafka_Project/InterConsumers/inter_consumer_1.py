import torch
import cv2
import numpy as np
from kafka import KafkaProducer, KafkaConsumer
import json
import time

consumer = KafkaConsumer(
    'image_frames', 
    bootstrap_servers = ['localhost:9092', 'localhost:9093'], 
    group_id='inter', 
    enable_auto_commit=True,
    auto_offset_reset='earliest',
    consumer_timeout_ms=10000
)

producer = KafkaProducer(
    bootstrap_servers = ['localhost:9094','localhost:9095'],
    acks = 'all',
    retries = 100,
    max_in_flight_requests_per_connection = 5,
    compression_type = 'snappy',
    linger_ms = 5
)


model = torch.hub.load('ultralytics/yolov5', 'yolov5s', device="cpu")
count = 0
frame_no = 0

start = 0

for msg in consumer:
    if msg == None:
        continue
    if frame_no == 1:
        start = time.time()

    #read message(image) from consumer
    nparr = np.frombuffer(msg.value, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

    #perform yolo inferencing on the image        
    results = model(img)

    #get results of yolo (bbox with label & confidence)
    data = results.pandas().xyxy[0]

    #encode image to produce again
    _,img_buffer_arr = cv2.imencode(".jpg", img)
    img_bytes = img_buffer_arr.tobytes()

    #send image and result to topic B
    producer.send(
        topic = 'corpus', 
        value = img_bytes,
        headers = [(str(frame_no),str.encode(json.dumps(data.to_dict())))]
    )

    #send image and result to topic C
    producer.send(
        topic = 'aggregation', 
        value = msg.headers[0][1],
        headers = [(str(msg.headers[0][0]),str.encode(json.dumps(data.to_dict())))]
    )

    frame_no +=1

end = time.time()
print("Time taken to process " ,  frame_no, " messages : ", end - start - 10, start, end - 10)
