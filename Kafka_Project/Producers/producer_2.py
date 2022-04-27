from glob import glob
import cv2
import time
import os

from kafka import KafkaProducer

start = time.time()
total = 0

producer = KafkaProducer(
    bootstrap_servers = ['localhost:9092','localhost:9093'],
    acks = 'all',
    retries = 100,
    max_in_flight_requests_per_connection = 5,
    compression_type = 'snappy',
    linger_ms = 5
)

dataset = "../Data/Data2/"
video_paths = glob(dataset + "*.mp4")

for video_path in video_paths:
    video = cv2.VideoCapture(video_path)
    video_name = os.path.basename(video_path).split(".")[0]
    frame_no = 1

    while video.isOpened():
        
        ret, frame = video.read()
        if not ret:
            break
        if frame_no % 3 == 0:
            _, img_buffer_arr = cv2.imencode(".jpg", frame)
            img_bytes = img_buffer_arr.tobytes()
            producer.send(
                topic = 'image_frame', 
                value = img_bytes,
                headers=[(str(frame_no),str.encode(video_name))],
                timestamp_ms=frame_no
            )
            total += 1
            if total == 500:
                break
        frame_no += 1
    video.release()

print("Time Taken for the Producer to send" ,  total, " messages : ", time.time(), start, time.time() - start)