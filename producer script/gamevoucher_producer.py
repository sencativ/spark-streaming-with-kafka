import json
import uuid
import os
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer
from faker import Faker
from time import sleep
from datetime import datetime, timedelta

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = "game-voucher"

producer = KafkaProducer(bootstrap_servers=f"{kafka_host}:9092")
faker = Faker()

game_titles = ["Dota 2", "Valorant", "Mobile Legends", "HoK", "Roblox"]


class DataGenerator(object):
    @staticmethod
    def get_data():
        now = datetime.now()
        return [
            uuid.uuid4().__str__(),  # order_id
            faker.random_int(min=1, max=100),  # customer_id
            faker.random_element(elements=game_titles),  # game title
            faker.random_int(min=100, max=150000),  # price
            faker.unix_time(
                start_datetime=now - timedelta(minutes=60), end_datetime=now
            ),  # timestamp
        ]


try:
    while True:
        columns = [
            "order_id",
            "customer_id",
            "game_title",
            "price",
            "ts",
        ]
        data_list = DataGenerator.get_data()
        json_data = dict(zip(columns, data_list))
        _payload = json.dumps(json_data).encode("utf-8")
        print(_payload, flush=True)
        response = producer.send(topic=kafka_topic, value=_payload)
        print(response.get())
        sleep(3)
except KeyboardInterrupt:
    print("Producer stopped by user.")
    producer.close()  # Pastikan untuk menutup Kafka producer dengan benar
