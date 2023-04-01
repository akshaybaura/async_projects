import json
import uuid
import os
import io
import fastavro
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from fastavro.schema import parse_schema
import validator

class TaxiProducer:
    def __init__(self):
        producer_config = {"bootstrap.servers": 'localhost:9092',"schema.registry.url": 'http://localhost:8081'}
        # hardcoded key schema
        key_schema_string = """{"type": "string"}"""
        key_schema = avro.loads(key_schema_string)
        # reading file for value schema
        path = os.path.realpath(os.path.dirname(__file__))
        with open(f"{path}/schema/sch1.avsc") as f:
            value_schema = avro.loads(f.read())
        # avro serializer instantiation
        self.producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)
        self.topic = 'taxi'
        self.mymodel = validator.create_pydantic_model(f"{path}/schema/sch1.avsc")


    def send_record(self, json_value):
        key = str(uuid.uuid4())
        value = json_value
        try:
            #explicit type conversion
            if value.get('pickup_centroid_location'):
                value['pickup_centroid_location'] = json.dumps(value['pickup_centroid_location'])
            if value.get('dropoff_centroid_location'):
                value['dropoff_centroid_location'] = json.dumps(value['dropoff_centroid_location'])
            dd = self.mymodel(**value)
            self.producer.produce(topic=self.topic, key=key, value=value)
        except Exception as e:
            print(f"Exception while producing record value - {value} to topic - {self.topic}: {e}")
        # else:
        #     print(f"Successfully producing record value - {value} to topic - {self.topic}")
        else:
            del dd
        self.producer.flush()

p = TaxiProducer()    
# p.send_record({'trip_id': 'fecfaeb2101d51d30bce22e30ba9f9cfe44b42c7', 'taxi_id': '141074afe6aae95a4613cde037b5ed73011d4d19017b59d970600d380294700d5ef4a72d0e7b050b68e1138a6e7ab1ebd82768c68b4d18bcb116d3990afecab0', 'trip_start_timestamp': '2023-03-01T00:00:00.000', 'trip_end_timestamp': '2023-03-01T00:15:00.000', 'trip_seconds': 960, 'trip_miles': 12, 'pickup_community_area': 56, 'dropoff_community_area': 24, 'fare': 30.5, 'tips': 7.2, 'tolls': 0, 'extras': 5, 'trip_total': '42.7', 'payment_type': 'Credit Card', 'company': 'Taxi Affiliation Services', 'pickup_centroid_latitude': 41.79259236, 'pickup_centroid_longitude': -87.769615453, 'pickup_centroid_location': "{'type': 'Point', 'coordinates': [-87.7696154528, 41.7925923603]}", 'dropoff_centroid_latitude': '41.901206994', 'dropoff_centroid_longitude': -87.676355989, 'dropoff_centroid_location': {'type': 'Point', 'coordinates': '[-87.6763559892, 41.9012069941]'}})