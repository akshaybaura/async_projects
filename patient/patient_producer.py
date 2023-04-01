import json
import uuid
import os
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

class PatientProducer:
    def __init__(self):
        producer_config = {"bootstrap.servers": 'broker:29092',"schema.registry.url": 'http://schema-registry:8081'}
        # hardcoded key schema
        key_schema_string = """{"type": "string"}"""
        key_schema = avro.loads(key_schema_string)
        # reading file for value schema
        path = os.path.realpath(os.path.dirname(__file__))
        with open(f"{path}/sch1.avsc") as f:
            value_schema = avro.loads(f.read())
        # avro serializer instantiation
        self.producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)
        self.topic = 'patient'

    def send_record(self, json_value):
        
        key = str(uuid.uuid4())
        value = json_value

        try:
            self.producer.produce(topic=self.topic, key=key, value=value)
        except Exception as e:
            print(f"Exception while producing record value - {value} to topic - {self.topic}: {e}")
        # else:
        #     print(f"Successfully producing record value - {value} to topic - {self.topic}")

        self.producer.flush()