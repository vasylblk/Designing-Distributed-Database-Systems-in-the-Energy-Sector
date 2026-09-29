#!/usr/bin/env python3
"""Тестування Key-based партиціонування для ГЕС"""
import time
import json
from kafka import KafkaProducer
from hydro_generator import generate_hydro_record

TOPICS = ["hydro-part-2", "hydro-part-4", "hydro-part-8"]
NUM_RECORDS = 5000

producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    api_version=(3, 7, 1),
    acks=1,
    batch_size=65536,
    linger_ms=10,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"=== Початок тестування партиціонування ({NUM_RECORDS} записів) ===")

for topic in TOPICS:
    start_time = time.time()
    for _ in range(NUM_RECORDS):
        rec = generate_hydro_record()
        # Ключ - тип турбіни для маршрутизації
        key_bytes = rec["turbine_type"].encode('utf-8')
        producer.send(topic, key=key_bytes, value=rec)
    producer.flush()
    elapsed = time.time() - start_time
    throughput = NUM_RECORDS / elapsed
    print(f"Топік: {topic:<14} Час: {elapsed:.2f} c | Throughput: {throughput:.1f} rec/sec")

producer.close()