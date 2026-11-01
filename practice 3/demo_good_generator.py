#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Демонстрація ПРАВИЛЬНОГО підходу - Hourly Bucketing
"""

from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import random

def get_bucket_hour(timestamp):
    """Округлює timestamp до початку години"""
    return timestamp.replace(minute=0, second=0, microsecond=0)

def generate_good_data():
    """Генерує дані в ПРАВИЛЬНУ схему"""
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('demo_practice')
    
    # 30 днів для швидкої генерації та чесного порівняння з telemetry_bad
    days_to_generate = 30
    device_id = 'DEMO_SOLAR_001'
    start_date = datetime.now() - timedelta(days=days_to_generate)
    
    insert_stmt = session.prepare("""
        INSERT INTO telemetry_hourly
        (device_id, bucket_hour, recorded_at, power_output, voltage)
        VALUES (?, ?, ?, ?, ?)
    """)
    
    print("Генерація даних в ПРАВИЛЬНУ схему...")
    print(f"{days_to_generate} днів × 24 години = {days_to_generate * 24} партицій")
    print("Кожна партиція: ~60 записів = ~15 KB\n")
    
    count = 0
    partitions = set()
    current_time = start_date
    now = datetime.now()

    while current_time < now:
        bucket_hour = get_bucket_hour(current_time)
        partitions.add((device_id, bucket_hour))
        
        hour = current_time.hour
        if 6 <= hour <= 20:
            power = random.uniform(1.0, 4.0)
        else:
            power = random.uniform(0.0, 0.2)
            
        voltage = 400.0 + random.uniform(-5, 5)
        
        session.execute(insert_stmt, (
            device_id,
            bucket_hour,
            current_time,
            power,
            voltage
        ))
        count += 1
        if count % 5000 == 0:
            print(f" {count} записів → {len(partitions)} партицій")
        current_time += timedelta(minutes=1)
        
    print(f"\n Завершено!")
    print(f" Всього записів: {count}")
    print(f" Всього партицій: {len(partitions)}")
    print(f" Середній розмір партиції: {count / len(partitions):.0f} записів")
    cluster.shutdown()

if __name__ == "__main__":
    generate_good_data()
