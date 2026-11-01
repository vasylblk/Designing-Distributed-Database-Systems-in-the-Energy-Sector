#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import random

def generate_bad_data():
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('demo_practice')
    
    days_to_generate = 30
    device_id = 'DEMO_SOLAR_001'
    start_date = datetime.now() - timedelta(days=days_to_generate)
    
    insert_stmt = session.prepare("""
        INSERT INTO telemetry_bad (device_id, recorded_at, power_output, voltage)
        VALUES (?, ?, ?, ?)
    """)
    
    print("Генерація даних в ПОГАНУ схему (telemetry_bad)...")
    print(f"Створюємо {days_to_generate} днів × 1440 записів/день = {days_to_generate * 1440} записів")
    print("ВСІ записи підуть в ОДНУ партицію!\n")
    
    count = 0
    current_time = start_date
    now = datetime.now()
    
    while current_time < now:
        hour = current_time.hour
        power = random.uniform(1.0, 4.0) if 6 <= hour <= 20 else random.uniform(0.0, 0.2)
        voltage = 400.0 + random.uniform(-5, 5)
        
        session.execute(insert_stmt, (device_id, current_time, power, voltage))
        count += 1
        if count % 5000 == 0:
            print(f"Вставлено {count} записів... (все в одну партицію!)")
        current_time += timedelta(minutes=1)
        
    print(f"\nЗавершено! Всього записів: {count}")
    print(f"Розмір партиції для device_id='{device_id}': ВЕЛИЧЕЗНИЙ (wide row)!")
    cluster.shutdown()

if __name__ == "__main__":
    generate_bad_data()
