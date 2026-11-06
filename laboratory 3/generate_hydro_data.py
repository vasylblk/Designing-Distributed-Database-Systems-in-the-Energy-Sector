#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

STATIONS = [
    ('HPP_DNIPRO_1', 'Francis'),
    ('HPP_KREMENCHUK', 'Kaplan'),
    ('HPP_KANIW', 'Kaplan'),
    ('HPP_KAHIVKA', 'Kaplan'),
    ('HPP_DNIPRODZERZHYNSK', 'Kaplan')
]

def generate_datasets():
    step_seconds = 12
    total_steps = (30 * 24 * 3600) // step_seconds
    base_time = datetime.now() - timedelta(days=30)
    
    data_simple = []
    data_hourly = []
    data_daily = []

    for step in range(total_steps):
        cur_time = base_time + timedelta(seconds=step * step_seconds)
        b_hour = cur_time.replace(minute=0, second=0, microsecond=0)
        b_date = cur_time.date()
        hour = cur_time.hour

        # 1. Базовий розподіл потужності за годинами з методички
        if 12 <= hour <= 14:
            power_kw = random.uniform(3.0, 5.0)      # Денний пік: 3–5 кВт
        elif (6 <= hour <= 9) or (17 <= hour <= 19):
            power_kw = random.uniform(1.0, 3.0)      # Ранок/вечір: 1–3 кВт
        elif (hour >= 21) or (hour <= 5):
            power_kw = random.uniform(0.0, 0.2)      # Ніч: 0–0.2 кВт
        else:
            power_kw = random.uniform(1.5, 3.0)      # Перехідні години

        # 2. Хмарність: випадкове падіння потужності на 30–70% з імовірністю 20%
        if 6 <= hour <= 19 and random.random() < 0.20:
            cloud_drop = random.uniform(0.30, 0.70)
            power_kw *= (1.0 - cloud_drop)

        # 3. Синхронізовані гідравлічні параметри (Варіант 3)
        flow_m3s = 500.0 + (power_kw / 5.0) * 1800.0 + random.uniform(-25.0, 25.0)
        level_m = 50.0 + (power_kw / 5.0) * 1.2 + random.uniform(-0.1, 0.1)

        for station_id, turb_type in STATIONS:
            data_simple.append((station_id, cur_time, turb_type, flow_m3s, level_m, power_kw))
            data_hourly.append((station_id, b_hour, cur_time, turb_type, flow_m3s, level_m, power_kw))
            data_daily.append((station_id, b_date, cur_time, turb_type, flow_m3s, level_m, power_kw))

    return data_simple, data_hourly, data_daily

def run_isolated_benchmark():
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('hydro_analytics')

    data_simple, data_hourly, data_daily = generate_datasets()
    total_records = len(data_simple)
    batch_chunk = 5000
    pool_concurrency = 80


    bench_tasks = [
        ("Simple", "INSERT INTO telemetry_simple (station_id, recorded_at, turbine_type, water_flow, water_level, power_output) VALUES (?, ?, ?, ?, ?, ?)", data_simple),
        ("Hourly", "INSERT INTO telemetry_hourly (station_id, bucket_hour, recorded_at, turbine_type, water_flow, water_level, power_output) VALUES (?, ?, ?, ?, ?, ?, ?)", data_hourly),
        ("Daily",  "INSERT INTO telemetry_daily (station_id, bucket_date, recorded_at, turbine_type, water_flow, water_level, power_output) VALUES (?, ?, ?, ?, ?, ?, ?)", data_daily)
    ]

    results_table2 = []

    for name, cql, dataset in bench_tasks:
        print(f"\nЗавантаження в схему")
        stmt = session.prepare(cql)
        
        start_time = time.perf_counter()
        for i in range(0, total_records, batch_chunk):
            chunk = dataset[i:i + batch_chunk]
            execute_concurrent_with_args(session, stmt, chunk, concurrency=pool_concurrency)
            if (i + batch_chunk) % 200000 == 0 or (i + batch_chunk) >= total_records:
                print(f"    Прогрес: {min(i + batch_chunk, total_records)} / {total_records}")

        elapsed_time = time.perf_counter() - start_time
        writes_per_sec = total_records / elapsed_time if elapsed_time > 0 else 0
        avg_write_latency_ms = (elapsed_time / total_records) * 1000.0

        results_table2.append({
            "name": name,
            "writes_sec": writes_per_sec,
            "latency": avg_write_latency_ms,
            "batch_size": batch_chunk
        })
        print(f"    Час: {elapsed_time:.2f} с | Throughput: {writes_per_sec:.1f} оп/с")

    # Наповнення таблиці агрегатів
    print("\nНаповнення таблиці hydro_hourly_aggregates")
    agg_stmt = session.prepare("""
        INSERT INTO hydro_hourly_aggregates 
        (station_id, hour, avg_water_flow, max_water_flow, avg_water_level, total_power, samples_count)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)
    agg_data = []
    base_time = datetime.now() - timedelta(days=30)
    for day in range(30):
        for h in range(24):
            cur_h = (base_time + timedelta(days=day, hours=h)).replace(minute=0, second=0, microsecond=0)
            for station_id, _ in STATIONS:
                agg_data.append((station_id, cur_h, 1450.0, 2380.0, 50.4, 280.0, 300))
    execute_concurrent_with_args(session, agg_stmt, agg_data, concurrency=50)

    # Вивід готової Таблиці 2
    print("Таблиця 2: Write throughput ")
    print(f"{'Схема':<12} | {'Writes/sec':<16} | {'Avg Write Latency':<20} | {'Batch Size'}")
    for res in results_table2:
        print(f"{res['name']:<12} | {res['writes_sec']:>12.1f} оп/с | {res['latency']:>15.3f} ms | {res['batch_size']}")


    cluster.shutdown()

if __name__ == "__main__":
    run_isolated_benchmark()
