#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

def main():
    print("1. Підключення до Cassandra")
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()

    print("\n2. Створення Keyspace")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS dnipro_hydro_system
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    session.set_keyspace('dnipro_hydro_system')
    print("Keyspace 'dnipro_hydro_system' успішно активовано.")

    print("\n3. Створення таблиць")
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS unit_operational_readings (
            station_name text,
            unit_id text,
            recorded_at timestamp,
            water_level_m double,
            water_flow_m3s double,
            water_head_m double,
            power_mw double,
            PRIMARY KEY ((station_name, unit_id), recorded_at)
        ) WITH CLUSTERING ORDER BY (recorded_at DESC);
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS reservoir_conditions (
            reservoir_name text,
            recorded_date date,
            recorded_at timestamp,
            water_level_m double,
            inflow_m3s double,
            water_temp_c double,
            PRIMARY KEY ((reservoir_name, recorded_date), recorded_at)
        ) WITH CLUSTERING ORDER BY (recorded_at DESC);
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS daily_station_summaries (
            station_name text,
            summary_date date,
            total_generation_mwh double,
            avg_power_mw double,
            max_power_mw double,
            water_discharged_m3 double,
            PRIMARY KEY (station_name, summary_date)
        ) WITH CLUSTERING ORDER BY (summary_date DESC);
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS seasonal_station_analytics (
            season_year text,
            station_name text,
            total_flow_km3 double,
            avg_efficiency_pct double,
            peak_power_mw double,
            PRIMARY KEY (season_year, station_name)
        );
    """)

    # 3. Генерація та запис тестових даних
    print("\n4. Генерація та вставка даних")
    
    stations = {
        "Дніпровська ГЕС": [f"UNIT_{i:02d}" for i in range(1, 9)],
        "Середньодніпровська ГЕС": [f"UNIT_{i:02d}" for i in range(1, 5)],
        "Кременчуцька ГЕС": [f"UNIT_{i:02d}" for i in range(1, 4)]
    }

    base_time = datetime(2026, 10, 23, 0, 0, 0)
    readings_count = 0

    unit_stmt = session.prepare("""
        INSERT INTO unit_operational_readings (
            station_name, unit_id, recorded_at, water_level_m, water_flow_m3s, water_head_m, power_mw
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    # Моделювання добових замірів
    for station, units in stations.items():
        for unit in units:
            for step in range(12):
                rec_time = base_time + timedelta(hours=step * 2)
                level = round(random.uniform(15.0, 24.5), 2)
                flow = round(random.uniform(200.0, 450.0), 1)
                head = round(random.uniform(30.0, 38.0), 2)
                
                max_cap = 150.0 if "Дніпровська" in station else 85.0
                power = round(random.uniform(max_cap * 0.4, max_cap), 2)

                session.execute(unit_stmt, (station, unit, rec_time, level, flow, head, power))
                readings_count += 1

    # Запис даних водосховищ
    res_stmt = session.prepare("""
        INSERT INTO reservoir_conditions (
            reservoir_name, recorded_date, recorded_at, water_level_m, inflow_m3s, water_temp_c
        ) VALUES (?, ?, ?, ?, ?, ?)
    """)
    reservoirs = ["Дніпровське водосховище", "Кам'янське водосховище", "Кременчуцьке водосховище"]
    current_date = base_time.date()
    for res in reservoirs:
        for h in [6, 12, 18, 23]:
            session.execute(res_stmt, (
                res, current_date, base_time + timedelta(hours=h),
                round(random.uniform(15.2, 22.0), 2),
                round(random.uniform(1200.0, 2400.0), 1),
                round(random.uniform(8.5, 12.0), 1)
            ))

    # Запис денних зведень
    daily_stmt = session.prepare("""
        INSERT INTO daily_station_summaries (
            station_name, summary_date, total_generation_mwh, avg_power_mw, max_power_mw, water_discharged_m3
        ) VALUES (?, ?, ?, ?, ?, ?)
    """)
    for station in stations.keys():
        session.execute(daily_stmt, (
            station, current_date,
            round(random.uniform(5000.0, 18000.0), 1),
            round(random.uniform(250.0, 750.0), 1),
            round(random.uniform(600.0, 1200.0), 1),
            round(random.uniform(1500000.0, 3500000.0), 0)
        ))

    # Запис сезонної аналітики
    season_stmt = session.prepare("""
        INSERT INTO seasonal_station_analytics (
            season_year, station_name, total_flow_km3, avg_efficiency_pct, peak_power_mw
        ) VALUES (?, ?, ?, ?, ?)
    """)
    for station in stations.keys():
        session.execute(season_stmt, (
            "2026_AUTUMN", station,
            round(random.uniform(2.5, 6.2), 2),
            round(random.uniform(88.0, 94.5), 1),
            round(random.uniform(700.0, 1400.0), 1)
        ))

    print(f"Успішно записано {readings_count} оперативних записів телеметрії")

    print("\n5. Аналіз даних (Підваріант А)")
    
    rows = session.execute("SELECT station_name, unit_id, power_mw FROM unit_operational_readings;")
    all_readings = list(rows)

    powers = [r.power_mw for r in all_readings]
    avg_power = sum(powers) / len(powers)
    max_power = max(powers)

    peak_record = max(all_readings, key=lambda x: x.power_mw)

    # Переведення МВт у кВт
    avg_power_kw = avg_power * 1000.0
    max_power_kw = max_power * 1000.0

    print(f"Загальна кількість оброблених записів: {len(all_readings)}")
    print(f"Середня потужність = {avg_power_kw:,.2f} кВт, Максимальна = {max_power_kw:,.2f} кВт".replace(',', ' '))
    print(f"Об'єкт з найвищим показником: {peak_record.station_name} ({peak_record.unit_id}) з генерацією {peak_record.power_mw} МВт.")

    # Приклад останніх зчитаних даних
    print("\nПриклад останніх оперативних вимірювань для Дніпровська ГЕС:")
    sample_rows = session.execute("""
        SELECT recorded_at, water_head_m, water_flow_m3s, power_mw 
        FROM unit_operational_readings 
        WHERE station_name = 'Дніпровська ГЕС' AND unit_id = 'UNIT_01' 
        LIMIT 3;
    """)
    for r in sample_rows:
        print(f"  [{r.recorded_at}] Напір: {r.water_head_m} м | Витрата: {r.water_flow_m3s} м³/с | Потужність: {r.power_mw} МВт")

    cluster.shutdown()

if __name__ == "__main__":
    main()
