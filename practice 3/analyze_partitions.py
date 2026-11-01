#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Аналіз розміру партицій Cassandra для ПОГАНОЇ та ХОРОШОЇ схем
"""

from cassandra.cluster import Cluster
import subprocess

def analyze_table_stats(keyspace, table):
    """Отримує статистику таблиці через nodetool"""
    print(f"\n Аналіз таблиці {keyspace}.{table}")
    print("=" * 60)
    cmd = f"nodetool tablestats {keyspace}.{table}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    # Парсимо вивід
    for line in result.stdout.split('\n'):
        if any(keyword in line.lower() for keyword in [
            'table:', 'partition', 'compacted', 'space used'
        ]):
            print(f" {line.strip()}")
    print("=" * 60)

def count_partitions(keyspace, table, partition_key_columns):
    """Підраховує кількість унікальних партицій"""
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect(keyspace)
    columns = ', '.join(partition_key_columns)
    query = f"SELECT DISTINCT {columns} FROM {table}"
    print(f"\n Підрахунок партицій...")
    try:
        result = session.execute(query)
        partitions = list(result)
        count = len(partitions)
        print(f" Знайдено {count} унікальних партицій")
        if count > 0:
            print(f"\nПриклади партицій (перші 5):")
            for i, partition in enumerate(partitions[:5], 1):
                print(f" {i}. {partition}")
        return count
    except Exception as e:
        print(f" Помилка: {e}")
        return 0
    finally:
        cluster.shutdown()

def main():
    print(" Аналіз розміру партицій Cassandra")
    print("=" * 60)

    # Аналіз ПОГАНОЇ схеми
    print("\n ПОГАНА СХЕМА (Simple Wide Row)")
    analyze_table_stats('demo_practice', 'telemetry_bad')
    count_bad = count_partitions(
        'demo_practice',
        'telemetry_bad',
        ['device_id']
    )

    # Аналіз ХОРОШОЇ схеми
    print("\n ХОРОША СХЕМА (Hourly Bucketing)")
    analyze_table_stats('demo_practice', 'telemetry_hourly')
    count_good = count_partitions(
        'demo_practice',
        'telemetry_hourly',
        ['device_id', 'bucket_hour']
    )

    # Висновок
    print("\n ВИСНОВОК:")
    print("=" * 60)
    print(f" Погана схема: {count_bad} партиція (величезна!)")
    print(f" Хороша схема: {count_good} партицій (оптимально!)")
    if count_bad > 0:
        print(f" Покращення: {count_good / count_bad:.0f}x більше партицій")
    print("=" * 60)

if __name__ == "__main__":
    main()
