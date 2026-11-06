#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import time
import statistics

def test_mv():
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('hydro_analytics')
    station = 'HPP_DNIPRO_1'
    target_date = (datetime.now() - timedelta(days=2)).date()
    iters = 30

    def measure(query, params):
        latencies = []
        stmt = session.prepare(query)
        for _ in range(iters):
            t0 = time.perf_counter()
            res = session.execute(stmt, params)
            list(res)
            latencies.append((time.perf_counter() - t0) * 1000.0)
        return statistics.mean(latencies)

    print("Вимірювання часу виконання фільтрів")

    # 1. Запит 1: Піковий скид води (High Water Flow)
    q1_without = """
        SELECT * FROM telemetry_daily 
        WHERE station_id = ? AND bucket_date = ? AND water_flow > 2000.0 
        ALLOW FILTERING
    """
    q1_with = """
        SELECT * FROM mv_critical_high_flow 
        WHERE station_id = ? AND bucket_date = ? AND water_flow > 2000.0
    """
    t1_without = measure(q1_without, [station, target_date])
    t1_with = measure(q1_with, [station, target_date])
    speedup1 = t1_without / t1_with if t1_with > 0 else 1.0

    # 2. Запит 2: Високий рівень води (High Water Level)
    q2_without = """
        SELECT * FROM telemetry_daily 
        WHERE station_id = ? AND bucket_date = ? AND water_level > 50.5 
        ALLOW FILTERING
    """
    q2_with = """
        SELECT * FROM mv_high_water_level 
        WHERE station_id = ? AND bucket_date = ? AND water_level > 50.5
    """
    t2_without = measure(q2_without, [station, target_date])
    t2_with = measure(q2_with, [station, target_date])
    speedup2 = t2_without / t2_with if t2_with > 0 else 1.0

    # Вивід підсумкової Таблиці 4
    print("Вплив Materialized Views")
    print(f"{'Query Type':<35} | {'Without MV (ms)':<16} | {'With MV (ms)':<14} | {'Improvement':<12}")
    print(f"{'High water flow filter (> 2000)':<35} | {t1_without:>13.2f} ms | {t1_with:>11.2f} ms | {speedup1:>9.1f}x")
    print(f"{'High water level filter (> 50.5)':<35} | {t2_without:>13.2f} ms | {t2_with:>11.2f} ms | {speedup2:>9.1f}x")

    cluster.shutdown()

if __name__ == "__main__":
    test_mv()
