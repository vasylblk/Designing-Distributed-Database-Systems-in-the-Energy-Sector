#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import time
import statistics

class BenchmarkRunner:
    def __init__(self):
        self.cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = self.cluster.connect('hydro_analytics')
        self.station = 'HPP_DNIPRO_1'
        self.now = datetime.now() - timedelta(days=2)
        self.b_date = self.now.date()
        self.b_hour = self.now.replace(minute=0, second=0, microsecond=0)

    def measure(self, stmt, params=None, iters=50):
        latencies = []
        prepared = self.session.prepare(stmt)
        for _ in range(iters):
            t0 = time.perf_counter()
            res = self.session.execute(prepared, params or [])
            list(res)
            latencies.append((time.perf_counter() - t0) * 1000.0)

        latencies.sort()
        avg = statistics.mean(latencies)
        p50 = statistics.median(latencies)
        p95 = latencies[int(len(latencies) * 0.95)]
        p99 = latencies[int(len(latencies) * 0.99)]
        return avg, p50, p95, p99

    def run_all(self):
        print(f"{'Схема':<10} | {'Тип запиту':<20} | {'Avg (ms)':<10} | {'P95 (ms)':<10} | {'P99 (ms)':<10}")

        # 1. Simple
        q_simple_latest = "SELECT * FROM telemetry_simple WHERE station_id = ? LIMIT 100"
        q_simple_range = "SELECT * FROM telemetry_simple WHERE station_id = ? AND recorded_at >= ? AND recorded_at <= ?"
        t_start_6h = self.now - timedelta(hours=6)
        t_start_24h = self.now - timedelta(hours=24)

        avg, _, p95, p99 = self.measure(q_simple_latest, [self.station])
        print(f"{'Simple':<10} | {'Latest hour':<20} | {avg:<10.2f} | {p95:<10.2f} | {p99:<10.2f}")

        avg, _, p95, p99 = self.measure(q_simple_range, [self.station, t_start_6h, self.now])
        print(f"{'Simple':<10} | {'6 hour range':<20} | {avg:<10.2f} | {p95:<10.2f} | {p99:<10.2f}")

        avg, _, p95, p99 = self.measure(q_simple_range, [self.station, t_start_24h, self.now])
        print(f"{'Simple':<10} | {'Daily aggregation':<20} | {avg:<10.2f} | {p95:<10.2f} | {p99:<10.2f}")

        # 2. Hourly
        q_hourly_latest = "SELECT * FROM telemetry_hourly WHERE station_id = ? AND bucket_hour = ? LIMIT 100"
        avg, _, p95, p99 = self.measure(q_hourly_latest, [self.station, self.b_hour])
        print(f"{'Hourly':<10} | {'Latest hour':<20} | {avg:<10.2f} | {p95:<10.2f} | {p99:<10.2f}")

        hours = [self.b_hour - timedelta(hours=i) for i in range(6)]
        q_hourly_range = f"SELECT * FROM telemetry_hourly WHERE station_id = ? AND bucket_hour IN ({','.join(['?']*6)})"
        avg, _, p95, p99 = self.measure(q_hourly_range, [self.station] + hours)
        print(f"{'Hourly':<10} | {'6 hour range':<20} | {avg:<10.2f} | {p95:<10.2f} | {p99:<10.2f}")

        q_agg_daily = "SELECT * FROM hydro_hourly_aggregates WHERE station_id = ? LIMIT 24"
        avg, _, p95, p99 = self.measure(q_agg_daily, [self.station])
        print(f"{'Hourly':<10} | {'Daily aggregation':<20} | {avg:<10.2f} | {p95:<10.2f} | {p99:<10.2f}")

        # 3. Daily
        q_daily_latest = "SELECT * FROM telemetry_daily WHERE station_id = ? AND bucket_date = ? LIMIT 100"
        avg, _, p95, p99 = self.measure(q_daily_latest, [self.station, self.b_date])
        print(f"{'Daily':<10} | {'Latest hour':<20} | {avg:<10.2f} | {p95:<10.2f} | {p99:<10.2f}")

        q_daily_range = "SELECT * FROM telemetry_daily WHERE station_id = ? AND bucket_date = ? AND recorded_at >= ? AND recorded_at <= ?"
        avg, _, p95, p99 = self.measure(q_daily_range, [self.station, self.b_date, t_start_6h, self.now])
        print(f"{'Daily':<10} | {'6 hour range':<20} | {avg:<10.2f} | {p95:<10.2f} | {p99:<10.2f}")

        avg, _, p95, p99 = self.measure(q_agg_daily, [self.station])
        print(f"{'Daily':<10} | {'Daily aggregation':<20} | {avg:<10.2f} | {p95:<10.2f} | {p99:<10.2f}")

        print("\nВПЛИВ MATERIALIZED VIEWS")
        q_allow = "SELECT * FROM telemetry_daily WHERE station_id = ? AND bucket_date = ? AND water_flow > 2000.0 ALLOW FILTERING"
        q_mv = "SELECT * FROM mv_critical_high_flow WHERE station_id = ? AND bucket_date = ? AND water_flow > 2000.0"

        avg_allow, _, _, _ = self.measure(q_allow, [self.station, self.b_date], iters=30)
        avg_mv, _, _, _ = self.measure(q_mv, [self.station, self.b_date], iters=30)
        speedup = avg_allow / avg_mv if avg_mv > 0 else 1.0

        print(f"Without MV (ALLOW FILTERING): {avg_allow:.2f} ms")
        print(f"With MV (Materialized View):  {avg_mv:.2f} ms")
        print(f"Прискорення (Speedup):        {speedup:.1f}x")

        self.cluster.shutdown()

if __name__ == "__main__":
    BenchmarkRunner().run_all()
