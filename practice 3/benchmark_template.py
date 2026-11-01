#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Шаблон для benchmark-тестування ПРОДУКТИВНОСТІ Cassandra схем
Порівнює ПОГАНУ схему (Simple Wide Row) з ХОРОШОЮ схемою (Hourly Bucketing)
"""

from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import time
import statistics

class PerformanceTester:
    """Клас для тестування продуктивності"""
    def __init__(self, keyspace):
        self.cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = self.cluster.connect(keyspace)

    def measure_read_latency(self, query, params, iterations=50):
        """
        Вимірює латентність читання
        Повертає: avg, p50, p95, p99
        """
        latencies = []

        if '?' in query:
            statement = self.session.prepare(query)
        else:
            statement = query

        print(f"\n Вимірювання латентності ({iterations} ітерацій)...")
        for i in range(iterations):
            start = time.perf_counter()
            if isinstance(statement, str):
                result = self.session.execute(statement)
            else:
                result = self.session.execute(statement, params or [])

            list(result) # отримати всі дані
            end = time.perf_counter()
            latency_ms = (end - start) * 1000.0
            latencies.append(latency_ms)
            if (i + 1) % 10 == 0:
                print(f" Прогрес: {i+1}/{iterations}")

        avg = statistics.mean(latencies)
        p50 = statistics.median(latencies)
        p95 = self._percentile(latencies, 0.95)
        p99 = self._percentile(latencies, 0.99)
        return {
            'avg': avg,
            'p50': p50,
            'p95': p95,
            'p99': p99,
            'min': min(latencies),
            'max': max(latencies)
        }

    def _percentile(self, data, percentile):
        """Обчислює перцентиль"""
        sorted_data = sorted(data)
        index = int(len(sorted_data) * percentile)
        return sorted_data[min(index, len(sorted_data) - 1)]

    def print_results(self, name, results):
        """Виводить результати у вигляді таблиці"""
        print(f"\nРезультати для {name}:")
        print("=" * 50)
        print(f" Середня (avg): {results['avg']:>8.2f} ms")
        print(f" Медіана (p50): {results['p50']:>8.2f} ms")
        print(f" P95:          {results['p95']:>8.2f} ms")
        print(f" P99:          {results['p99']:>8.2f} ms")
        print(f" Мінімум:      {results['min']:>8.2f} ms")
        print(f" Максимум:     {results['max']:>8.2f} ms")
        print("=" * 50)

    def close(self):
        self.cluster.shutdown()

def demo_benchmark():
    tester = PerformanceTester('demo_practice')

    # Тест 1: Погана схема (сканування wide partition)
    print("\n Тестування ПОГАНОЇ схеми...")
    query_bad = """
        SELECT * FROM telemetry_bad
        WHERE device_id = ?
        ORDER BY recorded_at DESC
        LIMIT 100
    """
    results_bad = tester.measure_read_latency(
        query_bad,
        ['DEMO_SOLAR_001'],
        iterations=50
    )
    tester.print_results("Simple Wide Row (ПОГАНА)", results_bad)

    # Тест 2: Хороша схема (читання окремої ізольованої партиції)
    print("\n Тестування ХОРОШОЇ схеми...")
    query_good = """
        SELECT * FROM telemetry_hourly
        WHERE device_id = ?
        AND bucket_hour = ?
        ORDER BY recorded_at DESC
        LIMIT 100
    """
    # Беремо завершену годину з наявними даними
    target_hour = (datetime.now() - timedelta(hours=2)).replace(minute=0, second=0, microsecond=0)
    results_good = tester.measure_read_latency(
        query_good,
        ['DEMO_SOLAR_001', target_hour],
        iterations=50
    )
    tester.print_results("Hourly Bucketing (ХОРОША)", results_good)

    # Порівняння
    print("\n ПОРІВНЯННЯ:")
    print("=" * 50)
    improvement = (results_bad['p95'] / results_good['p95']) if results_good['p95'] > 0 else 1.0
    print(f"Прискорення (P95): {improvement:.1f}x швидше!")
    print("=" * 50)
    tester.close()

if __name__ == "__main__":
    demo_benchmark()
