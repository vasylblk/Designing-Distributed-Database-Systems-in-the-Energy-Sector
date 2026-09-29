#!/usr/bin/env python3
"""Симуляція критичних алертів ГЕС та вимірювання Jitter для SCADA"""
import time
import json
import statistics
from kafka import KafkaProducer

ALERT_TOPIC = "hydro-main"

producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    api_version=(3, 7, 1),
    acks=1,
    batch_size=8192,   # 8KB для швидкої відправки
    linger_ms=0,       # 0ms - негайна відправка без затримок
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

latencies = []
print("Запуск симуляції критичних алертів SCADA (100 подій)")

for i in range(100):
    alert_event = {
        "device_id": f"HYDRO_DN_{i%15 + 1:03d}",
        "timestamp": time.time(),
        "event_type": "CRITICAL_ALERT",
        "parameter": "water_level",
        "value": 24.85,  # Критичний рівень біля дамби (> 24.0 м)
        "severity": "HIGH",
        "action_required": "OPEN_SPILLWAY_GATE"
    }
    
    t_start = time.perf_counter()
    future = producer.send(ALERT_TOPIC, alert_event)
    future.get(timeout=5)  # чекаємо підтвердження брокера
    rtt_ms = (time.perf_counter() - t_start) * 1000
    latencies.append(rtt_ms)
    time.sleep(0.01)

producer.close()

# Розрахунок метрик Jitter та перцентилів
latencies.sort()
p50 = statistics.median(latencies)
p95 = latencies[int(len(latencies) * 0.95)]
jitter = statistics.stdev(latencies)

print("\nРЕЗУЛЬТАТИ SCADA АНАЛІЗУ ДЛЯ ПІДВАРІАНТА A")
print(f"Мінімальна затримка: {min(latencies):.2f} ms")
print(f"50th percentile (медіана): {p50:.2f} ms")
print(f"95th percentile: {p95:.2f} ms")
print(f"Максимальна затримка: {max(latencies):.2f} ms")
print(f"Network Jitter (стандартне відхилення): {jitter:.2f} ms")