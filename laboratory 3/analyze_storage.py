#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess
import re

TABLES = [
    ("Simple", "telemetry_simple"),
    ("Hourly", "telemetry_hourly"),
    ("Daily", "telemetry_daily")
]

def parse_stats(table_name):
    cmd = f"nodetool tablestats hydro_analytics.{table_name}"
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    out = res.stdout
    
    def extract(pattern, default="0"):
        m = re.search(pattern, out)
        return m.group(1).strip() if m else default

    space_total = extract(r"Space used \(total\):\s*([0-9]+)")
    partitions = extract(r"Number of partitions \(estimate\):\s*([0-9]+)")
    p_mean = extract(r"Compacted partition mean bytes:\s*([0-9]+)")
    p_max = extract(r"Compacted partition maximum bytes:\s*([0-9]+)")
    comp_ratio = extract(r"SSTable Compression Ratio:\s*([0-9,\.]+)")
    
    comp_ratio = comp_ratio.replace(',', '.')

    # Переведення в людиночитабельні одиниці
    total_mb = int(space_total) / (1024 * 1024)
    mean_kb = int(p_mean) / 1024
    max_kb = int(p_max) / 1024
    
    return {
        "total_mb": total_mb,
        "partitions": int(partitions),
        "mean_kb": mean_kb,
        "max_kb": max_kb,
        "comp_ratio": float(comp_ratio) if comp_ratio != "0" else 0.0
    }
  
print("Використання диску та аналіз партицій")
print(f"{'Схема':<10} | {'Total Size (MB)':<16} | {'Partitions':<12} | {'Avg Partition':<15} | {'Max Partition':<15} | {'Compression'}")

for label, tbl in TABLES:
    data = parse_stats(tbl)
    
    # Форматування розміру партицій (КБ або МБ)
    avg_str = f"{data['mean_kb']/1024:.2f} MB" if data['mean_kb'] > 1024 else f"{data['mean_kb']:.1f} KB"
    max_str = f"{data['max_kb']/1024:.2f} MB" if data['max_kb'] > 1024 else f"{data['max_kb']:.1f} KB"
    
    print(f"{label:<10} | {data['total_mb']:>13.2f} MB | {data['partitions']:>12} | {avg_str:>15} | {max_str:>15} | {data['comp_ratio']:>10.3f}")
