#!/usr/bin/env python3
"""Генератор телеметрії ГЕС Дніпровського каскаду (Варіант 3)"""
import json
import random
from datetime import datetime, timezone

TURBINE_TYPES = ["kaplan", "francis", "pelton"]
STATUSES = ["generating", "standby", "maintenance"]

def generate_hydro_record():
    dev_num = random.randint(1, 15)
    dev_id = f"HYDRO_DN_{dev_num:03d}"
    
    # Специфічні метрики ГЕС
    water_flow = round(random.uniform(500.0, 3000.0), 1)
    water_level = round(random.uniform(10.0, 25.0), 2)
    turbine_type = random.choice(TURBINE_TYPES)
    
    record = {
        "device_id": dev_id,
        "timestamp": datetime.now(timezone.utc).isoformat()[:23] + "Z",
        "power_output": round(random.uniform(10.0, 200.0), 1),
        "efficiency": round(random.uniform(85.0, 95.0), 1),
        "temperature": round(random.uniform(4.0, 20.0), 1),
        "voltage": round(random.uniform(15000.0, 16000.0), 1),
        "current": round(random.uniform(500.0, 8000.0), 1),
        "status": random.choice(STATUSES),
        "location": {
            "lat": round(random.uniform(47.5, 49.0), 4),
            "lon": round(random.uniform(33.0, 36.0), 4)
        },
        "maintenance_hours": random.randint(500, 6000),
        "water_flow": water_flow,
        "water_level": water_level,
        "turbine_type": turbine_type,
        "reserved": ""
    }
    
    # Вирівнювання структури рівно до 256 байт
    raw_json = json.dumps(record, ensure_ascii=False)
    diff = 256 - len(raw_json.encode('utf-8'))
    if diff > 0:
        record["reserved"] = "X" * diff
    return record

if __name__ == "__main__":
    print("Приклад згенерованого запису:")
    rec = generate_hydro_record()
    payload = json.dumps(rec, ensure_ascii=False).encode('utf-8')
    print(json.dumps(rec, indent=2, ensure_ascii=False))
    print(f"Розмір запису: {len(payload)} байт")
