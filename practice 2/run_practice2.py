#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from cassandra.cluster import Cluster

def print_separator(title):
    print("\n")
    print(f" {title}")
    

def main():
    print_separator("КРОК 0: ПІДКЛЮЧЕННЯ ДО APACHE CASSANDRA")
    try:
        cluster = Cluster(['127.0.0.1'], port=9042)
        session = cluster.connect()
        print("Успішно підключено до вузла 127.0.0.1:9042")
    except Exception as e:
        print(f"Помилка підключення: {e}")
        sys.exit(1)


    # 1. СТВОРЕННЯ KEYSPACE
    print_separator("КРОК 1: СТВОРЕННЯ ТА НАЛАШТУВАННЯ KEYSPACE")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS energy_monitoring 
        WITH replication = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        };
    """)
    print("Виконано")
    
    session.set_keyspace('energy_monitoring')
    print("Виконано: USE energy_monitoring;")

    # Перевірка списку keyspaces
    rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces;")
    ks_list = [r.keyspace_name for r in rows]
    print(f"Перевірка DESCRIBE KEYSPACES: знайдено {len(ks_list)} просторів імен.")
    if 'energy_monitoring' in ks_list:
        print("energy_monitoring успішно присутній у кластері!")

    # 2. СТВОРЕННЯ ТАБЛИЦЬ
    print_separator("КРОК 2: СТВОРЕННЯ ТАБЛИЦЬ")

    # 2.1 Таблиця електростанцій
    session.execute("""
    CREATE TABLE IF NOT EXISTS power_stations (
        station_id UUID PRIMARY KEY,
        station_name TEXT,
        station_type TEXT,
        location TEXT,
        region TEXT,
        max_capacity_mw DOUBLE,
        commissioned_date DATE,
        status TEXT,
        coordinates MAP<TEXT, DOUBLE>,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );
    """)
    print("Створено таблицю: power_stations")

    # 2.2 Таблиця телеметрії (часові ряди, TTL 7 днів = 604800)
    session.execute("""
    CREATE TABLE IF NOT EXISTS station_telemetry (
        station_id UUID,
        time_bucket TIMESTAMP,
        recorded_at TIMESTAMP,
        power_output_mw DOUBLE,
        voltage_kv DOUBLE,
        frequency_hz DOUBLE,
        temperature_c DOUBLE,
        efficiency_percent DOUBLE,
        fuel_consumption_rate DOUBLE,
        status TEXT,
        alert_level INT,
        PRIMARY KEY ((station_id, time_bucket), recorded_at)
    ) WITH CLUSTERING ORDER BY (recorded_at DESC)
      AND default_time_to_live = 604800;
    """)
    print("Створено таблицю: station_telemetry (з TTL = 604800)")

    # 2.3 Таблиця попереджень
    session.execute("""
    CREATE TABLE IF NOT EXISTS station_alerts (
        station_id UUID,
        alert_date DATE,
        alert_id TIMEUUID,
        alert_timestamp TIMESTAMP,
        severity TEXT,
        alert_type TEXT,
        message TEXT,
        current_value DOUBLE,
        threshold_value DOUBLE,
        acknowledged BOOLEAN,
        resolved BOOLEAN,
        PRIMARY KEY ((station_id, alert_date), alert_id)
    ) WITH CLUSTERING ORDER BY (alert_id DESC);
    """)
    print("Створено таблицю: station_alerts")

    # 2.4 Таблиця лічильників (COUNTER)
    session.execute("""
    CREATE TABLE IF NOT EXISTS hourly_alert_counts (
        station_id UUID,
        date DATE,
        hour TIMESTAMP,
        alert_count COUNTER,
        PRIMARY KEY ((station_id, date), hour)
    );
    """)
    print("Створено таблицю: hourly_alert_counts (з типом COUNTER)")

    # 2.5 Таблиця агрегованих даних
    session.execute("""
    CREATE TABLE IF NOT EXISTS hourly_statistics (
        station_id UUID,
        date DATE,
        hour TIMESTAMP,
        avg_power_mw DOUBLE,
        max_power_mw DOUBLE,
        min_power_mw DOUBLE,
        total_energy_mwh DOUBLE,
        avg_efficiency_percent DOUBLE,
        uptime_minutes INT,
        PRIMARY KEY ((station_id, date), hour)
    ) WITH CLUSTERING ORDER BY (hour DESC);
    """)
    print("Створено таблицю: hourly_statistics")

    # 3. СТВОРЕННЯ ІНДЕКСІВ
    print_separator("КРОК 3: СТВОРЕННЯ ВТОРИННИХ ІНДЕКСІВ (SECONDARY INDEXES)")
    indexes = [
        ("station_type_idx", "power_stations (station_type)"),
        ("station_region_idx", "power_stations (region)"),
        ("station_status_idx", "power_stations (status)"),
        ("alert_level_idx", "station_telemetry (alert_level)"),
        ("station_alerts_severity_idx", "station_alerts (severity)")
    ]
    for idx_name, target in indexes:
        session.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {target};")
        print(f"Створено індекс: {idx_name} ON {target}")

    # 4. ДОДАВАННЯ ТЕСТОВИХ ДАНИХ ТА ПЕРЕВІРКА
    print_separator("КРОК 4: ДОДАВАННЯ ТЕСТОВИХ ДАНИХ ТА ПЕРВИННА ПЕРЕВІРКА")
    
    # 4.1 Додаємо електростанції
    session.execute("""
    INSERT INTO power_stations (
        station_id, station_name, station_type, location, region,
        max_capacity_mw, commissioned_date, status, coordinates, 
        created_at, updated_at
    ) VALUES (
        123e4567-e89b-12d3-a456-426614174000,
        'Київська ТЕС', 'thermal', 'Київ', 'Центральний',
        1200.0, '2010-05-15', 'active',
        {'lat': 50.4501, 'lon': 30.5234},
        toTimestamp(now()), toTimestamp(now())
    );
    """)
    session.execute("""
    INSERT INTO power_stations (
        station_id, station_name, station_type, location, region,
        max_capacity_mw, commissioned_date, status, coordinates,
        created_at, updated_at
    ) VALUES (
        223e4567-e89b-12d3-a456-426614174001,
        'Дніпровська ГЕС', 'hydro', 'Запоріжжя', 'Південний',
        1548.0, '1932-10-10', 'active',
        {'lat': 47.8388, 'lon': 35.1396},
        toTimestamp(now()), toTimestamp(now())
    );
    """)
    session.execute("""
    INSERT INTO power_stations (
        station_id, station_name, station_type, location, region,
        max_capacity_mw, commissioned_date, status, coordinates,
        created_at, updated_at  
    ) VALUES (
        323e4567-e89b-12d3-a456-426614174002,
        'Сонячна станція Нікополь', 'solar', 'Нікополь', 'Південний',
        246.0, '2019-12-01', 'active',
        {'lat': 47.5659, 'lon': 34.3975},
        toTimestamp(now()), toTimestamp(now())
    );
    """)
    print("Додано 3 станції (Київська ТЕС, Дніпровська ГЕС, Нікополь)")

    # 4.2 Додаємо телеметрію
    session.execute("""
    INSERT INTO station_telemetry (
        station_id, time_bucket, recorded_at, power_output_mw,
        voltage_kv, frequency_hz, temperature_c, efficiency_percent,
        fuel_consumption_rate, status, alert_level
    ) VALUES (
        123e4567-e89b-12d3-a456-426614174000,
        '2025-09-17 14:00:00', '2025-09-17 14:15:30',
        1150.5, 220.1, 50.01, 75.2, 87.5, 2.8, 'online', 0
    );
    """)
    session.execute("""
    INSERT INTO station_telemetry (
        station_id, time_bucket, recorded_at, power_output_mw,
        voltage_kv, frequency_hz, temperature_c, efficiency_percent,
        fuel_consumption_rate, status, alert_level
    ) VALUES (
        123e4567-e89b-12d3-a456-426614174000,
        '2025-09-17 14:00:00', '2025-09-17 14:16:30',
        1145.8, 219.8, 49.99, 76.1, 87.2, 2.9, 'online', 0
    );
    """)
    session.execute("""
    INSERT INTO station_telemetry (
        station_id, time_bucket, recorded_at, power_output_mw,
        voltage_kv, frequency_hz, temperature_c, efficiency_percent,
        status, alert_level
    ) VALUES (
        223e4567-e89b-12d3-a456-426614174001,
        '2025-09-17 14:00:00', '2025-09-17 14:15:45',
        800.2, 400.5, 50.00, 22.3, 92.1, 'online', 0
    );
    """)

    session.execute("""
    INSERT INTO station_telemetry (
        station_id, time_bucket, recorded_at, power_output_mw,
        voltage_kv, frequency_hz, temperature_c, efficiency_percent,
        status, alert_level
    ) VALUES (
        123e4567-e89b-12d3-a456-426614174000,
        '2025-09-17 14:00:00', '2025-09-17 14:20:00',
        950.0, 215.0, 49.80, 85.0, 80.0, 'warning', 2
    );
    """)
    print("Додано 4 записи телеметрії")

    # 4.3 Додаємо попередження
    session.execute("""
    INSERT INTO station_alerts (
        station_id, alert_date, alert_id, alert_timestamp,
        severity, alert_type, message, current_value, 
        threshold_value, acknowledged, resolved
    ) VALUES (
        123e4567-e89b-12d3-a456-426614174000,
        '2025-09-17', now(), toTimestamp(now()),
        'critical', 'temperature', 'Температура перевищує норму',
        78.5, 75.0, false, false
    );
    """)
    print("Додано запис попередження (station_alerts)")

    # 4.4 Первинна перевірка даних
    print("\nПеревірка даних розділу 4")
    print("1. Перевірка конкретної станції (Київська ТЕС):")
    row = session.execute("""
        SELECT station_id, station_name, station_type, region, status 
        FROM power_stations 
        WHERE station_id = 123e4567-e89b-12d3-a456-426614174000;
    """).one()
    print(f"   {row.station_id} | {row.station_name} | {row.station_type} | {row.region} | {row.status}")

    print("\n2. Перевірка телеметрії Київської ТЕС (time_bucket = '2025-09-17 14:00:00'):")
    rows = session.execute("""
        SELECT recorded_at, power_output_mw, voltage_kv, status, alert_level 
        FROM station_telemetry 
        WHERE station_id = 123e4567-e89b-12d3-a456-426614174000 
          AND time_bucket = '2025-09-17 14:00:00';
    """)
    for r in rows:
        print(f"   Час: {r.recorded_at} | Потужність: {r.power_output_mw} МВт | Напруга: {r.voltage_kv} кВ | Alert: {r.alert_level}")

    print("\n3. Перевірка кількості записів (COUNT):")
    c_stat = session.execute("SELECT COUNT(*) FROM power_stations;").one()[0]
    c_telem = session.execute("SELECT COUNT(*) FROM station_telemetry;").one()[0]
    c_alert = session.execute("SELECT COUNT(*) FROM station_alerts;").one()[0]
    print(f"   power_stations: {c_stat} | station_telemetry: {c_telem} | station_alerts: {c_alert}")

    # 5. ЗАПИТИ ДЛЯ АНАЛІТИКИ
    print_separator("КРОК 5: ЗАПИТИ ДЛЯ АНАЛІТИКИ")

    # Запит 1: Перегляд усіх активних станцій
    print("Запит 5.1: Усі активні станції ")
    rows = session.execute("SELECT station_name, station_type, max_capacity_mw, location FROM power_stations WHERE status = 'active';")
    for r in rows:
        print(f"   * {r.station_name:<26} | Тип: {r.station_type:<8} | Потужність: {r.max_capacity_mw} МВт | Локація: {r.location}")

    # Запит 2: Останні показники конкретної станції
    print("\nЗапит 5.2: Останні показники станції")
    rows = session.execute("""
        SELECT recorded_at, power_output_mw, voltage_kv, efficiency_percent 
        FROM station_telemetry 
        WHERE station_id = 123e4567-e89b-12d3-a456-426614174000 
          AND time_bucket = '2025-09-17 14:00:00' 
        ORDER BY recorded_at DESC 
        LIMIT 10;
    """)
    for r in rows:
        print(f"   * {r.recorded_at} | Потужність: {r.power_output_mw} МВт | Напруга: {r.voltage_kv} кВ | ККД: {r.efficiency_percent}%")

    # Запит 3: Середні показники за годину (GROUP BY)
    print("\nЗапит 5.3: Середні показники за годину")
    rows = session.execute("""
        SELECT station_id, time_bucket, 
               AVG(power_output_mw) as avg_power, 
               AVG(efficiency_percent) as avg_efficiency, 
               COUNT(*) as measurements_count 
        FROM station_telemetry 
        WHERE station_id = 123e4567-e89b-12d3-a456-426614174000 
          AND time_bucket = '2025-09-17 14:00:00' 
        GROUP BY station_id, time_bucket;
    """)
    for r in rows:
        print(f"   * Станція: {r.station_id} | Час: {r.time_bucket}")
        print(f"    Середня потужність: {r.avg_power:.2f} МВт | Сер. ККД: {r.avg_efficiency:.2f}% | Замірів: {r.measurements_count}")

    # Запит 4: Критичні попередження за сьогодні
    print("\nЗапит 5.4: Критичні попередження")
    rows = session.execute("""
        SELECT station_id, alert_id, alert_timestamp, alert_type, message, current_value 
        FROM station_alerts 
        WHERE station_id = 123e4567-e89b-12d3-a456-426614174000 
          AND alert_date = '2025-09-17' 
          AND severity = 'critical' 
        ALLOW FILTERING;
    """)
    for r in rows:
        print(f"   * Alert ID: {r.alert_id} | Тип: {r.alert_type} | Значення: {r.current_value} | Повідомлення: '{r.message}'")

    # Запит 5: Станції з найвищою потужністю
    print("\nЗапит 5.5: Станції")
    rows = session.execute("SELECT station_name, station_type, max_capacity_mw FROM power_stations WHERE status = 'active' LIMIT 5 ALLOW FILTERING;")
    for r in rows:
        print(f"   * {r.station_name:<26} | Потужність: {r.max_capacity_mw} МВт")

    # Запит 6: Пошук станцій у конкретному регіоні
    print("\nЗапит 5.6: Станції у регіоні 'Південний':")
    rows = session.execute("SELECT station_name, location, max_capacity_mw FROM power_stations WHERE region = 'Південний';")
    for r in rows:
        print(f"   * {r.station_name:<26} | Місто: {r.location:<10} | Потужність: {r.max_capacity_mw} МВт")

    # Запит 7: Станції з проблемами (alert_level > 0)
    print("\nЗапит 5.7: Телеметрія з проблемами")
    rows = session.execute("SELECT station_id, time_bucket, alert_level FROM station_telemetry WHERE alert_level > 0 ALLOW FILTERING;")
    for r in rows:
        print(f"   * Станція ID: {r.station_id} | Час: {r.time_bucket} | Рівень тривоги: {r.alert_level}")

    # 6. ОПЕРАЦІЇ ОБСЛУГОВУВАННЯ
    print_separator("КРОК 6: ОПЕРАЦІЇ ОБСЛУГОВУВАННЯ")

    # 6.1 Оновлення статусу станції
    print("6.1 Оновлення статусу Сонячної станції Нікополь на 'maintenance':")
    session.execute("""
        UPDATE power_stations 
        SET status = 'maintenance', updated_at = toTimestamp(now()) 
        WHERE station_id = 323e4567-e89b-12d3-a456-426614174002;
    """)
    r = session.execute("SELECT station_name, status, updated_at FROM power_stations WHERE station_id = 323e4567-e89b-12d3-a456-426614174002;").one()
    print(f"   Результат: {r.station_name} | Статус: {r.status} | Оновлено о: {r.updated_at}")

    # 6.2 Вирішення попередження (з вибіркою точного alert_id)
    print("\n6.2 Вирішення попередження ")
    cur_alert = session.execute("""
        SELECT alert_id FROM station_alerts 
        WHERE station_id = 123e4567-e89b-12d3-a456-426614174000 
          AND alert_date = '2025-09-17' LIMIT 1;
    """).one()
    if cur_alert:
        actual_id = cur_alert.alert_id
        session.execute(f"""
            UPDATE station_alerts 
            SET acknowledged = true, resolved = true 
            WHERE station_id = 123e4567-e89b-12d3-a456-426614174000 
              AND alert_date = '2025-09-17' 
              AND alert_id = {actual_id};
        """)
        updated_alert = session.execute(f"""
            SELECT alert_id, acknowledged, resolved FROM station_alerts 
            WHERE station_id = 123e4567-e89b-12d3-a456-426614174000 
              AND alert_date = '2025-09-17' 
              AND alert_id = {actual_id};
        """).one()
        print(f"   Попередження {updated_alert.alert_id} успішно оновлено:")
        print(f"       acknowledged = {updated_alert.acknowledged}, resolved = {updated_alert.resolved}")

    # 6.3 Демонстрація створення таблиці з TWCS та TTL 30 днів
    print("\n6.3 Створення спеціалізованої архів-таблиці")
    session.execute("""
    CREATE TABLE IF NOT EXISTS station_telemetry_twcs (
        station_id UUID,
        time_bucket TIMESTAMP,
        recorded_at TIMESTAMP,
        power_output_mw DOUBLE,
        PRIMARY KEY ((station_id, time_bucket), recorded_at)
    ) WITH CLUSTERING ORDER BY (recorded_at DESC)
      AND compaction = {
        'class': 'TimeWindowCompactionStrategy',
        'compaction_window_unit': 'DAYS',
        'compaction_window_size': 1
      }
      AND default_time_to_live = 2592000;
    """)
    print("   Створено таблицю station_telemetry_twcs (TWCS + TTL 30 днів = 2592000 с)")

    # 6.4 Інкремент лічильника попереджень (COUNTER)
    print("\n6.4 Інкремент лічильника попереджень ")
    session.execute("""
        UPDATE hourly_alert_counts 
        SET alert_count = alert_count + 1 
        WHERE station_id = 123e4567-e89b-12d3-a456-426614174000 
          AND date = '2025-09-17' 
          AND hour = '2025-09-17 14:00:00';
    """)
    cnt = session.execute("""
        SELECT station_id, date, hour, alert_count 
        FROM hourly_alert_counts 
        WHERE station_id = 123e4567-e89b-12d3-a456-426614174000 
          AND date = '2025-09-17' 
          AND hour = '2025-09-17 14:00:00';
    """).one()
    print(f"   Значення alert_count для станції: {cnt.alert_count}")


    # 7. ПЕРЕВІРКА ПРОДУКТИВНОСТІ У CASSANDRA
    print_separator("КРОК 7: ПЕРЕВІРКА ПРОДУКТИВНОСТІ")

    # 7.1 Хеш-токени Murmur3Partitioner
    print("7.1 Розподілення даних по токенах ")
    rows = session.execute("SELECT token(station_id) as tok, station_id, station_name FROM power_stations;")
    for r in rows:
        print(f"   * Хеш-токен: {r.tok:<21} | Станція: {r.station_name}")

    # 7.2 Перевірка TTL
    print("\n7.2 Перевірка TTL для телеметрії ")
    rows = session.execute("SELECT station_id, recorded_at, TTL(power_output_mw) as ttl_val FROM station_telemetry LIMIT 4;")
    for r in rows:
        print(f"   * Запис: {r.recorded_at} | Залишок часу життя: {r.ttl_val} сек. (з початкових 604800)")

    # 7.3 Інспекція параметрів таблиці (DESCRIBE TABLE)
    print("\n7.3 Параметри таблиці station_telemetry")
    t_info = session.execute("""
        SELECT table_name, default_time_to_live, bloom_filter_fp_chance, caching, compaction, compression 
        FROM system_schema.tables 
        WHERE keyspace_name = 'energy_monitoring' AND table_name = 'station_telemetry';
    """).one()
    if t_info:
        print(f"   * Таблиця: {t_info.table_name}")
        print(f"   * Default TTL: {t_info.default_time_to_live} с (7 днів)")
        print(f"   * Bloom filter fp chance: {t_info.bloom_filter_fp_chance}")
        print(f"   * Caching: {t_info.caching}")
        print(f"   * Compaction: {t_info.compaction['class']}")
        print(f"   * Compression: {t_info.compression['class']}")

    cluster.shutdown()
    print_separator("Виконано")

if __name__ == "__main__":
    main()
