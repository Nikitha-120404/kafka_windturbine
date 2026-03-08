def enrich(data: dict) -> dict:
    record = dict(data)

    generator_temp = float(record.get('GeneratorTemp', 0))
    gear_temp = float(record.get('GearTemp', 0))

    if generator_temp > 130 or gear_temp > 300:
        record['health_status'] = 'CRITICAL'
    elif generator_temp > 100 or gear_temp > 200:
        record['health_status'] = 'WARNING'
    else:
        record['health_status'] = 'NORMAL'

    wind_speed = float(record.get('Wind_Speed', 0))
    power = float(record.get('Power', 0))
    record['power_efficiency'] = round(power / wind_speed, 3) if wind_speed > 0 else None
    record['is_anomaly'] = wind_speed > 20 and power < 100
    return record
