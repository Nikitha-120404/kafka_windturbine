from datetime import datetime

from src.db.connection import get_connection
from src.utils.config import DB


INSERT_SQL = f"""
INSERT INTO {DB.table} (
    time,
    turbine_id,
    wind_speed,
    power_kw,
    generator_temp,
    bearing_temp,
    gear_temp,
    blade_pitch_angle,
    nacelle_position,
    wind_direction,
    ambient_air_temp,
    gearbox_sump_temp,
    generator_speed,
    hub_speed,
    health_status,
    power_efficiency,
    is_anomaly
) VALUES (
    %(time)s,
    %(turbine_id)s,
    %(wind_speed)s,
    %(power_kw)s,
    %(generator_temp)s,
    %(bearing_temp)s,
    %(gear_temp)s,
    %(blade_pitch_angle)s,
    %(nacelle_position)s,
    %(wind_direction)s,
    %(ambient_air_temp)s,
    %(gearbox_sump_temp)s,
    %(generator_speed)s,
    %(hub_speed)s,
    %(health_status)s,
    %(power_efficiency)s,
    %(is_anomaly)s
);
"""


def insert_record(record: dict) -> None:
    mapped = {
        'time': datetime.fromisoformat(record['Timestamp'].replace('Z', '+00:00')),
        'turbine_id': record['Turbine_ID'],
        'wind_speed': record.get('Wind_Speed'),
        'power_kw': record.get('Power'),
        'generator_temp': record.get('GeneratorTemp'),
        'bearing_temp': record.get('Bearing_Temp'),
        'gear_temp': record.get('GearTemp'),
        'blade_pitch_angle': record.get('BladePitchAngle'),
        'nacelle_position': record.get('Nacelle_Position'),
        'wind_direction': record.get('Wind_direction'),
        'ambient_air_temp': record.get('Ambient_Air_temp'),
        'gearbox_sump_temp': record.get('GearBoxSumpTemp'),
        'generator_speed': record.get('Generator_Speed'),
        'hub_speed': record.get('Hub_Speed'),
        'health_status': record.get('health_status'),
        'power_efficiency': record.get('power_efficiency'),
        'is_anomaly': record.get('is_anomaly', False),
    }

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(INSERT_SQL, mapped)
    finally:
        conn.close()
