from jsonschema import ValidationError, validate

TURBINE_SCHEMA = {
    'type': 'object',
    'required': [
        'Turbine_ID',
        'Wind_Speed',
        'Power',
        'GeneratorTemp',
        'Bearing_Temp',
        'GearTemp',
        'BladePitchAngle',
        'Timestamp',
    ],
    'properties': {
        'Turbine_ID': {'type': 'string'},
        'Wind_Speed': {'type': 'number', 'minimum': 0, 'maximum': 30},
        'Power': {'type': 'number', 'minimum': 0, 'maximum': 2000},
        'GeneratorTemp': {'type': 'number', 'minimum': 0, 'maximum': 200},
        'Bearing_Temp': {'type': 'number', 'minimum': 0, 'maximum': 100},
        'GearTemp': {'type': 'number', 'minimum': 0, 'maximum': 400},
        'BladePitchAngle': {'type': 'number', 'minimum': 0, 'maximum': 90},
        'Ambient_Air_temp': {'type': 'number', 'minimum': -50, 'maximum': 60},
        'Timestamp': {'type': 'string'},
    },
}


def validate_message(data: dict) -> tuple[bool, str]:
    try:
        validate(instance=data, schema=TURBINE_SCHEMA)
    except ValidationError as exc:
        return False, str(exc.message)

    if data['Wind_Speed'] > 20 and data['Power'] < 100:
        return True, 'anomaly_candidate_high_wind_low_power'
    return True, ''
