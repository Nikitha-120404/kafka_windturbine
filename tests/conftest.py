import pytest


@pytest.fixture
def sample_payload():
    return {
        'Turbine_ID': 'Turbine_01',
        'Wind_Speed': 10.0,
        'Power': 800.0,
        'GeneratorTemp': 80.0,
        'Bearing_Temp': 40.0,
        'GearTemp': 120.0,
        'BladePitchAngle': 15.0,
        'Ambient_Air_temp': 22.0,
        'Nacelle_Position': 100.0,
        'Wind_direction': 180.0,
        'GearBoxSumpTemp': 70.0,
        'Generator_Speed': 50.0,
        'Hub_Speed': 3,
        'Timestamp': '2024-01-01T00:00:00+00:00',
    }
