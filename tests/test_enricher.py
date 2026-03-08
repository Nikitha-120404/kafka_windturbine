from src.transformer.enricher import enrich


def test_enrich_adds_status_and_efficiency(sample_payload):
    enriched = enrich(sample_payload)
    assert enriched['health_status'] in {'NORMAL', 'WARNING', 'CRITICAL'}
    assert enriched['power_efficiency'] is not None


def test_enrich_marks_anomaly():
    record = {
        'Turbine_ID': 'Turbine_01',
        'Wind_Speed': 22,
        'Power': 80,
        'GeneratorTemp': 90,
        'GearTemp': 120,
        'Timestamp': '2024-01-01T00:00:00+00:00',
    }
    enriched = enrich(record)
    assert enriched['is_anomaly'] is True
