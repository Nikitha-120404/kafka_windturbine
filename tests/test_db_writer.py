from unittest.mock import MagicMock, patch

from src.db.writer import insert_record


def test_insert_record_executes_sql(sample_payload):
    enriched = {
        **sample_payload,
        'health_status': 'NORMAL',
        'power_efficiency': 10.1,
        'is_anomaly': False,
    }

    fake_cursor = MagicMock()
    fake_conn = MagicMock()
    fake_conn.cursor.return_value.__enter__.return_value = fake_cursor
    fake_conn.__enter__.return_value = fake_conn

    with patch('src.db.writer.get_connection', return_value=fake_conn):
        insert_record(enriched)

    assert fake_cursor.execute.called
    assert fake_conn.close.called
