from src.transformer.validator import validate_message


def test_validate_message_valid_payload(sample_payload):
    is_valid, reason = validate_message(sample_payload)
    assert is_valid is True
    assert isinstance(reason, str)


def test_validate_message_missing_field_fails(sample_payload):
    sample_payload.pop('Power')
    is_valid, reason = validate_message(sample_payload)
    assert is_valid is False
    assert 'required' in reason.lower()
