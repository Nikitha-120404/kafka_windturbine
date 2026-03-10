from src.transform.date_normalizer import normalize_review_date


def test_relative_date_parse() -> None:
    dt = normalize_review_date("14 days ago", "2024-01-30T00:00:00+00:00")
    assert dt is not None
    assert dt.day == 16
