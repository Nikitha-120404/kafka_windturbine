from src.utils.hashing import stable_review_id


def test_stable_review_id_is_deterministic() -> None:
    a = stable_review_id("PID", "Alice", "Great", "Nice phone")
    b = stable_review_id("PID", "Alice", "Great", "Nice phone")
    assert a == b
