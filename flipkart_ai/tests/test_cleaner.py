from src.transform.cleaner import clean_review_text


def test_clean_text() -> None:
    assert clean_review_text("Hello READ MORE   World") == "hello world"
