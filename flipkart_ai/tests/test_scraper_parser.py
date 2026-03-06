from src.extract.parser import parse_reviews_from_html


HTML = """
<div class='col EPCmJX'>
  <div class='XQDdHH'>5</div>
  <p class='z9E0IG'>Awesome</p>
  <div class='ZmyHeo'>Great camera and battery</div>
  <p class='_2NsDsF'>John</p>
  <p class='MztJPv'>Certified Buyer, Delhi</p>
  <p>10 days ago</p>
</div>
"""


def test_parse_reviews_html() -> None:
    rows = parse_reviews_from_html(
        HTML,
        "https://www.flipkart.com/abc/product-reviews/itm?pid=PID123",
        1,
    )
    assert len(rows) == 1
    assert rows[0]["product_id"] == "PID123"
    assert rows[0]["rating"] == 5
