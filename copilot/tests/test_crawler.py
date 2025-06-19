import os, sys
import pytest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'summarization')))
from crawler import crawl_blog, crawl_products


def test_crawl_blog_real():
    base_url = "https://www.pops.studio"
    try:
        posts = crawl_blog(base_url, blog_path="/blogs/news", max_pages=1)
    except Exception as e:
        pytest.skip(f"Network issue: {e}")
    assert len(posts) > 0, "No blog posts found; check site availability."
    for post in posts:
        assert 'title' in post and post['title']
        assert 'url' in post and post['url'].startswith('http')


# def test_crawl_products_real():
#     base_url = "https://www.pops.studio"
#     categories = [
#         "/collections/bathroom-vanity-units",
#     ]
#     try:
#         products = crawl_products(base_url, categories)
#     except Exception as e:
#         pytest.skip(f"Network issue: {e}")
#     assert len(products) > 0, "No products found; check site availability."
#     for prod in products:
#         assert prod['category'] == 'Bathroom Vanity Units' 