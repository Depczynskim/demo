import json
import os
import time
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))
from logger import get_logger

# For changelog update
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'changes')))
from copilot.automation.update_changelog import main as update_changelog_main

logger = get_logger(__name__)

# --- HTTP helpers -----------------------------------------------------------

def fetch_url(url: str, retries: int = 3, backoff: float = 2.0, timeout: int = 10) -> str | None:
    """Fetch URL with simple exponential back-off retry. Returns HTML text or None."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Attempt {attempt}/{retries} failed for {url}: {e}")
            if attempt != retries:
                time.sleep(backoff * attempt)
    logger.error(f"Failed to fetch {url} after {retries} attempts.")
    return None

def crawl_blog(base_url: str, blog_path: str = "/blogs/news", max_pages: int | None = None) -> List[Dict]:
    """
    Crawl the blog index page and extract metadata for each blog post.

    Parameters
    ----------
    base_url : str
        Root site URL (e.g. "https://www.pops.studio").
    blog_path : str, default "/blogs/news"
        Path to the blog listing archive.
    max_pages : int | None, default None
        If set, stop crawling after this many pages. Useful for unit tests to
        avoid slow full-site crawls.

    Returns
    -------
    List[Dict]
        Each dict contains title, url, and publish_date keys.
    """
    seen_urls: set[str] = set()
    posts: List[Dict] = []

    page_num = 1
    while True:
        url = urljoin(base_url, f"{blog_path}?page={page_num}") if page_num > 1 else urljoin(base_url, blog_path)
        html = fetch_url(url)
        if html is None:
            break
        soup = BeautifulSoup(html, "html.parser")
        # Target div.card__information for each blog post
        for info in soup.select("div.card__information"):
            title_tag = info.select_one("h3.card__heading a.full-unstyled-link")
            date_tag = info.select_one("div.article-card__info time")
            title = title_tag.get_text(strip=True) if title_tag else None
            url = urljoin(base_url, title_tag['href']) if title_tag and title_tag.has_attr('href') else None
            publish_date = date_tag.get_text(strip=True) if date_tag else None
            if url and url not in seen_urls and title:
                posts.append({
                    "title": title,
                    "url": url,
                    "publish_date": publish_date
                })
                seen_urls.add(url)
        
        # Check if pagination continues. Shopify adds rel="next" link.
        next_link = soup.find("link", rel="next")
        if next_link and next_link.get("href") and (max_pages is None or page_num < max_pages):
            page_num += 1
            continue
        # Some themes use button/anchor.
        if soup.select_one("a.pagination__item--next") and (max_pages is None or page_num < max_pages):
            page_num += 1
            continue
        break

    logger.info(f"Crawled {len(posts)} blog posts across {page_num} pages.")
    return posts

def crawl_products(base_url: str, category_paths: List[str]) -> List[Dict]:
    """
    Crawl multiple product category pages and extract metadata for each product.
    Returns a list of dicts with title, url, price, and category.
    """
    products: List[Dict] = []
    seen_urls: set[str] = set()

    for path in category_paths:
        category_name = path.strip('/').split('/')[-1].replace('-', ' ').title()
        page = 1
        while True:
            url_path = f"{path}?page={page}" if page > 1 else path
            products_url = urljoin(base_url, url_path)
            html = fetch_url(products_url)
            if html is None:
                break
            soup = BeautifulSoup(html, "html.parser")

            cards = soup.select(".card__content")
            if not cards:
                break
            for card in cards:
                title_tag = card.select_one("a.card__heading, a.full-unstyled-link")
                title = title_tag.get_text(strip=True) if title_tag else None
                url = urljoin(base_url, title_tag['href']) if title_tag and title_tag.has_attr('href') else None
                price_tag = card.select_one(".price, .price-item")
                price = price_tag.get_text(strip=True) if price_tag else None
                if url and url not in seen_urls and title:
                    products.append({
                        "title": title,
                        "url": url,
                        "price": price,
                        "category": category_name
                    })
                    seen_urls.add(url)

            # Pagination detection on collection pages
            next_link = soup.find("link", rel="next")
            if next_link and next_link.get("href"):
                page += 1
                continue
            if soup.select_one("a.pagination__item--next"):
                page += 1
                continue
            break

    logger.info(f"Crawled {len(products)} products across {len(category_paths)} categories.")
    return products

def save_json(data, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def main():
    base_url = "https://www.pops.studio"
    blog_metadata = crawl_blog(base_url, blog_path="/blogs/news")
    save_json(blog_metadata, "copilot/content/blog_metadata.json")
    print(f"Saved {len(blog_metadata)} blog posts to copilot/content/blog_metadata.json")
    # Update changelog immediately after refreshing metadata
    update_changelog_main()
    # Product crawling is disabled by default. Uncomment below if needed.
    # category_paths = [
    #     "/collections/bathroom-vanity-units",
    #     "/collections/porch-swing-bed",
    #     "/collections/outdoor-swing",
    #     "/collections/indoor-swings",
    #     "/collections/bedside-shelves",
    #     "/collections/climbing"
    # ]
    # product_metadata = crawl_products(base_url, category_paths)
    # save_json(product_metadata, "copilot/content/product_metadata.json")
    # print(f"Saved {len(product_metadata)} products to copilot/content/product_metadata.json")

if __name__ == "__main__":
    main() 