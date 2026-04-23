import os
import re
import math
import json
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import httpx
import psycopg
from bs4 import BeautifulSoup

YOUTUBE_API_KEY = os.environ["YOUTUBE_API_KEY"]
DATABASE_URL = os.environ["DATABASE_URL"]

YOUTUBE_BASE = "https://www.googleapis.com/youtube/v3"
QUERIES = [
    "best wireless router review",
    "wireless router review",
    "asus router review",
    "netgear router review",
]
PUBLISHED_BEFORE = "2022-01-01T00:00:00Z"

URL_RE = re.compile(r'https?://[^\s<>()\]]+')
DOMAIN_RE = re.compile(r'\b(?:[a-z0-9-]+\.)+[a-z]{2,}(?:/[^\s<>()\]]*)?\b', re.I)

DROP_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid"
}

def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    query = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
             if k.lower() not in DROP_PARAMS]
    return urlunparse(parsed._replace(
        scheme=scheme,
        netloc=netloc,
        path=path,
        query=urlencode(query, doseq=True),
        fragment=""
    ))

def extract_urls(text: str) -> list[str]:
    if not text:
        return []
    found = set()
    for m in URL_RE.findall(text):
        found.add(m.strip(".,);]}>"))
    for m in DOMAIN_RE.findall(text):
        if not m.startswith(("http://", "https://")):
            found.add("https://" + m.strip(".,);]}>"))
    return sorted(found)

def years_old(published_at: str) -> float:
    dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    return (now - dt).days / 365.25

def is_old_and_popular(view_count: int, comment_count: int, published_at: str) -> bool:
    return years_old(published_at) >= 2 and view_count >= 100_000 and comment_count >= 50

def score(view_count: int, comment_count: int, age_years: float, broken_confidence: float = 1.0) -> float:
    video_score = math.log10(view_count + 1) * 0.45 + math.log10(comment_count + 1) * 0.20 + age_years * 0.10
    return video_score * 0.7 + broken_confidence * 0.3

async def youtube_search(client: httpx.AsyncClient, query: str) -> list[str]:
    params = {
        "key": YOUTUBE_API_KEY,
        "part": "snippet",
        "type": "video",
        "q": query,
        "order": "viewCount",
        "publishedBefore": PUBLISHED_BEFORE,
        "maxResults": 25,
    }
    r = await client.get(f"{YOUTUBE_BASE}/search", params=params)
    r.raise_for_status()
    data = r.json()
    return [item["id"]["videoId"] for item in data.get("items", [])]

async def youtube_videos(client: httpx.AsyncClient, ids: list[str]) -> list[dict]:
    if not ids:
        return []
    params = {
        "key": YOUTUBE_API_KEY,
        "part": "snippet,statistics",
        "id": ",".join(ids),
        "maxResults": len(ids),
    }
    r = await client.get(f"{YOUTUBE_BASE}/videos", params=params)
    r.raise_for_status()
    return r.json().get("items", [])

async def check_url(client: httpx.AsyncClient, url: str) -> dict:
    start = time.perf_counter()
    try:
        r = await client.get(url, follow_redirects=True, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        elapsed = int((time.perf_counter() - start) * 1000)
        text = r.text[:100000]
        soup = BeautifulSoup(text, "html.parser")
        title = soup.title.string.strip() if soup.title and soup.title.string else ""
        lower = text.lower()
        soft = any(s in lower for s in ["product not found", "page not found", "domain for sale", "buy this domain", "coming soon"])
        return {
            "http_status": r.status_code,
            "final_url": str(r.url),
            "response_time_ms": elapsed,
            "is_broken": r.status_code in {404, 410, 500, 502, 503, 504},
            "is_soft_broken": soft,
            "broken_type": "soft_broken" if soft else ("hard_error" if r.status_code >= 400 else None),
            "redirect_chain": [{"status": h.status_code, "url": str(h.url)} for h in r.history],
            "page_title": title,
        }
    except Exception as e:
        return {
            "http_status": None,
            "final_url": None,
            "response_time_ms": None,
            "is_broken": True,
            "is_soft_broken": False,
            "broken_type": type(e).__name__,
            "redirect_chain": [],
            "page_title": None,
        }

def upsert_video(cur, v: dict, query: str) -> int:
    stats = v.get("statistics", {})
    snippet = v.get("snippet", {})
    cur.execute("""
        insert into videos (
            video_id, channel_id, title, description, published_at, discovered_query,
            view_count, comment_count, like_count, is_old, is_popular
        ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        on conflict (video_id) do update set
            channel_id = excluded.channel_id,
            title = excluded.title,
            description = excluded.description,
            published_at = excluded.published_at,
            discovered_query = excluded.discovered_query,
            view_count = excluded.view_count,
            comment_count = excluded.comment_count,
            like_count = excluded.like_count,
            is_old = excluded.is_old,
            is_popular = excluded.is_popular
        returning id
    """, (
        v["id"],
        snippet.get("channelId"),
        snippet.get("title"),
        snippet.get("description", ""),
        snippet.get("publishedAt"),
        query,
        int(stats.get("viewCount", 0)),
        int(stats.get("commentCount", 0)),
        int(stats.get("likeCount", 0)),
        years_old(snippet["publishedAt"]) >= 2,
        is_old_and_popular(int(stats.get("viewCount", 0)), int(stats.get("commentCount", 0)), snippet["publishedAt"])
    ))
    return cur.fetchone()[0]

def upsert_link(cur, video_pk: int, original_url: str, normalized_url: str) -> int:
    cur.execute("""
        insert into links (video_id, source_type, original_url, normalized_url)
        values (%s, 'description', %s, %s)
        on conflict (video_id, normalized_url, source_type) do update set
            original_url = excluded.original_url
        returning id
    """, (video_pk, original_url, normalized_url))
    return cur.fetchone()[0]

def insert_check(cur, link_id: int, result: dict) -> int:
    cur.execute("""
        insert into link_checks (
            link_id, http_status, final_url, response_time_ms, is_broken, is_soft_broken,
            broken_type, redirect_chain, page_title
        ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        returning id
    """, (
        link_id,
        result["http_status"],
        result["final_url"],
        result["response_time_ms"],
        result["is_broken"],
        result["is_soft_broken"],
        result["broken_type"],
        json.dumps(result["redirect_chain"]),
        result["page_title"],
    ))
    return cur.fetchone()[0]

def upsert_opportunity(cur, video_pk: int, link_id: int, check_id: int, score_value: float, niche: str):
    cur.execute("""
        insert into opportunities (video_id, link_id, latest_check_id, opportunity_score, niche)
        values (%s,%s,%s,%s,%s)
        on conflict do nothing
    """, (video_pk, link_id, check_id, score_value, niche))

async def main():
    async with httpx.AsyncClient() as client, psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            for query in QUERIES:
                ids = await youtube_search(client, query)
                videos = await youtube_videos(client, ids)

                for v in videos:
                    stats = v.get("statistics", {})
                    snippet = v.get("snippet", {})
                    views = int(stats.get("viewCount", 0))
                    comments = int(stats.get("commentCount", 0))
                    published_at = snippet["publishedAt"]

                    if not is_old_and_popular(views, comments, published_at):
                        continue

                    video_pk = upsert_video(cur, v, query)
                    urls = extract_urls(snippet.get("description", ""))

                    for url in urls:
                        normalized = normalize_url(url)
                        link_id = upsert_link(cur, video_pk, url, normalized)
                        result = await check_url(client, normalized)

                        if result["is_broken"] or result["is_soft_broken"]:
                            check_id = insert_check(cur, link_id, result)
                            opp_score = score(views, comments, years_old(published_at), 1.0)
                            upsert_opportunity(cur, video_pk, link_id, check_id, opp_score, query)

            conn.commit()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
