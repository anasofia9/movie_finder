"""Supabase persistence layer.

Falls back gracefully: if SUPABASE_URL / SUPABASE_KEY are not set (or the
supabase package is missing), get_client() returns None and callers use the
existing local file caches instead.
"""
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

try:
    from dotenv import load_dotenv
    load_dotenv()  # loads .env for local dev; no-op if the file doesn't exist
except ImportError:
    pass

_client = None
_client_lock = threading.Lock()
_warned = False


def get_client():
    """Lazy singleton Supabase client. Returns None if not configured."""
    global _client, _warned
    if _client is not None:
        return _client

    url = os.environ.get('SUPABASE_URL')
    key = os.environ.get('SUPABASE_KEY')
    if not url or not key:
        if not _warned:
            print("⚠️  SUPABASE_URL/SUPABASE_KEY not set — using local file caches")
            _warned = True
        return None

    with _client_lock:
        if _client is None:
            try:
                from supabase import create_client
                _client = create_client(url, key)
            except Exception as e:
                if not _warned:
                    print(f"⚠️  Could not create Supabase client ({e}) — using local file caches")
                    _warned = True
                return None
    return _client


def is_enabled() -> bool:
    return get_client() is not None


# ---------------------------------------------------------------------------
# letterboxd_ratings
# ---------------------------------------------------------------------------

def get_fresh_ratings(urls: List[str], max_age_hours: int = 24) -> Dict[str, Dict]:
    """Bulk fetch rating rows newer than max_age_hours, keyed by letterboxd_url.

    Chunks the .in_() queries to keep request URLs a reasonable size.
    """
    client = get_client()
    if client is None or not urls:
        return {}

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=max_age_hours)).isoformat()
    results: Dict[str, Dict] = {}
    try:
        for i in range(0, len(urls), 100):
            chunk = urls[i:i + 100]
            resp = (
                client.table('letterboxd_ratings')
                .select('*')
                .in_('letterboxd_url', chunk)
                .gte('updated_at', cutoff)
                .execute()
            )
            for row in resp.data or []:
                results[row['letterboxd_url']] = row
    except Exception as e:
        print(f"⚠️  Supabase get_fresh_ratings failed: {e}")
    return results


_warned_director_column = False


def upsert_rating(letterboxd_url: str, title: str, rating, rating_count, year, resolved_url=None, genres=None, director=None) -> None:
    """Upsert a rating row. Negative results are stored too:
    rating None + resolved_url set = found but unrated; both None = not found.
    genres is a '|'-joined string ('' = fetched, none found; None = never fetched)."""
    global _warned_director_column
    client = get_client()
    if client is None:
        return
    row = {
        'letterboxd_url': letterboxd_url,
        'title': title,
        'rating': rating,
        'rating_count': str(rating_count) if rating_count is not None else None,
        'year': str(year) if year is not None else None,
        'resolved_url': resolved_url,
        'genres': genres,
        'director': director,
        'updated_at': datetime.now(timezone.utc).isoformat(),
    }
    try:
        client.table('letterboxd_ratings').upsert(row).execute()
    except Exception as e:
        # Tolerate a missing 'director' column so the app keeps working
        # until the ALTER TABLE has been run in Supabase
        if 'director' in str(e):
            if not _warned_director_column:
                print("⚠️  letterboxd_ratings has no 'director' column yet — run: "
                      "alter table letterboxd_ratings add column director text;")
                _warned_director_column = True
            row.pop('director', None)
            try:
                client.table('letterboxd_ratings').upsert(row).execute()
                return
            except Exception as e2:
                e = e2
        print(f"⚠️  Supabase upsert_rating failed for {letterboxd_url}: {e}")


def get_all_genres() -> List[str]:
    """Distinct genres across the whole letterboxd_ratings table, sorted."""
    client = get_client()
    if client is None:
        return []
    genres = set()
    try:
        resp = client.table('letterboxd_ratings').select('genres').not_.is_('genres', 'null').execute()
        for row in resp.data or []:
            for g in (row.get('genres') or '').split('|'):
                if g.strip():
                    genres.add(g.strip())
    except Exception as e:
        print(f"⚠️  Supabase get_all_genres failed: {e}")
    return sorted(genres)


# ---------------------------------------------------------------------------
# theater_showings
# ---------------------------------------------------------------------------

def get_latest_showings() -> Dict[str, Dict]:
    """Return the latest cached showings per theater (one row per theater_id),
    shaped like the theater_cache dict: {theater_id: {date, movies, cached_at}}.
    Freshness/staleness is decided by the caller."""
    client = get_client()
    if client is None:
        return {}
    try:
        resp = client.table('theater_showings').select('*').execute()
        return {
            row['theater_id']: {
                'date': row['show_date'],
                'movies': row['movies'],
                'cached_at': row['cached_at'],
            }
            for row in (resp.data or [])
        }
    except Exception as e:
        print(f"⚠️  Supabase get_latest_showings failed: {e}")
        return {}


def upsert_theater_showings(theater_id: str, date_str: str, movies: List[Dict]) -> None:
    client = get_client()
    if client is None:
        return
    try:
        client.table('theater_showings').upsert({
            'theater_id': theater_id,
            'show_date': date_str,
            'movies': movies,
            'cached_at': datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception as e:
        print(f"⚠️  Supabase upsert_theater_showings failed for {theater_id}: {e}")


def delete_stale_showings(cutoff_date_str: str) -> None:
    """TTL cleanup: delete showings rows with show_date older than the cutoff."""
    client = get_client()
    if client is None:
        return
    try:
        client.table('theater_showings').delete().lt('show_date', cutoff_date_str).execute()
    except Exception as e:
        print(f"⚠️  Supabase delete_stale_showings failed: {e}")
