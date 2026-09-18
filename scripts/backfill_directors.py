"""One-off: backfill letterboxd_ratings.director for existing rows.

Fetches each found movie's Letterboxd page, extracts the director from the
JSON-LD block, and updates the row. Requires the 'director' column to exist:
    alter table letterboxd_ratings add column director text;

Run: ./venv/bin/python scripts/backfill_directors.py
"""
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

sys.path.insert(0, '.')
from src import db  # noqa: E402

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}


def extract_director(url: str):
    resp = requests.get(url, headers=HEADERS, timeout=15)
    if resp.status_code != 200:
        return None
    for m in re.finditer(r'<script type="application/ld\+json">(.*?)</script>', resp.text, re.S):
        content = re.sub(r'/\*\s*<!\[CDATA\[\s*\*/\s*', '', m.group(1))
        content = re.sub(r'\s*/\*\s*\]\]>\s*\*/', '', content).strip()
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and data.get('@type') == 'Movie':
            d = data.get('director')
            if isinstance(d, list):
                return ', '.join(x.get('name') for x in d if isinstance(x, dict) and x.get('name')) or None
            if isinstance(d, dict):
                return d.get('name')
            if isinstance(d, str):
                return d
    return None


def main():
    client = db.get_client()
    if client is None:
        print('Supabase not configured')
        return

    rows = (client.table('letterboxd_ratings')
            .select('letterboxd_url, resolved_url, title, director')
            .not_.is_('resolved_url', 'null')
            .execute()).data or []
    todo = [r for r in rows if not r.get('director')]
    print(f'{len(rows)} found movies, {len(todo)} missing director')

    def work(row):
        director = extract_director(row['resolved_url'])
        if director:
            client.table('letterboxd_ratings').update({'director': director}) \
                .eq('letterboxd_url', row['letterboxd_url']).execute()
        return row['title'], director

    done = failed = 0
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(work, r) for r in todo]
        for f in as_completed(futures):
            try:
                title, director = f.result()
                done += 1
                if director:
                    print(f'  [{done}/{len(todo)}] {title[:40]} -> {director}')
                else:
                    print(f'  [{done}/{len(todo)}] {title[:40]} -> (no director found)')
            except Exception as e:
                failed += 1
                print(f'  error: {e}')
    print(f'Backfill complete: {done} processed, {failed} failed')


if __name__ == '__main__':
    main()
