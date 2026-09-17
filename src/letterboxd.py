import requests
from bs4 import BeautifulSoup
import re
import time
import json
import asyncio
import csv
import os
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from typing import Optional, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

try:
    from . import db
except ImportError:
    import db

class LetterboxdAPI:
    """Fetch Letterboxd ratings for movies"""
    
    def __init__(self):
        self.base_url = "https://letterboxd.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.cache = {}
        self.movies_found_no_rating = []
        self.cache_file = 'letterboxd_cache.csv'
        self.csv_cache = {}
        if not db.is_enabled():
            # Local file fallback when Supabase isn't configured
            self._load_csv_cache()
        self._lock = threading.Lock()  # For thread-safe operations
        self._browser_sem = threading.Semaphore(2)  # Cap concurrent Playwright fallbacks
    
    def _load_csv_cache(self):
        """Load existing cache from CSV file (includes negative results: no-rating/not-found)"""
        if os.path.exists(self.cache_file):
            try:
                # Legacy files may have no header row at all
                with open(self.cache_file, 'r', newline='', encoding='utf-8') as csvfile:
                    first_line = csvfile.readline()
                has_header = first_line.startswith('letterboxd_url')

                with open(self.cache_file, 'r', newline='', encoding='utf-8') as csvfile:
                    if has_header:
                        reader = csv.DictReader(csvfile)
                    else:
                        reader = csv.DictReader(csvfile, fieldnames=self.CSV_FIELDS[:6])
                    has_resolved_url = 'resolved_url' in (reader.fieldnames or [])
                    has_genres = 'genres' in (reader.fieldnames or [])
                    for row in reader:
                        letterboxd_url = row['letterboxd_url']
                        rating = float(row['rating']) if row['rating'] and row['rating'] != 'None' else None

                        if has_resolved_url:
                            resolved = row['resolved_url'] if row['resolved_url'] and row['resolved_url'] != 'None' else None
                        else:
                            # Legacy rows (pre-resolved_url): only positives were saved, keyed by their own URL
                            resolved = letterboxd_url

                        self.csv_cache[letterboxd_url] = {
                            'title': row['title'],
                            'rating': rating,
                            'rating_count': row['rating_count'] if row['rating_count'] and row['rating_count'] != 'None' else None,
                            'year': row['year'] if row['year'] and row['year'] != 'None' else None,
                            'updated': row['updated'],
                            'url': resolved,
                            'genres': self._decode_genres(row.get('genres')) if has_genres else None
                        }

                # One-time migration: rewrite legacy file with the new header so appends stay aligned
                if (not has_resolved_url or not has_genres) and self.csv_cache:
                    self._rewrite_csv_cache()
            except Exception as e:
                print(f"Error loading cache: {e}")
                self.csv_cache = {}

    def _rewrite_csv_cache(self):
        """Rewrite the whole CSV cache file with the current schema"""
        try:
            with open(self.cache_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.CSV_FIELDS)
                writer.writeheader()
                for key, entry in self.csv_cache.items():
                    writer.writerow({
                        'letterboxd_url': key,
                        'title': entry['title'],
                        'rating': entry['rating'],
                        'rating_count': entry['rating_count'],
                        'year': entry['year'],
                        'updated': entry['updated'],
                        'resolved_url': entry['url'],
                        'genres': self._encode_genres(entry.get('genres'))
                    })
        except Exception as e:
            print(f"Error rewriting cache: {e}")
    
    CSV_FIELDS = ['letterboxd_url', 'title', 'rating', 'rating_count', 'year', 'updated', 'resolved_url', 'genres']

    # In-memory 'genres' values: None = never fetched, '' = fetched but none found,
    # 'Drama|Comedy' = '|'-joined genre list. CSV can't store None vs '', so we
    # encode never-fetched as '' and fetched-none as '-' on disk.
    @staticmethod
    def _encode_genres(genres):
        if genres is None:
            return ''
        return genres if genres else '-'

    @staticmethod
    def _decode_genres(value):
        if not value:
            return None
        if value == '-':
            return ''
        return value

    def _save_to_csv_cache(self, letterboxd_url: str, title: str, rating_data: Dict):
        """Save rating data (including negative results) to the cache.

        Keyed by the originally generated letterboxd_url; rating_data['url'] holds
        the resolved URL (may differ if a fallback URL matched, or be None if not found).
        """
        self.csv_cache[letterboxd_url] = {
            'title': title,
            'rating': rating_data['rating'],
            'rating_count': rating_data['rating_count'],
            'year': rating_data['year'],
            'updated': datetime.now().isoformat(),
            'url': rating_data['url'],
            'genres': rating_data.get('genres')
        }

        if db.is_enabled():
            db.upsert_rating(
                letterboxd_url,
                title,
                rating_data['rating'],
                rating_data['rating_count'],
                rating_data['year'],
                rating_data['url'],
                rating_data.get('genres')
            )
            return

        # Write to CSV file
        try:
            # Check if file exists to determine if we need to write headers
            file_exists = os.path.exists(self.cache_file)

            with self._lock:
                with open(self.cache_file, 'a', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=self.CSV_FIELDS)

                    if not file_exists:
                        writer.writeheader()

                    writer.writerow({
                        'letterboxd_url': letterboxd_url,
                        'title': title,
                        'rating': rating_data['rating'],
                        'rating_count': rating_data['rating_count'],
                        'year': rating_data['year'],
                        'updated': datetime.now().isoformat(),
                        'resolved_url': rating_data['url'],
                        'genres': self._encode_genres(rating_data.get('genres'))
                    })
        except Exception as e:
            print(f"Error saving to cache: {e}")
    
    def _get_from_cache(self, letterboxd_url: str) -> Optional[Dict]:
        """Get rating data from cache.

        Ratings stay fresh for 7 days; negative results (no rating / not found)
        are only reused for 1 day so new ratings show up quickly."""
        if letterboxd_url in self.csv_cache:
            cached_data = self.csv_cache[letterboxd_url].copy()

            try:
                updated_time = datetime.fromisoformat(cached_data['updated'])
                # Supabase timestamps are tz-aware; CSV timestamps are naive local
                now = datetime.now(updated_time.tzinfo) if updated_time.tzinfo else datetime.now()
                time_diff = now - updated_time

                # Entries cached before genre support have genres=None ('' means
                # "fetched, none found") — refetch found movies so genres populate
                if cached_data.get('url') is not None and cached_data.get('genres') is None:
                    return None

                max_age = timedelta(days=7) if cached_data['rating'] is not None else timedelta(days=1)
                if time_diff <= max_age:
                    hours_ago = time_diff.total_seconds() // 3600
                    # print(f"Using cached rating for: {cached_data['title']} (cached {int(hours_ago)}h ago)")
                    # Cached "found but no rating" result — keep the tracking list accurate
                    if cached_data['rating'] is None and cached_data['url'] is not None:
                        with self._lock:
                            if cached_data['url'] not in self.movies_found_no_rating:
                                self.movies_found_no_rating.append(cached_data['url'])
                    return cached_data
                else:
                    print(f"Cache expired for: {cached_data['title']} (cached {time_diff.days} days ago), fetching fresh data")
                    return None
                    
            except (ValueError, KeyError) as e:
                print(f"Error parsing cache timestamp for {cached_data.get('title', 'unknown')}: {e}")
                return None
                
        return None

    def search_movie(self, title: str) -> Optional[str]:
        """Search for a movie and return its Letterboxd URL"""
        # Clean title for search
        clean_title = re.sub(r'[^\w\s]', '', title.lower())
        search_url = f"{self.base_url}/film/{clean_title.replace(' ', '+')}"
        
        try:
            response = requests.get(search_url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Find first film result
            film_link = soup.select_one('.film-detail a')
            if film_link:
                return self.base_url + film_link['href']
        except Exception as e:
            print(f"Error searching for {title}: {e}")
        
        return None
    
    def get_rating_from_url(self, letterboxd_url: str, title: str) -> Dict:
        """Get rating and metadata for a movie using direct Letterboxd URL"""
        # Check cache first (positive AND negative results, 24h freshness)
        cached_data = self._get_from_cache(letterboxd_url)
        if cached_data:
            return cached_data

        if title in self.cache:
            return self.cache[title]

        result = self._resolve_rating(letterboxd_url, title)

        # Cache every outcome (rated / found-no-rating / not-found) keyed by the
        # originally generated URL so restarts within 24h skip the re-fetch.
        # Transient fetch errors are not cached.
        self.cache[title] = result
        if not result.get('error'):
            self._save_to_csv_cache(letterboxd_url, title, result)
        return result

    def _resolve_rating(self, letterboxd_url: str, title: str) -> Dict:
        """Fetch rating from Letterboxd, trying URL fallbacks (no caching here)"""
        # Rate limiting - commented out for maximum speed with threading
        # time.sleep(0.2)

        # Try the original URL first
        result = self._fetch_rating_from_url(letterboxd_url, title)
        if result['url'] is not None:  # Found the movie (even if no rating)
            return result
        
        # If failed and URL contains a year, try without the year
        if re.search(r'-\d{4}/?$', letterboxd_url):
            # Import the scraper to regenerate clean URL without year
            from .scraper import MovieScraper
            scraper = MovieScraper(use_cache=False)  # only used for URL generation
            # Remove year from title and regenerate URL
            title_without_year = re.sub(r'\s*\(\d{4}\)', '', title)
            # The generate_letterboxd_url expects just the clean title, not a title with year
            # So we pass the title without year, and it will generate a clean URL
            clean_url_without_year = scraper.generate_letterboxd_url(title_without_year)
            result_no_year = self._fetch_rating_from_url(clean_url_without_year, title)
            if result_no_year['url'] is not None:  # Found the movie (even if no rating)
                return result_no_year
            
            # Both attempts failed - try more fallbacks
            from .scraper import MovieScraper
            scraper = MovieScraper(use_cache=False)  # only used for URL generation
            
            # Try removing "with xxxxx" suffix
            if ' with ' in title.lower():
                title_without_with = re.sub(r'\s+with\s+.*$', '', title, flags=re.IGNORECASE)
                clean_url_without_with = scraper.generate_letterboxd_url(title_without_with)
                result_no_with = self._fetch_rating_from_url(clean_url_without_with, title)
                if result_no_with['url'] is not None:
                    return result_no_with
            
            # Try removing & completely (for cases like "Stiller & Meara" -> "Stiller Meara")
            if '&' in title:
                title_no_ampersand = re.sub(r'\s*&\s*', ' ', title)
                title_no_ampersand = re.sub(r'\s+', ' ', title_no_ampersand).strip()  # Clean up extra spaces
                clean_url_no_ampersand = scraper.generate_letterboxd_url(title_no_ampersand)
                result_no_ampersand = self._fetch_rating_from_url(clean_url_no_ampersand, title)
                if result_no_ampersand['url'] is not None:
                    return result_no_ampersand
            # else:
                # print(f"Movie not found at {letterboxd_url}, tried without year at {clean_url_without_year} - both failed")
                
            # All attempts failed, so truly not found
            return {
                'rating': None,
                'rating_count': None,
                'url': None,  # Truly not found
                'year': None,
                'genres': None,
                'error': result.get('error', False)
            }
        
        # If URL doesn't have year and failed, try removing "with xxxxx" suffix
        if ' with ' in title.lower():
            # print(f"Movie not found at {letterboxd_url} - trying without 'with' suffix...")
            from .scraper import MovieScraper
            scraper = MovieScraper(use_cache=False)  # only used for URL generation
            # Remove "with xxxxx" suffix and regenerate URL
            title_without_with = re.sub(r'\s+with\s+.*$', '', title, flags=re.IGNORECASE)
            clean_url_without_with = scraper.generate_letterboxd_url(title_without_with)
            result_no_with = self._fetch_rating_from_url(clean_url_without_with, title)
            if result_no_with['url'] is not None:  # Found the movie (even if no rating)
                return result_no_with
        
        # Try removing & completely (for cases like "Stiller & Meara" -> "Stiller Meara")
        if '&' in title:
            # print(f"Movie not found at {letterboxd_url} - trying without ampersand...")
            from .scraper import MovieScraper
            scraper = MovieScraper(use_cache=False)  # only used for URL generation
            title_no_ampersand = re.sub(r'\s*&\s*', ' ', title)
            title_no_ampersand = re.sub(r'\s+', ' ', title_no_ampersand).strip()  # Clean up extra spaces
            clean_url_no_ampersand = scraper.generate_letterboxd_url(title_no_ampersand)
            result_no_ampersand = self._fetch_rating_from_url(clean_url_no_ampersand, title)
            if result_no_ampersand['url'] is not None:
                return result_no_ampersand
            
        # Truly not found
        return {
            'rating': None,
            'rating_count': None,
            'url': None,  # Truly not found
            'year': None,
            'error': result.get('error', False)
        }
    
    def _fetch_rating_from_url(self, letterboxd_url: str, title: str) -> Dict:
        """Internal method to fetch rating from a specific URL"""
       
        try:
            response = requests.get(letterboxd_url, headers=self.headers, timeout=10)
            if response.status_code != 200:
                return {'rating': None, 'rating_count': None, 'url': None, 'year': None, 'genres': None}  # Not found

            soup = BeautifulSoup(response.content, 'lxml')

            # Look for JSON-LD structured data
            json_scripts = soup.find_all('script', type='application/ld+json')
            rating = None
            rating_count = None
            year = None
            genres = None
            found_movie_data = False
            
            for script in json_scripts:
                try:
                    # Clean the script content - remove CDATA comments
                    content = script.string
                    if content:
                        # Remove CDATA wrapper
                        content = re.sub(r'/\*\s*<!\[CDATA\[\s*\*/\s*', '', content)
                        content = re.sub(r'\s*/\*\s*\]\]>\s*\*/', '', content)
                        content = content.strip()
                        
                        data = json.loads(content)
                        
                        # Check if we found movie data (even if no rating)
                        if isinstance(data, dict) and data.get('@type') == 'Movie':
                            found_movie_data = True
                            
                            # Extract genres ('genre' may be a string or a list)
                            genre_data = data.get('genre')
                            if isinstance(genre_data, str):
                                genres = genre_data
                            elif isinstance(genre_data, list):
                                genres = '|'.join(str(g) for g in genre_data)
                            else:
                                genres = ''  # fetched, none found

                            # Extract year from dateCreated
                            if 'dateCreated' in data:
                                year_match = re.search(r'\d{4}', data['dateCreated'])
                                if year_match:
                                    year = year_match.group()
                            
                            if 'aggregateRating' in data:
                                aggregate = data['aggregateRating']
                                rating = float(aggregate.get('ratingValue', 0))
                                rating_count = int(aggregate.get('ratingCount', 0))
                            else:
                                # Movie found but no aggregateRating - try dynamic loading with Playwright
                                # Semaphore caps concurrent Chromium launches (memory on Render)
                                with self._browser_sem:
                                    rating, rating_count, is_computed = asyncio.run(self._get_dynamic_rating(letterboxd_url))
                                if rating is not None and is_computed:
                                    # Mark this as computed from histogram
                                    rating_count = f"{rating_count}*"
                            break
                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    continue
            
            # Fallback to HTML parsing if JSON-LD fails or if we found movie but no rating
            if rating is None and found_movie_data:
                rating_elem = soup.select_one('.average-rating')
                if rating_elem:
                    rating_text = rating_elem.text.strip()
                    rating = float(rating_text) if rating_text else None
                
                if rating is None:
                    # Movie found but no rating - add to special list
                    
                    self.movies_found_no_rating.append(letterboxd_url)
                
            elif rating is None:
                
                rating_elem = soup.select_one('.average-rating')
                if rating_elem:
                    rating_text = rating_elem.text.strip()
                    rating = float(rating_text) if rating_text else None
                    
                year_elem = soup.select_one('.film-title-wrapper a')
                if year_elem:
                    year_match = re.search(r'\d{4}', year_elem.text)
                    if year_match:
                        year = year_match.group()
                        
                # If we got here and found rating/year, that means the movie exists
                if rating is not None or year is not None:
                    found_movie_data = True
                    
            # Only set URL if we actually found the movie
            result = {
                'rating': rating,
                'rating_count': rating_count,
                'url': letterboxd_url if found_movie_data else None,
                'year': year,
                # '' (fetched, none) when found via HTML fallback without JSON-LD genres
                'genres': (genres if genres is not None else '') if found_movie_data else None,
                'computed_from_histogram': isinstance(rating_count, str) and rating_count.endswith('*')
            }
            
            # Caching happens in get_rating_from_url (keyed by the original URL)
            if result['url'] is not None and result['rating'] is None:
                # Movie found but no rating - add to special tracking list
                with self._lock:
                    if letterboxd_url not in self.movies_found_no_rating:
                        self.movies_found_no_rating.append(letterboxd_url)

            return result
            
        except Exception as e:
            print(f"Error getting rating from URL {letterboxd_url}: {e}")
            # 'error' flag prevents caching a transient failure as "not found" for 24h
            return {'rating': None, 'rating_count': None, 'url': None, 'year': None, 'genres': None, 'error': True}

    def get_rating(self, title: str) -> Dict:
        """Get rating and metadata for a movie using search"""
        movie_url = self.search_movie(title)
        if not movie_url:
            return {'rating': None, 'rating_count': None, 'url': None, 'year': None}
        
        return self.get_rating_from_url(movie_url, title)
    
    async def _get_dynamic_rating(self, letterboxd_url: str) -> tuple:
        """Use Playwright to get rating from dynamically loaded content"""
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                # Use shorter timeout and less strict wait condition
                await page.goto(letterboxd_url, wait_until='domcontentloaded', timeout=15000)
                
                # Wait for the CSI rating data to load
                try:
                    await page.wait_for_selector('.csi[data-on-load="rating-histogram"]', timeout=3000)
                    await page.wait_for_timeout(2000)  # Additional wait for content to populate
                except:
                    # Even if CSI doesn't load, try to get what we can
                    await page.wait_for_timeout(1000)  # Give it a moment
                
                # Print the HTML to see what we got
                html_content = await page.content()
                # print("=== DYNAMIC LOADING HTML ===")
                # print(html_content)
                # print("=== END DYNAMIC HTML ===")
                
                # Parse the HTML content with BeautifulSoup
                soup = BeautifulSoup(html_content, 'lxml')
                await browser.close()
                
                # Try to get rating from parsed HTML
                rating, rating_count, is_computed = self._parse_rating_from_html(soup)
                return rating, rating_count, is_computed
                    
        except Exception as e:
            print(f"Error with dynamic loading: {e}")
            return None, None, False
    
    def _parse_rating_from_html(self, soup: BeautifulSoup) -> tuple:
        """Parse rating from HTML soup - either from average-rating or histogram"""
        
        # First try to find an existing average rating
        avg_rating_elem = soup.select_one('.average-rating')
        if avg_rating_elem:
            try:
                rating = float(avg_rating_elem.text.strip())
                # Try to find rating count
                rating_count_elem = soup.select_one('.rating-count, [data-rating-count]')
                count = 0
                if rating_count_elem:
                    count_text = re.sub(r'[^0-9]', '', rating_count_elem.text)
                    count = int(count_text) if count_text else 0
                return rating, count, False
            except (ValueError, AttributeError):
                pass
        
        # Parse the rating histogram to calculate average ourselves
        histogram_bars = soup.select('.rating-histogram-bar a[data-original-title]')
        if histogram_bars:
            total_weighted_rating = 0
            total_count = 0
            
            # print(f"Found {len(histogram_bars)} histogram bars, parsing...")
            # print(histogram_bars)
            for bar in histogram_bars:
                tooltip = bar.get('data-original-title', '')
                # print(f"Parsing tooltip: {tooltip}")
                
                # Parse formats like "10 half-★ ratings (2%)" or "45 ★★ ratings (10%)"
                count_match = re.match(r'^(\d+)\s', tooltip)
                if count_match:
                    count = int(count_match.group(1))
                    
                    # Determine star value
                    star_value = 0
                    if 'half-★' in tooltip:
                        star_value = 0.5
                    else:
                        # Count ★ symbols
                        stars = tooltip.count('★')
                        if '½' in tooltip:
                            star_value = stars + 0.5
                        else:
                            star_value = stars
                    
                    # print(f"  {count} ratings at {star_value} stars")
                    total_weighted_rating += count * star_value
                    total_count += count
            
            if total_count > 0:
                average_rating = total_weighted_rating / total_count
                average_rating = round(average_rating, 2)  # Round to 2 decimals
                # print(f"Calculated average: {average_rating} from {total_count} total ratings")
                return average_rating, total_count, True  # True = computed from histogram
        
        return None, None, False
    
    def get_all_genres(self) -> List[str]:
        """Distinct genres across the whole ratings cache (Supabase or local CSV)."""
        if db.is_enabled():
            return db.get_all_genres()
        genres = set()
        for entry in self.csv_cache.values():
            for g in (entry.get('genres') or '').split('|'):
                if g.strip():
                    genres.add(g.strip())
        return sorted(genres)

    def filter_movies_by_cache(self, movies: List[Dict]) -> tuple[List[Dict], List[Dict]]:
        """Separate movies into cached and uncached lists"""
        cached_movies = []
        uncached_movies = []
        
        for movie in movies:
            letterboxd_url = movie.get('letterboxd_url')
            cached_data = self._get_from_cache(letterboxd_url) if letterboxd_url else None
            if cached_data:
                # Movie is in cache (rated, no-rating, or not-found), add cached data
                movie['letterboxd_rating'] = cached_data['rating']
                movie['letterboxd_url'] = cached_data['url']
                movie['year'] = cached_data['year']
                movie['genres'] = [g for g in (cached_data.get('genres') or '').split('|') if g]
                cached_movies.append(movie)
            else:
                # Movie needs to be processed
                uncached_movies.append(movie)
        
        return cached_movies, uncached_movies
    
    def _preload_db_cache(self, movies: List[Dict]) -> None:
        """Bulk-preload fresh ratings from Supabase into the in-memory cache
        (single main-thread query; worker threads only write/upsert)."""
        if not db.is_enabled():
            return
        urls = [m['letterboxd_url'] for m in movies if m.get('letterboxd_url')]
        # 7-day window; _get_from_cache applies stricter 1-day freshness to negatives
        for url, row in db.get_fresh_ratings(urls, max_age_hours=24 * 7).items():
            # Include negative results (rating None); resolved_url distinguishes
            # found-but-unrated (set) from not-found (None)
            self.csv_cache[url] = {
                'title': row.get('title'),
                'rating': row.get('rating'),
                'rating_count': row.get('rating_count'),
                'year': row.get('year'),
                'updated': row.get('updated_at'),
                'url': row.get('resolved_url'),
                'genres': row.get('genres')
            }

    def apply_cached_ratings(self, movies: List[Dict]) -> tuple:
        """Attach ratings/genres from caches only (Supabase or CSV) — never
        hits the network. Returns (with_cached_data, without_cached_data)."""
        if not movies:
            return [], []
        self._preload_db_cache(movies)
        return self.filter_movies_by_cache(movies)

    def process_movie_batch(self, movies: List[Dict], progress_callback=None, max_workers=12) -> List[Dict]:
        """Process multiple movies concurrently with threading"""
        if not movies:
            return []

        self._preload_db_cache(movies)

        # Filter movies by cache first
        cached_movies, uncached_movies = self.filter_movies_by_cache(movies)
        
        if progress_callback:
            progress_callback(f"📂 Found {len(cached_movies)} movies in cache, processing {len(uncached_movies)} new movies")
        
        if not uncached_movies:
            return cached_movies
        
        # Process uncached movies with threading
        processed_movies = []
        movies_not_found = []
        
        def process_single_movie(movie):
            """Process a single movie - thread-safe"""
            letterboxd_url = movie.get('letterboxd_url')
            title = movie.get('title', 'Unknown')
            
            if letterboxd_url:
                rating_data = self.get_rating_from_url(letterboxd_url, title)
                
                # Thread-safe update of movie data
                movie['letterboxd_rating'] = rating_data['rating']
                movie['letterboxd_url'] = rating_data['url']
                movie['year'] = rating_data['year']
                movie['genres'] = [g for g in (rating_data.get('genres') or '').split('|') if g]
                
                if rating_data['rating'] is None and rating_data['url'] is None:
                    with self._lock:
                        movies_not_found.append(movie)
            
            return movie
        
        # Use ThreadPoolExecutor for concurrent processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all jobs
            future_to_movie = {executor.submit(process_single_movie, movie): movie for movie in uncached_movies}
            
            # Process completed jobs
            completed = 0
            for future in as_completed(future_to_movie):
                try:
                    processed_movie = future.result()
                    processed_movies.append(processed_movie)
                    completed += 1
                    
                    if progress_callback and completed % max(1, len(uncached_movies) // 10) == 0:
                        progress_callback(f"📊 Processed {completed}/{len(uncached_movies)} movies ({completed/len(uncached_movies)*100:.0f}%)")
                        
                except Exception as e:
                    movie = future_to_movie[future]
                    if progress_callback:
                        progress_callback(f"❌ Error processing {movie.get('title', 'Unknown')}: {e}")
        
        # Update the movies_not_found list in a thread-safe way
        with self._lock:
            self.movies_found_no_rating.extend([url for movie in movies_not_found for url in [movie.get('letterboxd_url')] if url])
        
        # Combine cached and processed movies
        all_movies = cached_movies + processed_movies
        
        if progress_callback:
            progress_callback(f"✅ Completed processing {len(all_movies)} total movies ({len(cached_movies)} from cache, {len(processed_movies)} newly processed)")
        
        return all_movies
